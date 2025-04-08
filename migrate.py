import os
import argparse
import json
import pandas as pd
import psycopg2
import clickhouse_connect
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from google.oauth2 import service_account


def get_pg_connection(db_name):
    return psycopg2.connect(
        host=os.environ['PG_HOST'],
        dbname=db_name,
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD']
    )


def get_pg_engine(db_name):
    return create_engine(
        f"postgresql://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}@"
        f"{os.environ['PG_HOST']}:{os.environ.get('PG_PORT')}/{db_name}"
    )


def get_pg_tables(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """))
        return [row[0] for row in result]


def read_table_to_df(table, engine):
    return pd.read_sql_table(table, con=engine, schema='public')


def migrate_to_clickhouse(db_name):
    pg_conn = get_pg_connection(db_name)
    pg_cursor = pg_conn.cursor()
    pg_engine = get_pg_engine(db_name)
    tables = get_pg_tables(pg_engine)

    ch_client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )

    type_mapping = {
        'integer': 'Int32', 'bigint': 'Int64', 'smallint': 'Int16',
        'serial': 'Int32', 'bigserial': 'Int64', 'boolean': 'UInt8',
        'text': 'String', 'varchar': 'String', 'character varying': 'String',
        'timestamp without time zone': 'DateTime', 'timestamp with time zone': 'DateTime',
        'date': 'Date', 'double precision': 'Float64', 'real': 'Float32',
        'numeric': 'Float64', 'json': 'String',
    }

    for table in tables:
        pg_cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        columns = pg_cursor.fetchall()

        if not columns:
            print(f"⚠️ Skipping empty or inaccessible table: {table}")
            continue

        ch_columns = [f"`{name}` {type_mapping.get(pg_type, 'String')}" for name, pg_type in columns]
        create_sql = f"CREATE TABLE IF NOT EXISTS {db_name}.{table} (\n    {', '.join(ch_columns)}\n) ENGINE = MergeTree() ORDER BY tuple()"

        try:
            ch_client.command(create_sql)
            print(f"✅ Created table: {db_name}.{table}")
        except Exception as e:
            print(f"❌ Failed to create table {table}: {e}")
            continue

        insert_sql = (
            f"INSERT INTO {db_name}.{table} "
            f"SELECT * FROM postgresql('{os.environ['PG_HOST']}', '{db_name}', '{table}', "
            f"'{os.environ['PG_USER']}', '{os.environ['PG_PASSWORD']}')"
        )
        try:
            ch_client.command(insert_sql)
            print(f"✅ Imported data from: {table}")
        except Exception as e:
            print(f"❌ Failed to import {table}: {e}")

    pg_cursor.close()
    pg_conn.close()


def migrate_to_bigquery(db_name):
    bq_project = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(credentials=credentials, project=bq_project)

    dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=db_name)

    try:
        bq_client.get_dataset(dataset_ref)
        print(f"🗑️ Deleting existing dataset {db_name} and its contents...")
        bq_client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
        print(f"✅ Deleted dataset {db_name}")
    except Exception as e:
        print(f"⚠️ Could not retrieve or delete dataset: {e}")

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "us-central1"
    bq_client.create_dataset(dataset)
    print(f"📦 Created BigQuery dataset: {db_name}")

    pg_engine = get_pg_engine(db_name)
    tables = get_pg_tables(pg_engine)

    for table in tables:
        try:
            print(f"📥 Reading table {table} from PostgreSQL...")
            df = read_table_to_df(table, pg_engine)

            if df.empty:
                print(f"⚠️ Skipping empty table: {table}")
                continue

            bq_table_id = f"{bq_project}.{db_name}.{table}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

            print(f"📤 Uploading to BigQuery: {bq_table_id}")
            load_job = bq_client.load_table_from_dataframe(df, bq_table_id, job_config=job_config)
            load_job.result()
            print(f"✅ Loaded {table} to BigQuery")

        except Exception as e:
            print(f"❌ Failed to load table {table}: {e}")


def migrate_bigquery_to_clickhouse(db_name):
    bq_project = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    ch_client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(credentials=credentials, project=bq_project)
    dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=db_name)

    tables = list(bq_client.list_tables(dataset_ref))

    with open(credentials_path) as f:
        key_json = json.dumps(json.load(f))

    for table in tables:
        table_name = table.table_id
        ch_table = f"{db_name}.{table_name}"
        try:
            print(f"📤 Importing {table_name} from BigQuery into ClickHouse...")
            insert_sql = (
                f"INSERT INTO {ch_table} "
                f"SELECT * FROM bigquery('{bq_project}', '{db_name}', '{table_name}', '{key_json}')"
            )
            ch_client.command(insert_sql)
            print(f"✅ Imported {table_name}")
        except Exception as e:
            print(f"❌ Failed to import {table_name}: {e}")


def migrate_bigquery_to_postgresql(db_name):
    ensure_postgres_database_exists(db_name)
    bq_project = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    pg_engine = get_pg_engine(db_name)

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(credentials=credentials, project=bq_project)
    dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=db_name)

    tables = list(bq_client.list_tables(dataset_ref))

    for table in tables:
        table_name = table.table_id
        try:
            print(f"📥 Downloading {table_name} from BigQuery...")
            df = bq_client.list_rows(f"{bq_project}.{db_name}.{table_name}").to_dataframe()

            if df.empty:
                print(f"⚠️ Skipping empty table: {table_name}")
                continue

            print(f"📤 Inserting into PostgreSQL table: {table_name}")
            df.to_sql(table_name, con=pg_engine, index=False, if_exists='replace')
            print(f"✅ Inserted {table_name} into PostgreSQL")
        except Exception as e:
            print(f"❌ Failed to migrate {table_name}: {e}")


def migrate_clickhouse_to_postgresql(db_name):
    ch_client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )
    pg_engine = get_pg_engine(db_name)

    tables = ch_client.query(f"SHOW TABLES FROM {db_name}").result_rows

    for (table,) in tables:
        try:
            print(f"📥 Reading from ClickHouse: {db_name}.{table}")
            result = ch_client.query(f"SELECT * FROM {db_name}.{table}")
            df = pd.DataFrame(result.result_rows, columns=result.column_names)

            if df.empty:
                print(f"⚠️ Skipping empty table: {table}")
                continue

            print(f"📤 Writing to PostgreSQL: {table}")
            df.to_sql(table, con=pg_engine, index=False, if_exists='replace')
            print(f"✅ Migrated {table} to PostgreSQL")
        except Exception as e:
            print(f"❌ Failed to migrate {table}: {e}")


def migrate_clickhouse_to_bigquery(db_name):
    ch_client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )

    bq_project = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(credentials=credentials, project=bq_project)

    dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=db_name)
    try:
        bq_client.get_dataset(dataset_ref)
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "us-central1"
        bq_client.create_dataset(dataset)
        print(f"📦 Created BigQuery dataset: {db_name}")

    tables = ch_client.query(f"SHOW TABLES FROM {db_name}").result_rows

    for (table,) in tables:
        try:
            print(f"📥 Reading from ClickHouse: {db_name}.{table}")
            result = ch_client.query(f"SELECT * FROM {db_name}.{table}")
            df = pd.DataFrame(result.result_rows, columns=result.column_names)

            if df.empty:
                print(f"⚠️ Skipping empty table: {table}")
                continue

            table_id = f"{bq_project}.{db_name}.{table}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            print(f"📤 Uploading to BigQuery: {table_id}")
            load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
            load_job.result()
            print(f"✅ Migrated {table} to BigQuery")
        except Exception as e:
            print(f"❌ Failed to migrate {table}: {e}")

def ensure_postgres_database_exists(db_name):
    admin_conn = psycopg2.connect(
        host=os.environ['PG_HOST'],
        dbname='postgres',  # default admin db
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD']
    )
    admin_conn.autocommit = True
    admin_cursor = admin_conn.cursor()
    admin_cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = admin_cursor.fetchone()
    if not exists:
        print(f"🛠️ Creating PostgreSQL database: {db_name}")
        admin_cursor.execute(f'CREATE DATABASE "{db_name}"')
    else:
        print(f"✅ PostgreSQL database '{db_name}' already exists.")
    admin_cursor.close()
    admin_conn.close()



def main():
    parser = argparse.ArgumentParser(description="Migrate data between PostgreSQL, ClickHouse, and BigQuery")
    parser.add_argument('--source', choices=['postgresql', 'bigquery'], required=True, help='Source database')
    parser.add_argument('--target', choices=['clickhouse', 'bigquery', 'postgresql'], required=True, help='Target database')
    parser.add_argument('--database', choices=['basketball_player', 'basketball_team', 'basketball_game'], required=True, help='Which database to migrate')

    args = parser.parse_args()
    db_name = args.database
    source = args.source
    target = args.target

    os.environ['DATABASE_PLAYER'] = db_name

    if source == 'postgresql' and target == 'clickhouse':
        migrate_to_clickhouse(db_name)
    elif source == 'postgresql' and target == 'bigquery':
        migrate_to_bigquery(db_name)
    elif source == 'bigquery' and target == 'clickhouse':
        migrate_bigquery_to_clickhouse(db_name)
    elif source == 'bigquery' and target == 'postgresql':
        migrate_bigquery_to_postgresql(db_name)
    elif source == 'clickhouse' and target == 'postgresql':
        migrate_clickhouse_to_postgresql(db_name)
    elif source == 'clickhouse' and target == 'bigquery':
        migrate_clickhouse_to_bigquery(db_name)        
    else:
        print(f"❌ Unsupported source → target combination: {source} → {target}")


if __name__ == '__main__':
    main()
