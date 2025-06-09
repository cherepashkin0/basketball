# This script consolidates all basketball data from separate DBMS into a single PostgreSQL database

import os
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, inspect
import clickhouse_connect
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery_storage_v1 import BigQueryReadClient
from migrate import get_pg_engine, ensure_postgres_database_exists, copy_from_stringio, migrate_bigquery_to_postgresql, migrate_clickhouse_to_postgresql


# Configuration
DATA_ORIGINS = {
    "basketball_game": "clickhouse",
    "basketball_player": "postgresql",
    "basketball_team": "bigquery"
}
TARGET_DB = "basketball_data"
CHUNK_SIZE = 50000

# --- PostgreSQL Utilities ---
def get_pg_engine(db_name):
    return create_engine(
        f"postgresql://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}@"
        f"{os.environ['PG_HOST']}:{os.environ.get('PG_PORT', '5432')}/{db_name}"
    )


def ensure_postgres_database_exists(db_name):
    conn = psycopg2.connect(
        host=os.environ['PG_HOST'],
        dbname='postgres',
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD']
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    if not cur.fetchone():
        print(f"🛠️ Creating PostgreSQL database: {db_name}")
        cur.execute(f'CREATE DATABASE "{db_name}"')
    else:
        print(f"✅ PostgreSQL database '{db_name}' already exists.")
    cur.close()
    conn.close()


def copy_from_stringio(df, table_name, pg_engine):
    buffer = pd.io.common.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    conn = pg_engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
        conn.commit()
        print(f"✅ Fast copied into {table_name}")
    except Exception as e:
        print(f"❌ COPY failed for {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# --- BigQuery to PostgreSQL ---
def migrate_bigquery_to_postgresql(db_name, source_db):
    bq_project = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(credentials=credentials, project=bq_project)
    bq_storage_client = BigQueryReadClient(credentials=credentials)

    pg_engine = get_pg_engine(db_name)
    dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=source_db)
    tables = list(bq_client.list_tables(dataset_ref))

    for table in tables:
        table_name = table.table_id
        full_table_id = f"{bq_project}.{source_db}.{table_name}"

        try:
            print(f"📥 Downloading {table_name} from BigQuery...")
            df = bq_client.list_rows(full_table_id).to_dataframe(bqstorage_client=bq_storage_client)
            if df.empty:
                print(f"⚠️ Skipping empty table: {table_name}")
                continue

            chunks = np.array_split(df, int(len(df) / CHUNK_SIZE) + 1)
            first_chunk = True
            for chunk in chunks:
                if chunk.empty:
                    continue
                chunk = chunk.where(pd.notnull(chunk), None)
                if_exists = "replace" if first_chunk else "append"
                chunk.head(0).to_sql(table_name, pg_engine, index=False, if_exists=if_exists)
                copy_from_stringio(chunk, table_name, pg_engine)
                first_chunk = False
            print(f"✅ Inserted {len(df)} rows into PostgreSQL table: {table_name}")
        except Exception as e:
            print(f"❌ Failed to migrate {table_name}: {e}")


# --- ClickHouse to PostgreSQL ---
def migrate_clickhouse_to_postgresql(db_name, source_db):
    ch_client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )
    pg_engine = get_pg_engine(db_name)
    tables = ch_client.query(f"SHOW TABLES FROM {source_db}").result_rows

    for (table,) in tables:
        try:
            print(f"📥 Reading from ClickHouse: {source_db}.{table}")
            df = ch_client.query_df(f"SELECT * FROM {source_db}.{table}")

            if df.empty:
                print(f"⚠️ Skipping empty table: {table}")
                continue

            df.head(0).to_sql(table, pg_engine, index=False, if_exists='replace')
            copy_from_stringio(df, table, pg_engine)
            print(f"✅ Migrated {table} to PostgreSQL")
        except Exception as e:
            print(f"❌ Failed to migrate {table}: {e}")

def migrate_postgres_to_postgresql(target_db, source_db):
    source_engine = get_pg_engine(source_db)
    target_engine = get_pg_engine(target_db)

    inspector = inspect(source_engine)
    tables = inspector.get_table_names(schema="public")

    for table in tables:
        try:
            print(f"📥 Copying table {table} from {source_db} to {target_db}...")
            df = pd.read_sql_table(table, con=source_engine, schema='public')
            if df.empty:
                print(f"⚠️ Skipping empty table: {table}")
                continue

            df.head(0).to_sql(table, target_engine, index=False, if_exists="replace")
            copy_from_stringio(df, table, target_engine)
            print(f"✅ Copied {table} to {target_db}")
        except Exception as e:
            print(f"❌ Failed to copy table {table}: {e}")


# --- Main Consolidation Logic ---
def consolidate_to_postgres():
    ensure_postgres_database_exists(TARGET_DB)

    for db_name, source in DATA_ORIGINS.items():
        print(f"\n🔄 Migrating {db_name} from {source} → postgresql ({TARGET_DB})")

        if source == "clickhouse":
            migrate_clickhouse_to_postgresql(db_name=TARGET_DB, source_db=db_name)
        elif source == "bigquery":
            migrate_bigquery_to_postgresql(db_name=TARGET_DB, source_db=db_name)
        elif source == "postgresql":
            migrate_postgres_to_postgresql(target_db=TARGET_DB, source_db=db_name)
        else:
            raise ValueError(f"Unsupported source DBMS: {source}")

    print(f"\n✅ All 15 tables should now be in the unified PostgreSQL DB: {TARGET_DB}")


if __name__ == "__main__":
    consolidate_to_postgres()