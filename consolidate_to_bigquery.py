import os
import pandas as pd
import numpy as np
import clickhouse_connect
import psycopg2
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery_storage_v1 import BigQueryReadClient

# Configuration
DATA_ORIGINS = {
    "basketball_game": "clickhouse",
    "basketball_player": "postgresql",
    "basketball_team": "bigquery"
}
TARGET_DB = "basketball_data"
CHUNK_SIZE = 50000

# --- BQ Utilities ---
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    )
    return bigquery.Client(credentials=credentials, project=os.environ['GCP_PROJECT_ID'])

def ensure_bigquery_dataset_exists(dataset_name):
    client = get_bq_client()
    dataset_id = f"{client.project}.{dataset_name}"
    dataset_ref = bigquery.Dataset(dataset_id)
    dataset_ref.location = "us-central1"

    try:
        client.get_dataset(dataset_id)
        print(f"✅ BigQuery dataset '{dataset_id}' already exists.")
    except:
        client.create_dataset(dataset_ref)
        print(f"🛠️ Created BigQuery dataset: {dataset_id}")

# --- PostgreSQL to BigQuery ---
def get_pg_engine(db_name):
    return create_engine(
        f"postgresql://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}@"
        f"{os.environ['PG_HOST']}:{os.environ.get('PG_PORT', '5432')}/{db_name}"
    )

def migrate_postgresql_to_bigquery(db_name, source_db):
    engine = get_pg_engine(source_db)
    client = get_bq_client()
    tables = engine.table_names()

    for table in tables:
        try:
            print(f"📥 Reading table {table} from PostgreSQL...")
            df = pd.read_sql_table(table, con=engine)
            if df.empty:
                print(f"⚠️ Skipping empty table: {table}")
                continue

            table_id = f"{client.project}.{db_name}.{table}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            print(f"📤 Uploading {table} to BigQuery...")
            load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            load_job.result()
            print(f"✅ Loaded {table} to BigQuery")
        except Exception as e:
            print(f"❌ Failed to load table {table}: {e}")

# --- ClickHouse to BigQuery ---
def migrate_clickhouse_to_bigquery(db_name, source_db):
    ch_client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )
    bq_client = get_bq_client()

    tables = ch_client.query(f"SHOW TABLES FROM {source_db}").result_rows
    for (table,) in tables:
        try:
            print(f"📥 Reading from ClickHouse: {source_db}.{table}")
            result = ch_client.query(f"SELECT * FROM {source_db}.{table}")
            df = pd.DataFrame(result.result_rows, columns=result.column_names)

            if df.empty:
                print(f"⚠️ Skipping empty table: {table}")
                continue

            table_id = f"{bq_client.project}.{db_name}.{table}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            print(f"📤 Uploading to BigQuery: {table_id}")
            load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
            load_job.result()
            print(f"✅ Migrated {table} to BigQuery")
        except Exception as e:
            print(f"❌ Failed to migrate {table}: {e}")

# --- BigQuery to BigQuery (copy within project) ---
def migrate_bigquery_to_bigquery(db_name, source_db):
    bq_client = get_bq_client()
    source_dataset = f"{bq_client.project}.{source_db}"
    target_dataset = f"{bq_client.project}.{db_name}"
    tables = list(bq_client.list_tables(source_dataset))

    for table in tables:
        source_table_id = f"{source_dataset}.{table.table_id}"
        target_table_id = f"{target_dataset}.{table.table_id}"
        try:
            print(f"📤 Copying {source_table_id} to {target_table_id}")
            job = bq_client.copy_table(source_table_id, target_table_id)
            job.result()
            print(f"✅ Copied {table.table_id} to BigQuery target dataset")
        except Exception as e:
            print(f"❌ Failed to copy {table.table_id}: {e}")

# --- Main Consolidation Logic ---
def consolidate_to_bigquery():
    ensure_bigquery_dataset_exists(TARGET_DB)

    for db_name, source in DATA_ORIGINS.items():
        print(f"\n🔄 Migrating {db_name} from {source} → BigQuery ({TARGET_DB})")

        if source == "clickhouse":
            migrate_clickhouse_to_bigquery(db_name=TARGET_DB, source_db=db_name)
        elif source == "bigquery":
            migrate_bigquery_to_bigquery(db_name=TARGET_DB, source_db=db_name)
        elif source == "postgresql":
            migrate_postgresql_to_bigquery(db_name=TARGET_DB, source_db=db_name)
        else:
            raise ValueError(f"Unsupported source DBMS: {source}")

    print(f"\n✅ All 15 tables should now be in the unified BigQuery dataset: {TARGET_DB}")


if __name__ == "__main__":
    consolidate_to_bigquery()
