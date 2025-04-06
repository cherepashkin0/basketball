import os
import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from google.oauth2 import service_account




# Load credentials from environment
pg_host = os.environ['PG_HOST']
pg_user = os.environ['PG_USER']
pg_db = os.environ['DATABASE_PLAYER']
pg_password = os.environ['PG_PASSWORD']
pg_port = os.environ.get('PG_PORT')

bq_project = os.environ['GCP_PROJECT_ID']
bq_dataset = os.environ['DATABASE_PLAYER']
credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

# Initialize clients
pg_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
pg_engine = create_engine(pg_url)

credentials = service_account.Credentials.from_service_account_file(credentials_path)
bq_client = bigquery.Client(credentials=credentials, project=bq_project)

# Construct DatasetReference with project + dataset ID
dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=bq_dataset)

try:
    bq_client.get_dataset(dataset_ref)
    print(f"✅ Dataset {bq_dataset} already exists in project {bq_project}.")
except Exception:
    dataset = bigquery.Dataset(dataset_ref)  # DatasetRef carries the project info correctly
    dataset.location = "us-central1"  # or your preferred location
    bq_client.create_dataset(dataset)
    print(f"📦 Created BigQuery dataset: {bq_dataset}")

# Get all PostgreSQL tables in the public schema
with pg_engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """))
    tables = [row[0] for row in result]

# Migrate each table
for table in tables:
    try:
        print(f"📥 Reading table {table} from PostgreSQL...")
        df = pd.read_sql_table(table, con=pg_engine, schema='public')

        if df.empty:
            print(f"⚠️ Skipping empty table: {table}")
            continue

        bq_table_id = f"{bq_project}.{bq_dataset}.{table}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        print(f"📤 Uploading to BigQuery: {bq_table_id}")
        load_job = bq_client.load_table_from_dataframe(df, bq_table_id, job_config=job_config)
        load_job.result()  # Waits for job to finish
        print(f"✅ Loaded {table} to BigQuery")

    except Exception as e:
        print(f"❌ Failed to load table {table}: {e}")
