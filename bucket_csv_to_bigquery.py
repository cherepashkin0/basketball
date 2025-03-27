import os
from google.cloud import storage, bigquery

# Retrieve configuration from environment variables
project_id = os.environ.get("PROJECT_ID")
dataset_id = os.environ.get("DATASET_ID")
bucket_name = os.environ.get("BUCKET_NAME")
prefix = "csv/csv/"

# Initialize BigQuery and Storage clients
bq_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)

# Create the dataset if it doesn't exist
dataset_ref = bigquery.Dataset(f"{dataset_id}")
try:
    bq_client.create_dataset(dataset_ref)
    print(f"Created dataset {dataset_id}")
except Exception as e:
    print(f"Dataset {dataset_id} may already exist: {e}")

# List CSV files from the bucket directory
bucket = storage_client.get_bucket(bucket_name)
blobs = bucket.list_blobs(prefix=prefix)

# Filter for CSV files and create full gs:// URIs
csv_files = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith('.csv')]

for csv_uri in csv_files:
    # Derive a table name from the CSV file (e.g., basketball/csv/file1.csv -> file1)
    table_name = os.path.basename(csv_uri).split('.')[0]
    full_table_id = f"{dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,            # Let BigQuery infer the schema
        skip_leading_rows=1         # Skip header rows if present
    )
    
    print(f"Loading {csv_uri} into table {full_table_id}...")
    load_job = bq_client.load_table_from_uri(csv_uri, full_table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete.
    print(f"Loaded {csv_uri} into {full_table_id} successfully.")
