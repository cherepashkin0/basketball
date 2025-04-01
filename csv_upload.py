import os
import glob
import json
import argparse
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, inspect, text
from clickhouse_connect import get_client
from google.cloud import bigquery 
from google.cloud.exceptions import NotFound



CHUNK_SIZE_POSTGRES = 500_000
CHUNK_SIZE_CLICKHOUSE = 500_000

CSV_DIR = "/home/wsievolod/projects/basketball/dataset/csv/"
TABLES_DICT_FILE = "tables_dict.json"

dict_always_str = {
    'game_time': str,
    'live_pc_time': str,
    'natl_tv_broadcaster_abbreviation': str,
    'wctimestring': str,
    'wl_home': str,
    'wl_away': str,
    'scoremargin': str,
    'fg3_pct_home': str,
    'fg3_pct_away': str
}

def load_tables_dict(filepath: str) -> dict:
    with open(filepath, 'r') as file:
        return json.load(file)

def find_csv_files(directory: str, table_names: set) -> list:
    all_csv_files = glob.glob(os.path.join(directory, "*.csv"))
    return [f for f in all_csv_files if os.path.splitext(os.path.basename(f))[0] in table_names]

def upload_csv_to_postgres(engine, file_path: str):
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    create_postgres_table(engine, file_path)
    chunk_iterator = pd.read_csv(file_path, chunksize=CHUNK_SIZE_POSTGRES)
    for chunk in tqdm(chunk_iterator, desc=f"Uploading {table_name}", unit="chunk"):
        chunk.to_sql(table_name, con=engine, if_exists='append', index=False)


def get_postgres_engine(database_env_var: str):
    PG_HOST = os.environ.get("PG_HOST")
    PG_PORT = os.environ.get("PG_PORT", 5432)
    PG_USER = os.environ.get("PG_USER")
    PG_PASSWORD = os.environ.get("PG_PASSWORD")
    PG_DATABASE = os.environ.get(database_env_var)

    required_vars = {"PG_HOST": PG_HOST, "PG_USER": PG_USER, "PG_PASSWORD": PG_PASSWORD, database_env_var: PG_DATABASE}
    missing_vars = [k for k, v in required_vars.items() if v is None]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

    return create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}')


def clear_postgres_database(engine):
    inspector = inspect(engine)
    with engine.connect() as conn:
        for table_name in inspector.get_table_names():
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))            

def get_clickhouse_client(database_env_var: str):
    env_vars = {
        "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST"),
        "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT", 8123),
        "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER"),
        "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
        database_env_var: os.getenv(database_env_var)
    }

    missing_vars = [key for key, value in env_vars.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

    return get_client(
        host=env_vars["CLICKHOUSE_HOST"],
        port=int(env_vars["CLICKHOUSE_PORT"]),
        username=env_vars["CLICKHOUSE_USER"],
        password=env_vars["CLICKHOUSE_PASSWORD"],
        database=env_vars[database_env_var]
    )

def upload_to_postgres(table_type, database_env_var, clear_db):
    tables_dict = load_tables_dict(TABLES_DICT_FILE)
    selected_table_names = set(tables_dict[table_type])
    engine = get_postgres_engine(database_env_var)

    if clear_db:
        print("Clearing PostgreSQL database...")
        clear_postgres_database(engine)
        print("PostgreSQL database cleared.")

    csv_files = find_csv_files(CSV_DIR, selected_table_names)

    if not csv_files:
        print(f"No {table_type} CSV files found.")
        return

    print(f"Found {len(csv_files)} {table_type} CSV files. Uploading...")

    for file_path in tqdm(csv_files, desc=f"Uploading {table_type} files", unit="file"):
        upload_csv_to_postgres(engine, file_path)

    print("CSV files have been uploaded successfully!")

def merge_inferred_types(type1, type2):
    """Merges inferred data types from different chunks into a compatible type."""
    if type1 == type2:
        return type1

    type_set = {type1, type2}

    if "string" in type_set:
        return "string"

    if type_set <= {"Int64", "Float64", "boolean"}: #  set comparison to check if both types are int he numeric type
        return "Float64" if "Float64" in type_set else "Int64"

    if "datetime64[ns]" in type_set:
        return "string"

    return "string"

def infer_numeric_type(series: pd.Series) -> str:
    if pd.api.types.is_float_dtype(series):
        return "Float32" if series.apply(lambda x: x is None or abs(x) < 1e38).all() else "Float64"
    
    if pd.api.types.is_integer_dtype(series):
        min_val, max_val = series.min(), series.max()
        if -128 <= min_val <= max_val <= 127:
            return "Int8"
        elif -32768 <= min_val <= max_val <= 32767:
            return "Int16"
        elif -2**31 <= min_val <= max_val <= 2**31 - 1:
            return "Int32"
        else:
            return "Int64"
    
    return "Float64"  # fallback

def infer_string_type(series: pd.Series, threshold: int = 255) -> str:
    max_len = series.dropna().map(len).max() if not series.dropna().empty else 0
    return f"FixedString({max_len})" if max_len <= threshold else "String"

def get_final_types(csv_file_path):
    final_types = {}
    for chunk in pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE_CLICKHOUSE, dtype=dict_always_str, low_memory=False):
        chunk = chunk.convert_dtypes()
        for col in chunk.columns:
            inferred_dtype = str(chunk[col].dtype)
            is_nullable = chunk[col].isnull().any()
            if col not in final_types:
                final_types[col] = {
                    "dtype": inferred_dtype,
                    "nullable": is_nullable,
                    "values": chunk[col]
                }
            else:
                prev = final_types[col]
                prev["nullable"] |= is_nullable
                prev["values"] = pd.concat([prev["values"], chunk[col]])

    inferred_types = {}
    for col, info in final_types.items():
        series = info["values"]
        dtype = "string" if col in dict_always_str else str(series.infer_objects().dtype)

        if pd.api.types.is_numeric_dtype(series):
            final_type = infer_numeric_type(series)
        elif pd.api.types.is_string_dtype(series):
            final_type = infer_string_type(series)
        elif pd.api.types.is_bool_dtype(series):
            final_type = "UInt8"
        elif pd.api.types.is_datetime64_any_dtype(series):
            final_type = "DateTime"
        else:
            final_type = "String"

        inferred_types[col] = (final_type, info["nullable"])

    return inferred_types


def clear_clickhouse_database(client):
    result = client.command('SHOW TABLES')
    tables = result.result_rows if hasattr(result, 'result_rows') else result.fetchall() if hasattr(result, 'fetchall') else []

    for row in tables:
        table = row[0] if isinstance(row, (list, tuple)) else row
        if table:
            client.command(f'DROP TABLE IF EXISTS {table}')

def process_csv_to_clickhouse(csv_file_path, client):
    table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    final_types = get_final_types(csv_file_path)
    columns = list(final_types.keys())

    clickhouse_types = []
    for col in columns:
        base_type, is_nullable = final_types[col]
        ch_type = type_mapping.get(base_type, "String")
        col_type = f"Nullable({ch_type})" if is_nullable else ch_type
        clickhouse_types.append(col_type)

    client.command(f"DROP TABLE IF EXISTS {table_name}")

    col_defs = ", ".join([f"`{col}` {ctype}" for col, ctype in zip(columns, clickhouse_types)])
    create_query = f"CREATE TABLE {table_name} ({col_defs}) ENGINE = MergeTree() ORDER BY tuple()"
    client.command(create_query)

    for chunk in tqdm(pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE_CLICKHOUSE, dtype=dict_always_str), desc=f"Uploading {table_name}", unit="chunk"):
        chunk = chunk.where(pd.notnull(chunk), None)
        client.insert(table=table_name, data=chunk.values.tolist(), column_names=columns)

def upload_to_clickhouse(table_type, database_env_var, clear_db):
    client = get_clickhouse_client(database_env_var)

    if clear_db:
        print("Clearing ClickHouse database...")
        clear_clickhouse_database(client)
        print("ClickHouse database cleared.")

    tables_dict = load_tables_dict(TABLES_DICT_FILE)
    tables_to_upload = set(tables_dict[table_type])
    csv_files = find_csv_files(CSV_DIR, tables_to_upload)

    if not csv_files:
        print(f"No {table_type} CSV files found.")
        return

    for csv_file in tqdm(csv_files, desc=f"Processing {table_type} files", unit="file"):
        process_csv_to_clickhouse(csv_file, client)


def get_bigquery_client():
    return bigquery.Client()

def upload_csv_to_bigquery(client, csv_file_path, dataset_id):
    table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    table_id = f"{client.project}.{dataset_id}.{table_name}"

    # Delete existing table if it exists
    if args.clear_db:
        try:
            client.delete_table(table_id)
            print(f"Deleted existing table: {table_id}")
        except NotFound:
            print(f"Table {table_id} does not exist. Proceeding with upload.")

    schema = infer_bigquery_schema(csv_file_path)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE"  # Optional redundancy
    )

    with open(csv_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()
    print(f"Uploaded {csv_file_path} to {table_id}")


def ensure_bigquery_dataset(client, dataset_id):
    project = client.project
    dataset_ref = bigquery.DatasetReference(project, dataset_id)

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Optional: make this configurable
        client.create_dataset(dataset)
        print(f"Created BigQuery dataset: {dataset_id}")


def upload_to_bigquery(table_type, dataset_env_var):
    client = get_bigquery_client()
    dataset_id = os.getenv(dataset_env_var)
    
    if not dataset_id:
        raise ValueError(f"Missing environment variable: {dataset_env_var}")
    
    ensure_bigquery_dataset(client, dataset_id)
    
    tables_dict = load_tables_dict(TABLES_DICT_FILE)
    selected_table_names = set(tables_dict[table_type])
    csv_files = find_csv_files(CSV_DIR, selected_table_names)

    if not csv_files:
        print(f"No {table_type} CSV files found.")
        return

    for file_path in tqdm(csv_files, desc=f"Uploading {table_type} to BigQuery", unit="file"):
        print(297, file_path)
        # Step 1: Preprocess file to clean float integers
        cleaned_file_path = file_path.replace(".csv", "_cleaned.csv")
        preprocess_csv_for_bigquery(file_path, cleaned_file_path)


        # Step 2: Upload cleaned file
        upload_csv_to_bigquery(client, cleaned_file_path, dataset_id)

    print("CSV files have been uploaded to BigQuery!")


def infer_postgres_type(series: pd.Series) -> str:
    if pd.api.types.is_float_dtype(series):
        return "REAL" if series.apply(lambda x: x is None or abs(x) < 1e38).all() else "DOUBLE PRECISION"
    
    if pd.api.types.is_integer_dtype(series):
        min_val, max_val = series.min(), series.max()
        if -32768 <= min_val <= max_val <= 32767:
            return "SMALLINT"
        elif -2**31 <= min_val <= max_val <= 2**31 - 1:
            return "INTEGER"
        else:
            return "BIGINT"

    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"

    if pd.api.types.is_string_dtype(series):
        max_len = series.dropna().map(len).max() if not series.dropna().empty else 0
        return f"VARCHAR({max_len})" if max_len <= 255 else "TEXT"

    return "TEXT"

def create_postgres_table(engine, file_path: str):
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f"Creating PostgreSQL table: {table_name}")

    for chunk in pd.read_csv(file_path, chunksize=1, dtype=dict_always_str):
        chunk = chunk.convert_dtypes()
        break  # Only need first chunk to infer schema

    col_defs = []
    for col in chunk.columns:
        series = chunk[col]
        dtype = infer_postgres_type(series)
        col_defs.append(f'"{col}" {dtype}')

    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)});'

    with engine.connect() as conn:
        conn.execute(text(ddl))

def infer_bigquery_schema(csv_file_path: str) -> list:
    schema = []
    sample = pd.read_csv(csv_file_path, nrows=1000, dtype=dict_always_str)
    sample = sample.convert_dtypes()

    for col in sample.columns:
        # Use STRING if the column is explicitly forced to string
        if col in dict_always_str:
            field_type = "STRING"
        else:
            series = sample[col]
            if pd.api.types.is_integer_dtype(series):
                field_type = "INT64"
            elif pd.api.types.is_float_dtype(series):
                field_type = "FLOAT64"
            elif pd.api.types.is_bool_dtype(series):
                field_type = "BOOL"
            elif pd.api.types.is_datetime64_any_dtype(series):
                field_type = "DATETIME"
            else:
                field_type = "STRING"

        schema.append(bigquery.SchemaField(col, field_type))

    return schema


def preprocess_csv_for_bigquery(input_path: str, output_path: str, chunk_size: int = 100_000):
    first_chunk = True
    total_lines = sum(1 for _ in open(input_path)) - 1  # Exclude header
    num_chunks = (total_lines // chunk_size) + 1

    for chunk in pd.read_csv(input_path, dtype=dict_always_str, chunksize=chunk_size): 
    # tqdm(pd.read_csv(input_path, dtype=dict_always_str, chunksize=chunk_size), desc="Preprocessing CSV for BigQuery", total=num_chunks, unit="chunk"):
        for col in chunk.columns:
            if col in dict_always_str:
                continue  # Force to string, skip type conversion

            if pd.api.types.is_float_dtype(chunk[col]):
                if chunk[col].dropna().apply(lambda x: float(x).is_integer()).all():
                    chunk[col] = chunk[col].astype("Int64")  # Nullable integers
            elif pd.api.types.is_object_dtype(chunk[col]):
                try:
                    converted = pd.to_numeric(chunk[col], errors='coerce')
                    if converted.dropna().apply(lambda x: float(x).is_integer()).all():
                        chunk[col] = converted.astype("Int64")
                except Exception:
                    pass

        chunk.to_csv(output_path, mode='w' if first_chunk else 'a', header=first_chunk, index=False)
        first_chunk = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload CSV files to either PostgreSQL or ClickHouse")
    parser.add_argument("--target", required=True, choices=['postgres', 'clickhouse', 'bigquery'], help="Target database to upload")    
    parser.add_argument("--table_type", required=True, choices=['game_specific', 'player_specific', 'team_specific', 'test_specific'], help="Type of tables to upload")
    parser.add_argument("--database_env_var", required=True, help="Environment variable name for the target database")
    parser.add_argument("--clear_db", action="store_true", help="Clear the target database before uploading")
    args = parser.parse_args()

    if args.target == "postgres":
        upload_to_postgres(args.table_type, args.database_env_var, args.clear_db)
    elif args.target == "clickhouse":
        upload_to_clickhouse(args.table_type, args.database_env_var, args.clear_db)
    elif args.target == "bigquery":
        upload_to_bigquery(args.table_type, args.database_env_var)