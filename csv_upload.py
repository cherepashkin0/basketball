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
import io
import csv


CHUNK_SIZE_POSTGRES = 1_000_000
CHUNK_SIZE_CLICKHOUSE = 1_000_000

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

type_mapping = {
    "Int8": "Int8",
    "Int16": "Int16",
    "Int32": "Int32",
    "Int64": "Int64",
    "UInt8": "UInt8",
    "UInt16": "UInt16",
    "UInt32": "UInt32",
    "UInt64": "UInt64",
    "Float32": "Float32",
    "Float64": "Float64",
    "string": "String",
    "String": "String",
    "FixedString(255)": "FixedString(255)",
    "DateTime": "DateTime",
    "boolean": "UInt8",  # In ClickHouse, booleans are typically stored as UInt8
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
        chunk.to_sql(table_name, engine, if_exists='append', index=False, method=psql_insert_copy)


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
    # Read the CSV file in chunks and infer types
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
        # dtype = "string" if col in dict_always_str else str(series.infer_objects().dtype)

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

def upload_csv_to_bigquery_stream(client, csv_file_path, dataset_id, clear_db=False):
    table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    table_id = f"{client.project}.{dataset_id}.{table_name}"

    if clear_db:
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
        write_disposition="WRITE_TRUNCATE"
    )

    # Upload the entire CSV in-memory stream in chunks
    full_stream = io.StringIO()
    for buffer in preprocess_csv_for_bigquery_stream(csv_file_path):
        full_stream.write(buffer.read())
    full_stream.seek(0)

    load_job = client.load_table_from_file(full_stream, table_id, job_config=job_config)
    load_job.result()
    print(f"Uploaded {csv_file_path} to {table_id}")



def ensure_bigquery_dataset(client, dataset_id, clear_db=False):
    project = client.project
    dataset_ref = bigquery.DatasetReference(project, dataset_id)

    if clear_db:
        try:
            client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
            print(f"Deleted existing dataset: {dataset_id}")
        except Exception as e:
            print(f"Error deleting dataset {dataset_id}: {e}")

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Optional: make this configurable
        client.create_dataset(dataset)
        print(f"Created BigQuery dataset: {dataset_id}")



def upload_to_bigquery(table_type, dataset_env_var, clear_db):
    client = get_bigquery_client()
    dataset_id = os.getenv(dataset_env_var)

    if not dataset_id:
        raise ValueError(f"Missing environment variable: {dataset_env_var}")

    ensure_bigquery_dataset(client, dataset_id, clear_db=clear_db)

    tables_dict = load_tables_dict(TABLES_DICT_FILE)
    selected_table_names = set(tables_dict[table_type])
    csv_files = find_csv_files(CSV_DIR, selected_table_names)

    if not csv_files:
        print(f"No {table_type} CSV files found.")
        return

    for file_path in tqdm(csv_files, desc=f"Uploading {table_type} to BigQuery", unit="file"):
        upload_csv_to_bigquery_stream(client, file_path, dataset_id, clear_db=clear_db)

    print("CSV files have been uploaded to BigQuery!")



def infer_postgres_type(series: pd.Series) -> str:
    if pd.api.types.is_float_dtype(series):
        return "REAL" if series.dropna().apply(lambda x: abs(x) < 1e38).all() else "DOUBLE PRECISION"
    
    if pd.api.types.is_integer_dtype(series):
        numeric_series = series.dropna()
        if numeric_series.empty:
            return "INTEGER"  # Default fallback for empty integer series

        min_val, max_val = numeric_series.min(), numeric_series.max()

        if pd.isna(min_val) or pd.isna(max_val):
            return "INTEGER"  # Default fallback if min/max is NA

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
        max_len = series.dropna().map(len).max() if not series.dropna().empty else 10
        if max_len == 0:
            max_len = 10  # Avoid VARCHAR(0)
        return f"VARCHAR({max_len})" if max_len <= 255 else "TEXT"

    return "TEXT"


def infer_schema_from_multiple_chunks(file_path: str, dtype_dict: dict, sample_chunks: int = 5):
    final_schema = {}

    chunk_iterator = pd.read_csv(file_path, chunksize=10000, dtype=dtype_dict)
    chunks_read = 0

    for chunk in chunk_iterator:
        chunk = chunk.convert_dtypes()
        for col in chunk.columns:
            series = chunk[col]

            current_type = infer_postgres_type(series)

            # Merge inferred types safely
            if col not in final_schema:
                final_schema[col] = current_type
            else:
                final_schema[col] = merge_postgres_types(final_schema[col], current_type)

        chunks_read += 1
        if chunks_read >= sample_chunks:
            break  # Limit to first few chunks to avoid long runtime

    return final_schema

def merge_postgres_types(type1, type2):
    priority = {
        "TEXT": 5,
        "VARCHAR": 4,
        "DOUBLE PRECISION": 3,
        "REAL": 2,
        "BIGINT": 1,
        "INTEGER": 0,
        "SMALLINT": -1,
        "BOOLEAN": -2,
        "TIMESTAMP": -3
    }

    # Simple logic: higher priority type overrides lower priority type
    type1_base = "VARCHAR" if type1.startswith("VARCHAR") else type1
    type2_base = "VARCHAR" if type2.startswith("VARCHAR") else type2

    chosen_type = type1 if priority[type1_base] >= priority[type2_base] else type2

    # For VARCHAR, take the largest length seen
    if "VARCHAR" in [type1_base, type2_base]:
        len1 = int(type1[type1.find("(")+1:type1.find(")")]) if "VARCHAR" in type1 else 0
        len2 = int(type2[type2.find("(")+1:type2.find(")")]) if "VARCHAR" in type2 else 0
        chosen_type = f"VARCHAR({max(len1, len2, 10)})"  # minimum 10 to avoid VARCHAR(0)

        if max(len1, len2) > 255:
            chosen_type = "TEXT"

    return chosen_type


def create_postgres_table(engine, file_path: str):
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f"Creating PostgreSQL table: {table_name}")

    schema = infer_schema_from_multiple_chunks(file_path, dict_always_str)

    col_defs = [f'"{col}" {dtype}' for col, dtype in schema.items()]

    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)});'

    with engine.connect() as conn:
        conn.execute(text(ddl))


def infer_bigquery_schema(csv_file_path: str) -> list:
    schema = []
    # todo: change nrows=1000 to chunks. in case first nrows may be NaN or have different value (float instead of integer)
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


def preprocess_csv_for_bigquery_stream(input_path: str, chunk_size: int = 100_000):
    first_chunk = True
    for chunk in pd.read_csv(input_path, dtype=dict_always_str, chunksize=chunk_size):
        for col in chunk.columns:
            if col in dict_always_str:
                continue
            if pd.api.types.is_float_dtype(chunk[col]):
                if chunk[col].dropna().apply(lambda x: float(x).is_integer()).all():
                    chunk[col] = chunk[col].astype("Int64")
            elif pd.api.types.is_object_dtype(chunk[col]):
                try:
                    converted = pd.to_numeric(chunk[col], errors='coerce')
                    if converted.dropna().apply(lambda x: float(x).is_integer()).all():
                        chunk[col] = converted.astype("Int64")
                except Exception:
                    pass

        buffer = io.StringIO()
        chunk.to_csv(buffer, header=first_chunk, index=False)
        buffer.seek(0)
        yield buffer
        first_chunk = False


def psql_insert_copy(table, conn, keys, data_iter):
    with conn.connection.cursor() as cursor:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerows(data_iter)
        buffer.seek(0)

        columns = ', '.join(f'"{k}"' for k in keys)
        table_name = table.name if hasattr(table, 'name') else table
        sql = f'COPY "{table_name}" ({columns}) FROM STDIN WITH CSV'
        cursor.copy_expert(sql=sql, file=buffer)


def main():
    parser = argparse.ArgumentParser(description="Upload CSV files to either PostgreSQL or ClickHouse")
    parser.add_argument("--target", required=True, choices=['postgres', 'clickhouse', 'bigquery'], help="Target database to upload")    
    parser.add_argument("--table_type", required=True, choices=['game_specific', 'player_specific', 'team_specific', 'test_specific'], help="Type of tables to upload")
    parser.add_argument("--database_env_var", required=True, help="Environment variable name for the target database")
    parser.add_argument("--clear_db", action="store_true", help="Clear the target database before uploading")
    args = parser.parse_args()

    clear_db = args.clear_db

    if args.target == "postgres":
        upload_to_postgres(args.table_type, args.database_env_var, clear_db)
    elif args.target == "clickhouse":
        upload_to_clickhouse(args.table_type, args.database_env_var, clear_db)
    elif args.target == "bigquery":
        upload_to_bigquery(args.table_type, args.database_env_var, clear_db)

if __name__ == "__main__":
    main()