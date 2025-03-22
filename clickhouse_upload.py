import os
import glob
import pandas as pd
from clickhouse_connect import get_client

# Set the desired chunk size (number of rows per insert)
CHUNK_SIZE = 50000

# Always read these columns as string.
dict_always_str = {
    'game_time': str,
    'live_pc_time': str,
    'natl_tv_broadcaster_abbreviation': str,
    'wctimestring': str,
    'wl_home': str,
    'wl_away': str,
    'scoremargin': str
}

def infer_chunk_clickhouse_type(series, threshold=0.1):
    """
    Infer the base ClickHouse type for a pandas Series from one chunk.
    Returns a tuple (base_type, is_nullable) where base_type is one of:
      "String", "Int64", "Float64", "UInt8", "DateTime"
    and is_nullable is True if any nulls are present.
    
    If the column name is in dict_always_str, always return "String".
    """
    # Always force string for specified columns.
    if series.name in dict_always_str:
        return ("String", series.isnull().any())
    
    non_null_count = series.notnull().sum()
    numeric_series = pd.to_numeric(series, errors='coerce')
    conversion_failures = ((series.notnull()) & (numeric_series.isnull())).sum()
    
    # If many values can’t be converted to numeric, treat as string.
    if non_null_count > 0 and (conversion_failures / non_null_count) > threshold:
        inferred = "String"
    else:
        if pd.api.types.is_integer_dtype(series):
            inferred = "Int64"
        elif pd.api.types.is_float_dtype(series):
            inferred = "Float64"
        elif pd.api.types.is_bool_dtype(series):
            inferred = "UInt8"
        elif pd.api.types.is_datetime64_any_dtype(series):
            inferred = "DateTime"
        else:
            inferred = "String"
    
    is_nullable = series.isnull().any()
    return (inferred, is_nullable)

def merge_base_types(type1, type2):
    """
    Merge two inferred base types according to these rules:
      - If either is "String", return "String".
      - If one is Float64 and the other Int64 (or UInt8), return "Float64".
      - If one is Int64 and the other UInt8, return "Int64".
      - If both are the same, return that type.
      - Otherwise, default to "String".
    """
    if type1 == type2:
        return type1
    if "String" in (type1, type2):
        return "String"
    numeric_types = {"Int64", "Float64", "UInt8"}
    if type1 in numeric_types and type2 in numeric_types:
        if "Float64" in (type1, type2):
            return "Float64"
        else:
            return "Int64"
    # For DateTime conflicts or any other mismatch, fallback to String.
    if "DateTime" in (type1, type2):
        return "String"
    return "String"

def get_final_types(csv_file_path):
    """
    Read the entire CSV file in chunks and merge the inferred type
    (base type and nullability) for each column.
    Returns a dictionary: { column_name: (final_base_type, is_nullable) }
    """
    final_types = {}
    for chunk in pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE, dtype=dict_always_str, low_memory=False):
        for col_idx, col in enumerate(chunk.columns):
            print(87, col_idx,col)
            inferred, is_nullable = infer_chunk_clickhouse_type(chunk[col])
            if col not in final_types:
                final_types[col] = (inferred, is_nullable)
            else:
                prev_type, prev_nullable = final_types[col]
                merged_type = merge_base_types(prev_type, inferred)
                merged_nullable = prev_nullable or is_nullable
                final_types[col] = (merged_type, merged_nullable)
    return final_types

def process_csv_file(csv_file_path, client):
    """
    Process a single CSV file:
      1. Use the file name (without extension) as the table name.
      2. Read the CSV in chunks to merge inferred types across all chunks.
      3. Drop the table if it exists and create a new table with the final types.
      4. Read and insert the CSV data in chunks using pandas.
    """
    # Use the file name (without extension) as the table name.
    table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    
    print(f"Inferring column types for file: {csv_file_path}")
    final_types = get_final_types(csv_file_path)
    columns = list(final_types.keys())
    
    # Build the list of ClickHouse types; if nullable then wrap with Nullable(...)
    clickhouse_types = []
    for col in columns:
        base_type, is_nullable = final_types[col]
        if is_nullable:
            col_type = f"Nullable({base_type})"
        else:
            col_type = base_type
        clickhouse_types.append(col_type)
    
    # Drop table if exists.
    client.command(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Table '{table_name}' dropped (if it existed).")
    
    # Build and execute the CREATE TABLE query.
    col_defs = ", ".join([f"`{col}` {ctype}" for col, ctype in zip(columns, clickhouse_types)])
    create_table_query = f"CREATE TABLE {table_name} ({col_defs}) ENGINE = MergeTree() ORDER BY tuple()"
    client.command(create_table_query)
    print(f"Table '{table_name}' created with columns and types: {list(zip(columns, clickhouse_types))}")
    
    total_rows = 0
    # Second pass: read CSV in chunks and insert the data.
    for chunk in pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE, dtype=dict_always_str):
        # Replace NaN with None so that ClickHouse can handle missing values.
        chunk = chunk.where(pd.notnull(chunk), None)
        data = chunk.values.tolist()
        client.insert(table=table_name, data=data, column_names=columns)
        total_rows += len(chunk)
        print(f"Inserted chunk of {len(chunk)} rows into '{table_name}'. Total inserted: {total_rows}")
    
    print(f"Finished processing '{csv_file_path}'. Total rows inserted: {total_rows}")

def main():
    # Get the ClickHouse password from the environment variable.
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD")
    if clickhouse_password is None:
        raise ValueError("Environment variable CLICKHOUSE_PASSWORD is not set.")
    
    # Establish a connection using clickhouse-connect.
    client = get_client(
        host='34.70.242.157',
        port=8123,
        username='seva_admin',
        password=clickhouse_password,
        database='basketball'
    )
    
    # Directory containing CSV files.
    csv_dir = "/home/wsievolod/projects/datasets/basketball/csv/"
    print("Files in CSV directory:", os.listdir(csv_dir))
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    if not csv_files:
        print("No CSV files found in directory:", csv_dir)
        return
    
    # Process each CSV file.
    for csv_file in csv_files:
        print(f"\nProcessing file: {csv_file}")
        process_csv_file(csv_file, client)

if __name__ == "__main__":
    main()
