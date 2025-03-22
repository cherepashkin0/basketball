import os
import glob
import pandas as pd
from clickhouse_connect import get_client

# Set the desired chunk size (number of rows per insert)
CHUNK_SIZE = 50000
# Number of rows to use for type inference
SAMPLE_ROWS = 10000

dict_always_str = {'game_time': str, 'live_pc_time': str, 'natl_tv_broadcaster_abbreviation': str, 'wctimestring': str,
                    'wl_home': str, 'wl_away': str}

def infer_clickhouse_type(series, threshold=0.1):
    """
    Infer the ClickHouse type for a pandas Series.
    
    - Attempt to convert the series to numeric.
    - If more than 'threshold' fraction of non-null values fail conversion,
      then treat the column as a String.
    - Otherwise, use the native numeric type.
    - Wrap the type in Nullable() if any null values are present.
    """
    if series.name in dict_always_str:
        return "Nullable(String)"
    non_null_count = series.notnull().sum()
    numeric_series = pd.to_numeric(series, errors='coerce')
    conversion_failures = ((series.notnull()) & (numeric_series.isnull())).sum()
    
    if non_null_count > 0 and (conversion_failures / non_null_count) > threshold:
        inferred = 'String'
    else:
        if pd.api.types.is_integer_dtype(series):
            inferred = 'Int64'
        elif pd.api.types.is_float_dtype(series):
            inferred = 'Float64'
        elif pd.api.types.is_bool_dtype(series):
            inferred = 'UInt8'
        elif pd.api.types.is_datetime64_any_dtype(series):
            inferred = 'DateTime'
        else:
            inferred = 'String'
    
    if series.isnull().any():
        return f"Nullable({inferred})"
    else:
        return inferred

def process_csv_file(csv_file_path, client):
    """
    Process a single CSV file:
      1. Use the file name (without extension) as the table name.
      2. Read a sample of the file with pandas to detect column names and types.
      3. Drop the table if it exists and create a new table with the inferred types.
      4. Read and insert the CSV data in chunks using pandas.
    """
    # Use the file name (without extension) as the table name.
    table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    
    # Force game_time to be read as a string by specifying its dtype.
    sample_df = pd.read_csv(csv_file_path, nrows=SAMPLE_ROWS, dtype=dict_always_str)
    columns = sample_df.columns.tolist()
    
    # Infer ClickHouse types for each column using the sample.
    clickhouse_types = [infer_clickhouse_type(sample_df[col]) for col in columns]
    
    # Drop the table if it already exists.
    client.command(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Table '{table_name}' dropped (if it existed).")
    
    # Build and execute the CREATE TABLE query with the inferred column types.
    col_defs = ", ".join([f"`{col}` {ctype}" for col, ctype in zip(columns, clickhouse_types)])
    create_table_query = f"CREATE TABLE {table_name} ({col_defs}) ENGINE = MergeTree() ORDER BY tuple()"
    client.command(create_table_query)
    print(f"Table '{table_name}' created with columns and types: {list(zip(columns, clickhouse_types))}")
    
    total_rows = 0
    # Process the CSV file in chunks, also forcing game_time to be a string.
    for chunk in pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE, dtype=dict_always_str):
        # Replace NaN with None so that ClickHouse can handle missing values.
        chunk = chunk.where(pd.notnull(chunk), None)
        data = chunk.values.tolist()
        client.insert(table=table_name, data=data, column_names=columns)
        total_rows += len(chunk)
        print(f"Inserted a chunk of {len(chunk)} rows into '{table_name}'. Total inserted: {total_rows}")
    
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
    print(os.listdir(csv_dir))
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
