import os
import glob
import pandas as pd
from clickhouse_connect import get_client

# Set the desired chunk size (number of rows per insert)
CHUNK_SIZE = 10000

def map_dtype_to_clickhouse(pd_dtype):
    """
    Map a pandas dtype to an appropriate ClickHouse type.
    """
    if pd_dtype.name == 'object':
        return 'String'
    elif 'int' in pd_dtype.name:
        return 'Int64'
    elif 'float' in pd_dtype.name:
        return 'Float64'
    elif pd_dtype.name == 'bool':
        # ClickHouse does not have a native boolean, so use UInt8 (0 or 1)
        return 'UInt8'
    elif 'datetime' in pd_dtype.name:
        return 'DateTime'
    else:
        return 'String'

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
    
    # Read a small sample from the CSV file to infer column names and dtypes.
    sample_df = pd.read_csv(csv_file_path, nrows=100)
    columns = sample_df.columns.tolist()
    dtypes = sample_df.dtypes
    clickhouse_types = [map_dtype_to_clickhouse(dtype) for dtype in dtypes]
    
    # Drop the table if it already exists.
    client.command(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Table '{table_name}' dropped (if it existed).")
    
    # Build and execute the CREATE TABLE query with the inferred column types.
    col_defs = ", ".join([f"`{col}` {ctype}" for col, ctype in zip(columns, clickhouse_types)])
    create_table_query = f"CREATE TABLE {table_name} ({col_defs}) ENGINE = MergeTree() ORDER BY tuple()"
    client.command(create_table_query)
    print(f"Table '{table_name}' created with columns and types: {list(zip(columns, clickhouse_types))}")
    
    # Insert the CSV data in chunks using pandas.
    total_rows = 0
    for chunk in pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE):
        # Convert the DataFrame chunk into a list of lists.
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
    csv_dir = "/home/wsievolod/projects/datasets/basketball/csv"
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
