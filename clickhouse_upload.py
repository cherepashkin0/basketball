import os
import glob
import json
import pandas as pd
from clickhouse_connect import get_client

CHUNK_SIZE = 50000

# Columns always treated as string
dict_always_str = {
    'game_time': str,
    'live_pc_time': str,
    'natl_tv_broadcaster_abbreviation': str,
    'wctimestring': str,
    'wl_home': str,
    'wl_away': str,
    'scoremargin': str
}

def merge_inferred_types(type1, type2):
    if type1 == type2:
        return type1
    if "string" in (type1, type2):
        return "string"
    numeric_types = {"Int64", "Float64", "boolean"}
    if type1 in numeric_types and type2 in numeric_types:
        if "Float64" in (type1, type2):
            return "Float64"
        return "Int64"
    if "datetime64[ns]" in (type1, type2):
        return "string"
    return "string"

def get_final_types(csv_file_path):
    final_types = {}
    for chunk in pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE, dtype=dict_always_str, low_memory=False):
        chunk = chunk.convert_dtypes()
        for col in chunk.columns:
            inferred = "string" if col in dict_always_str else str(chunk[col].dtype)
            is_nullable = chunk[col].isnull().any()
            if col not in final_types:
                final_types[col] = (inferred, is_nullable)
            else:
                prev_type, prev_nullable = final_types[col]
                merged_type = merge_inferred_types(prev_type, inferred)
                merged_nullable = prev_nullable or is_nullable
                final_types[col] = (merged_type, merged_nullable)
    return final_types

type_mapping = {
    "string": "String",
    "Int64": "Int64",
    "Float64": "Float64",
    "boolean": "UInt8",
    "datetime64[ns]": "DateTime"
}

def process_csv_file(csv_file_path, client):
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
    create_table_query = f"CREATE TABLE {table_name} ({col_defs}) ENGINE = MergeTree() ORDER BY tuple()"
    client.command(create_table_query)

    for chunk in pd.read_csv(csv_file_path, chunksize=CHUNK_SIZE, dtype=dict_always_str):
        chunk = chunk.where(pd.notnull(chunk), None)
        data = chunk.values.tolist()
        client.insert(table=table_name, data=data, column_names=columns)

def main():
    host = os.getenv("CLICKHOUSE_HOST")
    port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    user = os.getenv("CLICKHOUSE_USER")
    password = os.getenv("CLICKHOUSE_PASSWORD")
    database = os.getenv("DATABASE_GAME")

    if None in [host, user, password, database]:
        raise ValueError("Missing required ClickHouse environment variables.")

    client = get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database
    )

    csv_dir = "/home/wsievolod/projects/datasets/basketball/csv/"

    with open('tables_dict.json', 'r') as f:
        tables_dict = json.load(f)
    tables_to_upload = set(tables_dict['game_specific'])

    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    csv_files = [f for f in csv_files if os.path.splitext(os.path.basename(f))[0] in tables_to_upload]

    if not csv_files:
        print("No player-specific CSV files found.")
        return

    for csv_file in csv_files:
        print(f"Processing player-specific file: {csv_file}")
        process_csv_file(csv_file, client)

if __name__ == "__main__":
    main()
