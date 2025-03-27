import pandas as pd
import os
import glob
import json
from tqdm import tqdm
from sqlalchemy import create_engine
import argparse


def load_tables_dict(filepath: str) -> dict:
    with open(filepath, 'r') as file:
        return json.load(file)


def get_database_engine(database_env_var: str) -> create_engine:
    PG_HOST = os.environ.get("PG_HOST")
    PG_PORT = os.environ.get("PG_PORT", 5432)
    PG_USER = os.environ.get("PG_USER")
    PG_PASSWORD = os.environ.get("PG_PASSWORD")
    PG_DATABASE = os.environ.get(database_env_var)

    required_vars = {"PG_HOST": PG_HOST, "PG_USER": PG_USER, "PG_PASSWORD": PG_PASSWORD, database_env_var: PG_DATABASE}
    missing_vars = [k for k, v in required_vars.items() if v is None]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

    engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}')
    return engine


def find_csv_files(directory: str, table_names: set) -> list:
    all_csv_files = glob.glob(os.path.join(directory, "*.csv"))
    # splitext - splits content on root and ext, system independent
    return [f for f in all_csv_files if os.path.splitext(os.path.basename(f))[0] in table_names] 


def upload_csv_file(engine, file_path: str, chunk_size: int):
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f'Processing table: {table_name}')

    chunk_iterator = pd.read_csv(file_path, chunksize=chunk_size) # Returns an iterator that yields DataFrames, each with up to chunk_size rows.
    for chunk in tqdm(chunk_iterator, desc=f"Uploading {table_name}", unit="chunk"):
        # append is necessary for: creating new tables, if exist, upload new chunks to existing tables
        chunk.to_sql(table_name, con=engine, if_exists='append', index=False)


def main(table_type: str, database_env_var: str):
    tables_dict = load_tables_dict('tables_dict.json') # file with names of game-specific and player-specific tables
    selected_table_names = set(tables_dict[table_type]) # set is for deduplication

    engine = get_database_engine(database_env_var)

    csv_directory = '/home/wsievolod/projects/datasets/basketball/csv/'
    csv_files = find_csv_files(csv_directory, selected_table_names)

    if not csv_files:
        print(f"No {table_type} CSV files found.")
        return

    print(f"Found {len(csv_files)} {table_type} CSV files. Uploading...")

    CHUNK_SIZE = 500000 # number of rows to load in one chunk. 

    for file_path in tqdm(csv_files, desc=f"Uploading {table_type} files", unit="file"):
        upload_csv_file(engine, file_path, CHUNK_SIZE)

    print("CSV files have been uploaded successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload CSV files to PostgreSQL")
    parser.add_argument("--table_type", required=True, choices=['game_specific', 'player_specific'], help="Type of tables to upload")
    parser.add_argument("--database_env_var", required=True, help="Environment variable name for the target database")
    args = parser.parse_args()

    main(args.table_type, args.database_env_var)
