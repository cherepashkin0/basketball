import os
import pandas as pd
import numpy as np
import clickhouse_connect
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery_storage_v1 import BigQueryReadClient
import traceback
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# Configuration for ClickHouse consolidation
DATA_ORIGINS = {
    "basketball_game": "clickhouse",
    "basketball_player": "postgresql",
    "basketball_team": "bigquery"
}

dict_special = {'fg_pct_home': float,
                'fg3_pct_home': float, 
                'ft_pct_home': float,
                'fg_pct_away': float, 
                'fg3_pct_away': float,
                'ft_pct_away': float}
TARGET_DB = "basketball_data"
CHUNK_SIZE = 50000

# --- ClickHouse Utilities ---
def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )

def ensure_clickhouse_database_exists(db_name):
    ch_client = get_ch_client()
    try:
        existing_dbs = ch_client.query("SHOW DATABASES").result_rows
        db_names = {row[0] for row in existing_dbs}
        if db_name not in db_names:
            print(f"🛠️ Creating ClickHouse database: {db_name}")
            ch_client.command(f"CREATE DATABASE {db_name}")
        else:
            print(f"✅ ClickHouse database '{db_name}' already exists.")
    except Exception as e:
        traceback.print_exc()               # Shows a full Python traceback
        print(f"❌ Failed to check or create ClickHouse database '{db_name}': {e}")

def copy_to_clickhouse(df, table_name, ch_client, db_name):
    print(42, df.dtypes)
    print(43, df.head(10))
    # Inside copy_to_clickhouse, replace the type inference logic:
    columns = []
    # Attempt to convert potential date columns first
    for col in df.columns:
        if 'date' in col.lower() or 'time' in col.lower(): # Simple name check
            try:
                # Try converting to datetime to check feasibility
                pd.to_datetime(df[col].dropna(), errors='raise')
                # If conversion works, assume it's a datetime column
                # Ensure the data is actually converted for insertion later if needed
                columns.append(f"`{col}` Nullable(DateTime64)") # Or DateTime
                continue # Go to next column
            except (ValueError, TypeError, OverflowError):
                # If conversion fails, fall back to other checks
                pass

        # Existing checks
        if pd.api.types.is_float_dtype(df[col]):
            columns.append(f"`{col}` Nullable(Float64)")
        elif pd.api.types.is_integer_dtype(df[col]):
            # Check for potential boolean integers if necessary
            # if set(df[col].dropna().unique()) <= {0, 1}:
            #     columns.append(f"`{col}` Nullable(UInt8)") # Or Bool if supported/desired
            # else:
            columns.append(f"`{col}` Nullable(Int64)")
        elif pd.api.types.is_datetime64_any_dtype(df[col]): # Check for numpy datetime
            columns.append(f"`{col}` Nullable(DateTime64)")
        elif pd.api.types.is_string_dtype(df[col]) or df[col].dtype == object:
            # Keep as Nullable(String) for objects and explicit strings
            columns.append(f"`{col}` Nullable(String)")
        else:
            # Fallback for other types (e.g., boolean, complex)
            print(f"⚠️ Unhandled dtype {df[col].dtype} for column {col}. Defaulting to Nullable(String).")
            columns.append(f"`{col}` Nullable(String)")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        {', '.join(columns)}
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    try:
        ch_client.command(create_table_sql)
        print(f"✅ Created ClickHouse table '{db_name}.{table_name}'")
    except Exception as e:
        print(f"❌ Error creating table {table_name} in ClickHouse: {e}")
        return

    # Insert data in chunks
    for chunk in np.array_split(df, int(len(df) / CHUNK_SIZE) + 1):
        if chunk.empty:
            continue
        try:
            chunk = clean_clickhouse_data(chunk)

            columns = list(chunk.columns)
            # Convert the DataFrame to a 2D list of rows
            data_to_insert = chunk.replace({np.nan: None}).values.tolist()

            ch_client.insert(
                f'{db_name}.{table_name}',
                data_to_insert,
                column_names=columns
            )
            # data_to_insert = chunk.replace({np.nan: None}).to_dict(orient='records')
            # ch_client.insert(f"{db_name}.{table_name}", data_to_insert)
            print(f"✅ Inserted chunk of {len(chunk)} rows into {table_name}")
        except Exception as e:
            traceback.print_exc()               # Shows a full Python traceback
            print(f"❌ INSERT failed for table {table_name}: {e}")
            # Debug info...
            exit()
            break


def clean_clickhouse_data(df):
    # Replace string 'None' and 'nan' with actual None (NaN still handled separately)
    df = df.replace({'None': None, 'nan': None, '': None})

    # Convert NaN and NaT to None explicitly
    df = df.where(pd.notnull(df), None)

    # Clean String columns.
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: str(x).encode('utf-8', 'ignore').decode('utf-8') if pd.notnull(x) else x)

    return df

# --- BigQuery to ClickHouse ---
def migrate_bigquery_to_clickhouse(db_name, source_db):
    bq_project = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(credentials=credentials, project=bq_project)
    bq_storage_client = BigQueryReadClient(credentials=credentials)

    ch_client = get_ch_client()
    dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=source_db)
    tables = list(bq_client.list_tables(dataset_ref))

    for table in tables:
        table_name = table.table_id
        full_table_id = f"{bq_project}.{source_db}.{table_name}"

        try:
            print(f"📥 Downloading {table_name} from BigQuery...")
            df = bq_client.list_rows(full_table_id).to_dataframe(bqstorage_client=bq_storage_client)
            if df.empty:
                print(f"⚠️ Skipping empty table: {table_name}")
                continue

            copy_to_clickhouse(df, table_name, ch_client, db_name)
            print(f"✅ Migrated {table_name} to ClickHouse")
        except Exception as e:
            print(f"❌ Failed to migrate {table_name}: {e}")

# --- ClickHouse to ClickHouse ---
def migrate_clickhouse_to_clickhouse(db_name, source_db):
    ch_client = get_ch_client()
    tables = ch_client.query(f"SHOW TABLES FROM {source_db}").result_rows

    for (table,) in tables:
        try:
            print(f"📥 Reading from ClickHouse: {source_db}.{table}")
            result = ch_client.query(f"SELECT * FROM {source_db}.{table}")
            df = pd.DataFrame(result.result_rows, columns=result.column_names)

            for col in df.columns[df.dtypes == 'object']:
                unique_pytypes = {type(v) for v in df[col] if pd.notnull(v)}
                if len(unique_pytypes) > 1 or (unique_pytypes and list(unique_pytypes)[0] != str):
                    print(108, f"Column '{col}' has mixed types: {unique_pytypes}")
            # print(f"📤 Inserting {len(chunk)} rows into {db_name}.{table_name}...")
            for col_name in dict_special.keys():
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            # df['fg3_pct_home'] = pd.to_numeric(df['fg3_pct_home'], errors='coerce')
            # df['fg3_pct_away'] = pd.to_numeric(df['fg3_pct_away'], errors='coerce')
            # df['fg3_pct_away'] = pd.to_numeric(df['fg3_pct_away'], errors='coerce')
            df['wl_home'] = df['wl_home'].astype(str)

            if df.empty:
                print(f"⚠️ Skipping empty table: {table}")
                continue

            copy_to_clickhouse(df, table, ch_client, db_name)
            print(f"✅ Migrated {table} to ClickHouse")
        except Exception as e:
            print(f"❌ Failed to migrate {table}: {e}")

# --- Main Consolidation Logic ---
def consolidate_to_clickhouse():
    ensure_clickhouse_database_exists(TARGET_DB)

    for db_name, source in DATA_ORIGINS.items():
        print(f"\n🔄 Migrating {db_name} from {source} → ClickHouse ({TARGET_DB})")

        if source == "clickhouse":
            migrate_clickhouse_to_clickhouse(db_name=TARGET_DB, source_db=db_name)
        elif source == "bigquery":
            migrate_bigquery_to_clickhouse(db_name=TARGET_DB, source_db=db_name)
        elif source == "postgresql":
            print(f"⚠️ Skipping migration: assume {db_name} is already in ClickHouse.")
        else:
            raise ValueError(f"Unsupported source DBMS: {source}")

    print(f"\n✅ All 15 tables should now be in the unified ClickHouse DB: {TARGET_DB}")

if __name__ == "__main__":
    consolidate_to_clickhouse()

