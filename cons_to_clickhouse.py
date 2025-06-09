import os
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from sqlalchemy import create_engine, inspect
import clickhouse_connect
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery_storage_v1 import BigQueryReadClient
import io

# Configuration
DATA_ORIGINS = {
    "basketball_game": "clickhouse",
    "basketball_player": "postgresql",
    "basketball_team": "bigquery"
}
TARGET_DB = "basketball_data"
CHUNK_SIZE = 50000

# Type mapping for ClickHouse from other DBMSs
TYPE_MAPPING = {
    # PostgreSQL to ClickHouse
    "integer": "Int32",
    "bigint": "Int64",
    "smallint": "Int16",
    "text": "String",
    "varchar": "String",
    "double precision": "Float64",
    "real": "Float32",
    "boolean": "UInt8",
    "timestamp": "DateTime",
    
    # BigQuery to ClickHouse
    "INT64": "Int64",
    "FLOAT64": "Float64",
    "BOOLEAN": "UInt8",
    "STRING": "String",
    "DATETIME": "DateTime",
    "DATE": "Date",
    "TIMESTAMP": "DateTime"
}

def get_clickhouse_client():
    """Get ClickHouse client connection"""
    return clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD'],
        database=TARGET_DB
    )

def ensure_clickhouse_database_exists(client, db_name):
    """Ensure target ClickHouse database exists"""
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"✅ ClickHouse database '{db_name}' ready")
    except Exception as e:
        print(f"❌ Error ensuring ClickHouse database exists: {e}")
        raise

def get_pg_engine(db_name):
    """Create SQLAlchemy engine for PostgreSQL"""
    return create_engine(
        f"postgresql://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}@"
        f"{os.environ['PG_HOST']}:{os.environ.get('PG_PORT', '5432')}/{db_name}"
    )

def migrate_postgres_to_clickhouse(client, source_db):
    """Migrate PostgreSQL data to ClickHouse"""
    print(f"\n🔄 Migrating from PostgreSQL ({source_db}) to ClickHouse ({TARGET_DB})")
    
    pg_engine = get_pg_engine(source_db)
    inspector = inspect(pg_engine)
    tables = inspector.get_table_names(schema="public")
    
    for table in tables:
        try:
            print(f"📥 Reading table {table} from PostgreSQL...")
            # Get table structure
            columns = inspector.get_columns(table)
            
            # Create ClickHouse table with proper schema
            create_clickhouse_table_from_postgres(client, table, columns)
            
            # Read and insert data in chunks
            with pg_engine.connect() as conn:
                count_query = f"SELECT COUNT(*) FROM {table}"
                total_rows = conn.execute(count_query).scalar()
                
                print(f"📊 Total rows to migrate for {table}: {total_rows}")
                
                for offset in tqdm(range(0, total_rows, CHUNK_SIZE), desc=f"Migrating {table}"):
                    query = f"SELECT * FROM {table} LIMIT {CHUNK_SIZE} OFFSET {offset}"
                    df = pd.read_sql(query, conn)
                    
                    if not df.empty:
                        # Handle nulls properly for ClickHouse
                        df = df.where(pd.notnull(df), None)
                        
                        # Insert data to ClickHouse
                        client.insert(table, df.values.tolist(), column_names=df.columns.tolist())
            
            print(f"✅ Successfully migrated {table} with {total_rows} rows")
        except Exception as e:
            print(f"❌ Failed to migrate {table}: {e}")

def create_clickhouse_table_from_postgres(client, table_name, columns):
    """Create ClickHouse table based on PostgreSQL schema"""
    # Drop table if exists
    client.command(f"DROP TABLE IF EXISTS {TARGET_DB}.{table_name}")
    
    # Build column definitions
    column_defs = []
    for column in columns:
        col_name = column['name']
        pg_type = column['type'].__str__().lower()
        
        # Map PostgreSQL type to ClickHouse type
        ch_type = None
        for pg_pattern, ch_equivalent in TYPE_MAPPING.items():
            if pg_pattern.lower() in pg_type:
                ch_type = ch_equivalent
                break
        
        if not ch_type:
            ch_type = "String"  # Default fallback
        
        # Handle nullable
        if column['nullable']:
            ch_type = f"Nullable({ch_type})"
            
        column_defs.append(f"`{col_name}` {ch_type}")
    
    # Create table
    create_query = f"""
    CREATE TABLE {TARGET_DB}.{table_name} (
        {', '.join(column_defs)}
    ) ENGINE = MergeTree() ORDER BY tuple()
    """
    
    try:
        client.command(create_query)
        print(f"✅ Created ClickHouse table: {table_name}")
    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")
        print(f"Query was: {create_query}")
        raise

def migrate_clickhouse_to_clickhouse(client, source_db):
    """Migrate ClickHouse data to target ClickHouse database"""
    print(f"\n🔄 Migrating from ClickHouse ({source_db}) to ClickHouse ({TARGET_DB})")
    
    # Get source ClickHouse client
    source_client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD'],
        database=source_db
    )
    
    # Get tables from source database
    tables = source_client.query("SHOW TABLES").result_rows
    
    for (table,) in tables:
        try:
            print(f"📥 Reading table {table} from ClickHouse source...")
            
            # Get table structure
            columns_info = source_client.query(f"DESCRIBE TABLE {table}").named_results()
            
            # Create table in target database
            create_clickhouse_table_from_clickhouse(client, table, columns_info)
            
            # Insert data in chunks
            total_rows = source_client.query(f"SELECT count() FROM {table}").result_rows[0][0]
            print(f"📊 Total rows to migrate for {table}: {total_rows}")
            
            for offset in tqdm(range(0, total_rows, CHUNK_SIZE), desc=f"Migrating {table}"):
                query = f"SELECT * FROM {table} LIMIT {CHUNK_SIZE} OFFSET {offset}"
                result = source_client.query(query)
                
                if result.result_rows:
                    client.insert(
                        table, 
                        result.result_rows,
                        column_names=result.column_names
                    )
            
            print(f"✅ Successfully migrated {table} with {total_rows} rows")
        except Exception as e:
            print(f"❌ Failed to migrate {table}: {e}")

def create_clickhouse_table_from_clickhouse(client, table_name, columns_info):
    """Create ClickHouse table based on source ClickHouse schema"""
    # Drop table if exists
    client.command(f"DROP TABLE IF EXISTS {TARGET_DB}.{table_name}")
    
    # Build column definitions
    column_defs = []
    for col in columns_info:
        column_defs.append(f"`{col['name']}` {col['type']}")
    
    # Create table
    create_query = f"""
    CREATE TABLE {TARGET_DB}.{table_name} (
        {', '.join(column_defs)}
    ) ENGINE = MergeTree() ORDER BY tuple()
    """
    
    try:
        client.command(create_query)
        print(f"✅ Created ClickHouse table: {table_name}")
    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")
        raise

def migrate_bigquery_to_clickhouse(client, source_db):
    """Migrate BigQuery data to ClickHouse"""
    print(f"\n🔄 Migrating from BigQuery ({source_db}) to ClickHouse ({TARGET_DB})")
    
    # Set up BigQuery clients
    bq_project = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(credentials=credentials, project=bq_project)
    bq_storage_client = BigQueryReadClient(credentials=credentials)
    
    # Get tables from BigQuery dataset
    dataset_ref = bigquery.DatasetReference(project=bq_project, dataset_id=source_db)
    tables = list(bq_client.list_tables(dataset_ref))
    
    for table in tables:
        table_name = table.table_id
        full_table_id = f"{bq_project}.{source_db}.{table_name}"
        
        try:
            print(f"📥 Reading table {table_name} from BigQuery...")
            
            # Get table schema
            table_ref = bq_client.get_table(full_table_id)
            
            # Create table in ClickHouse
            create_clickhouse_table_from_bigquery(client, table_name, table_ref.schema)
            
            # Download data in chunks
            query = f"SELECT * FROM `{full_table_id}`"
            df = bq_client.query(query).to_dataframe(bqstorage_client=bq_storage_client)
            
            if df.empty:
                print(f"⚠️ Skipping empty table: {table_name}")
                continue
            
            # Insert data in chunks
            total_rows = len(df)
            print(f"📊 Total rows to migrate for {table_name}: {total_rows}")
            
            chunks = np.array_split(df, int(total_rows / CHUNK_SIZE) + 1)
            for chunk in tqdm(chunks, desc=f"Migrating {table_name}"):
                if not chunk.empty:
                    # Handle nulls properly for ClickHouse
                    chunk = chunk.where(pd.notnull(chunk), None)
                    
                    # Insert data to ClickHouse
                    client.insert(table_name, chunk.values.tolist(), column_names=chunk.columns.tolist())
            
            print(f"✅ Successfully migrated {table_name} with {total_rows} rows")
        except Exception as e:
            print(f"❌ Failed to migrate {table_name}: {e}")

def create_clickhouse_table_from_bigquery(client, table_name, schema):
    """Create ClickHouse table based on BigQuery schema"""
    # Drop table if exists
    client.command(f"DROP TABLE IF EXISTS {TARGET_DB}.{table_name}")
    
    # Build column definitions
    column_defs = []
    for field in schema:
        col_name = field.name
        bq_type = field.field_type
        
        # Map BigQuery type to ClickHouse type
        ch_type = TYPE_MAPPING.get(bq_type, "String")
        
        # Handle nullable fields (BigQuery fields are nullable by default)
        if not field.mode == 'REQUIRED':
            ch_type = f"Nullable({ch_type})"
            
        column_defs.append(f"`{col_name}` {ch_type}")
    
    # Create table
    create_query = f"""
    CREATE TABLE {TARGET_DB}.{table_name} (
        {', '.join(column_defs)}
    ) ENGINE = MergeTree() ORDER BY tuple()
    """
    
    try:
        client.command(create_query)
        print(f"✅ Created ClickHouse table: {table_name}")
    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")
        raise

def consolidate_to_clickhouse():
    """Main function to consolidate all data to ClickHouse"""
    # Get ClickHouse client for target database
    base_client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )
    
    # Ensure target database exists
    ensure_clickhouse_database_exists(base_client, TARGET_DB)
    
    # Get client for target database
    client = get_clickhouse_client()
    
    # Migrate from each source
    for db_name, source in DATA_ORIGINS.items():
        if source == "clickhouse":
            migrate_clickhouse_to_clickhouse(client, db_name)
        elif source == "postgresql":
            migrate_postgres_to_clickhouse(client, db_name)
        elif source == "bigquery":
            migrate_bigquery_to_clickhouse(client, db_name)
        else:
            print(f"⚠️ Unsupported source: {source}")
    
    print(f"\n✅ All basketball data has been consolidated into the ClickHouse database: {TARGET_DB}")

if __name__ == "__main__":
    consolidate_to_clickhouse()