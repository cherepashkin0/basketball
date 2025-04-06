import os
import psycopg2
import clickhouse_connect

# PostgreSQL and ClickHouse settings
pg_host = os.environ['PG_HOST']
pg_user = os.environ['PG_USER']
pg_db = os.environ['DATABASE_PLAYER']
pg_password = os.environ['PG_PASSWORD']

clickhouse_host = os.environ['CLICKHOUSE_HOST']
clickhouse_user = os.environ.get('CLICKHOUSE_USER', 'default')
clickhouse_password = os.environ['CLICKHOUSE_PASSWORD']
clickhouse_db = os.environ['DATABASE_PLAYER']  # Use same db name in CH

# Connect to PostgreSQL
pg_conn = psycopg2.connect(
    host=pg_host,
    dbname=pg_db,
    user=pg_user,
    password=pg_password
)
pg_cursor = pg_conn.cursor()

# Connect to ClickHouse
ch_client = clickhouse_connect.get_client(
    host=clickhouse_host,
    username=clickhouse_user,
    password=clickhouse_password
)

# Get all table names
pg_cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
""")
tables = [row[0] for row in pg_cursor.fetchall()]

# Mapping from PostgreSQL to ClickHouse types (very basic)
type_mapping = {
    'integer': 'Int32',
    'bigint': 'Int64',
    'smallint': 'Int16',
    'serial': 'Int32',
    'bigserial': 'Int64',
    'boolean': 'UInt8',
    'text': 'String',
    'varchar': 'String',
    'character varying': 'String',
    'timestamp without time zone': 'DateTime',
    'timestamp with time zone': 'DateTime',
    'date': 'Date',
    'double precision': 'Float64',
    'real': 'Float32',
    'numeric': 'Float64',
    'json': 'String',
}

# Process each table
for table in tables:
    # Get columns and types
    pg_cursor.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    columns = pg_cursor.fetchall()

    if not columns:
        print(f"⚠️ Skipping empty or inaccessible table: {table}")
        continue

    # Generate CREATE TABLE SQL for ClickHouse
    ch_columns = []
    for name, pg_type in columns:
        ch_type = type_mapping.get(pg_type, 'String')  # fallback to String
        ch_columns.append(f"`{name}` {ch_type}")
    create_sql = f"CREATE TABLE IF NOT EXISTS {clickhouse_db}.{table} (\n    {', '.join(ch_columns)}\n) ENGINE = MergeTree() ORDER BY tuple()"

    try:
        ch_client.command(create_sql)
        print(f"✅ Created table: {clickhouse_db}.{table}")
    except Exception as e:
        print(f"❌ Failed to create table {table}: {e}")
        continue

    # Then insert data
    insert_sql = f"""
    INSERT INTO {clickhouse_db}.{table}
    SELECT * FROM postgresql('{pg_host}', '{pg_db}', '{table}', '{pg_user}', '{pg_password}')
    """
    try:
        ch_client.command(insert_sql)
        print(f"✅ Imported data from: {table}")
    except Exception as e:
        print(f"❌ Failed to import {table}: {e}")

pg_cursor.close()
pg_conn.close()
