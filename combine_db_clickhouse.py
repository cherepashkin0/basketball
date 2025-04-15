from csv_upload import get_clickhouse_client

def merge_clickhouse_databases(source_dbs, target_db):
    client = get_clickhouse_client("DATABASE_UNITED")

    # Create the target database
    client.query(f"CREATE DATABASE IF NOT EXISTS {target_db}")

    # Get all tables from the source databases
    placeholders = ",".join([f"'{db}'" for db in source_dbs])
    tables = client.query(f"""
        SELECT database, name
        FROM system.tables
        WHERE database IN ({placeholders})
    """)

    for db_name, table_name in tables.result_set:
        full_source = f"{db_name}.{table_name}"
        full_target = f"{target_db}.{table_name}"
        print(f"📦 Merging {full_source} → {full_target}")

        # Create table in merged DB
        client.query(f"CREATE TABLE IF NOT EXISTS {full_target} AS {full_source}")

        # Insert data
        client.query(f"INSERT INTO {full_target} SELECT * FROM {full_source}")

    print(f"✅ All tables from {source_dbs} merged into {target_db}")


merge_clickhouse_databases(
    source_dbs=["basketball_game", "basketball_team", "basketball_player"],
    target_db="basketball_united"
)
