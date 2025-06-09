from csv_upload import get_postgres_engine
from sqlalchemy import create_engine, inspect, text
import os

def create_postgres_database_if_not_exists(db_name):
    admin_db_env = "PG_TEMP_DATABASE"
    os.environ[admin_db_env] = "postgres"  # default admin DB
    engine = get_postgres_engine(admin_db_env)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = :name"), {"name": db_name})
        if not result.scalar():
            print(f"🛠 Creating target database: {db_name}")
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
        else:
            print(f"✅ Target database '{db_name}' already exists.")

def merge_postgres_databases(source_dbs, target_db):
    create_postgres_database_if_not_exists(target_db)

    os.environ["PG_TEMP_DATABASE"] = target_db
    target_engine = get_postgres_engine("PG_TEMP_DATABASE")
    target_conn = target_engine.connect()

    for db_name in source_dbs:
        print(f"🔄 Connecting to source DB: {db_name}")
        os.environ["PG_TEMP_DATABASE"] = db_name
        source_engine = get_postgres_engine("PG_TEMP_DATABASE")
        source_conn = source_engine.connect()
        inspector = inspect(source_engine)

        for table_name in inspector.get_table_names():
            print(f"📦 Copying {db_name}.{table_name} → {target_db}.{table_name}")

            # Create table in target if it doesn't exist
            ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" AS TABLE "{table_name}" WITH NO DATA;'
            try:
                target_conn.execute(text(ddl))
            except Exception as e:
                print(f"⚠️ Skipping DDL for {table_name}: {e}")

            # Copy data from source
            try:
                data = source_conn.execute(text(f'SELECT * FROM "{table_name}"')).fetchall()
                if data:
                    insert_query = f'INSERT INTO "{table_name}" SELECT * FROM "{table_name}"'
                    for row in data:
                        target_conn.execute(
                            text(insert_query.replace("SELECT *", f"SELECT {', '.join(f'%s' for _ in row)}")),
                            row
                        )
            except Exception as e:
                print(f"❌ Failed to copy {table_name} from {db_name}: {e}")

        source_conn.close()

    target_conn.close()
    print(f"✅ Merged all tables from {source_dbs} into {target_db}")


# Example usage:
merge_postgres_databases(
    source_dbs=["basketball_game", "basketball_team", "basketball_player"],
    target_db="basketball_united"
)
