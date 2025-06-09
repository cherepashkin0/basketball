import os
from migrate import (
    ensure_postgres_database_exists,
    migrate_bigquery_to_postgresql,
    migrate_clickhouse_to_postgresql,
)

# Define mapping: where each data part lives
DATA_ORIGINS = {
    "basketball_game": "clickhouse",
    "basketball_player": "postgresql",
    "basketball_team": "bigquery"
}

TARGET_DB = "basketball_data"

def consolidate_to_postgres():
    ensure_postgres_database_exists(TARGET_DB)

    for db_name, source in DATA_ORIGINS.items():
        print(f"\n🔄 Migrating {db_name} from {source} → postgresql ({TARGET_DB})")

        if source == "clickhouse":
            migrate_clickhouse_to_postgresql(db_name=TARGET_DB, source_db=db_name)
        elif source == "bigquery":
            migrate_bigquery_to_postgresql(db_name=TARGET_DB, source_db=db_name)
        elif source == "postgresql":
            # You might already have data locally, so implement intra-Postgres copy if needed
            print(f"⚠️ Skipping migration: assume {db_name} is already in PostgreSQL.")
        else:
            raise ValueError(f"Unsupported source DBMS: {source}")

    print(f"\n✅ All 15 tables should now be in the unified PostgreSQL DB: {TARGET_DB}")

consolidate_to_postgres()
