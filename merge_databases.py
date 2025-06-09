import os
from sqlalchemy import create_engine, text
import clickhouse_connect
from google.cloud import bigquery
from google.oauth2 import service_account
from migrate import ensure_postgres_database_exists, ensure_clickhouse_database_exists


def get_pg_engine(db_name):
    return create_engine(
        f"postgresql://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}@"
        f"{os.environ['PG_HOST']}:{os.environ.get('PG_PORT', '5432')}/{db_name}"
    )


def merge_dataset_postgres(db_name):
    engine = get_pg_engine(db_name)
    merge_sql = """
    DROP TABLE IF EXISTS basketball_merged;
    CREATE TABLE basketball_merged AS
    SELECT 
        p.player_id,
        pi.display_first_last AS player_name,
        t.team_name,
        td.season_year,
        g.game_id,
        g.game_date_est AS game_date,
        s.pts AS team_points,
        ps.comment AS play_commentary
    FROM player p
    LEFT JOIN common_player_info pi ON p.player_id = pi.player_id
    LEFT JOIN draft_history dh ON p.player_id = dh.player_id
    LEFT JOIN team t ON p.team_id = t.team_id
    LEFT JOIN team_details td ON t.team_id = td.team_id
    LEFT JOIN game g ON p.game_id = g.game_id
    LEFT JOIN game_summary s ON g.game_id = s.game_id
    LEFT JOIN play_by_play ps ON g.game_id = ps.game_id;
    """
    with engine.connect() as conn:
        conn.execute(text(merge_sql))
    print("✅ Merged dataset created in PostgreSQL as 'basketball_merged'")


def merge_dataset_clickhouse(db_name):
    client = clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        username=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ['CLICKHOUSE_PASSWORD']
    )

    merge_sql = f"""
    DROP TABLE IF EXISTS {db_name}.basketball_merged;
    CREATE TABLE {db_name}.basketball_merged
    ENGINE = MergeTree() ORDER BY player_id AS
    SELECT 
        p.player_id,
        pi.display_first_last AS player_name,
        t.team_name,
        td.season_year,
        g.game_id,
        g.game_date_est AS game_date,
        s.pts AS team_points,
        ps.comment AS play_commentary
    FROM {db_name}.player p
    LEFT JOIN {db_name}.common_player_info pi ON p.player_id = pi.player_id
    LEFT JOIN {db_name}.draft_history dh ON p.player_id = dh.player_id
    LEFT JOIN {db_name}.team t ON p.team_id = t.team_id
    LEFT JOIN {db_name}.team_details td ON t.team_id = td.team_id
    LEFT JOIN {db_name}.game g ON p.game_id = g.game_id
    LEFT JOIN {db_name}.game_summary s ON g.game_id = s.game_id
    LEFT JOIN {db_name}.play_by_play ps ON g.game_id = ps.game_id;
    """
    client.command(merge_sql)
    print(f"✅ Merged dataset created in ClickHouse as '{db_name}.basketball_merged'")


def merge_dataset_bigquery(db_name):
    project_id = os.environ['GCP_PROJECT_ID']
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bq_client = bigquery.Client(credentials=credentials, project=project_id)

    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{db_name}.basketball_merged` AS
    SELECT 
        p.player_id,
        pi.display_first_last AS player_name,
        t.team_name,
        td.season_year,
        g.game_id,
        g.game_date_est AS game_date,
        s.pts AS team_points,
        ps.comment AS play_commentary
    FROM `{project_id}.{db_name}.player` p
    LEFT JOIN `{project_id}.{db_name}.common_player_info` pi ON p.player_id = pi.player_id
    LEFT JOIN `{project_id}.{db_name}.draft_history` dh ON p.player_id = dh.player_id
    LEFT JOIN `{project_id}.{db_name}.team` t ON p.team_id = t.team_id
    LEFT JOIN `{project_id}.{db_name}.team_details` td ON t.team_id = td.team_id
    LEFT JOIN `{project_id}.{db_name}.game` g ON p.game_id = g.game_id
    LEFT JOIN `{project_id}.{db_name}.game_summary` s ON g.game_id = s.game_id
    LEFT JOIN `{project_id}.{db_name}.play_by_play` ps ON g.game_id = ps.game_id;
    """
    job = bq_client.query(query)
    job.result()
    print(f"✅ Merged dataset created in BigQuery as '{project_id}.{db_name}.basketball_merged'")


ensure_postgres_database_exists("basketball_data")
merge_dataset_postgres("basketball_data")
# OR
ensure_clickhouse_database_exists("basketball_data")
merge_dataset_clickhouse("basketball_data")
# OR
merge_dataset_bigquery("basketball_data")
