#!/bin/bash
# python csv_upload.py --target bigquery --table_type 'team_specific' --database_env_var DATABASE_TEAM
# python csv_upload.py --target bigquery --table_type 'game_specific' --database_env_var DATABASE_GAME
# python csv_upload.py --target bigquery --table_type 'player_specific' --database_env_var DATABASE_PLAYER

python csv_upload.py --target clickhouse --table_type 'team_specific' --database_env_var DATABASE_TEAM
python csv_upload.py --target clickhouse --table_type 'game_specific' --database_env_var DATABASE_GAME
python csv_upload.py --target clickhouse --table_type 'player_specific' --database_env_var DATABASE_PLAYER

python csv_upload.py --target postgres --table_type 'team_specific' --database_env_var DATABASE_TEAM
python csv_upload.py --target postgres --table_type 'game_specific' --database_env_var DATABASE_GAME
python csv_upload.py --target postgres --table_type 'player_specific' --database_env_var DATABASE_PLAYER