#!/bin/bash
#python migrate.py --source postgresql --target bigquery --database basketball_player
#python migrate.py --source postgresql --target clickhouse --database basketball_player

# python migrate.py --source bigquery --target postgresql --database basketball_team
# python migrate.py --source bigquery --target clickhouse --database basketball_team

# python migrate.py --source clickhouse --target bigquery --database basketball_game
python migrate.py --source clickhouse --target postgresql --database basketball_game