-- Active: 1742470319960@@34.70.242.157@8123@basketball_player
SELECT * FROM basketball.play_by_play LIMIT 1000;

SELECT * FROM play_by_play LIMIT 1000;

SELECT * FROM basketball.play_by_play LIMIT 1000;

SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public';

SELECT 
  table_name, 
  column_name, 
  data_type, 
  is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;


SELECT p.*, t.team_name, g.date
FROM basketball_player p
JOIN basketball_team t ON p.team_id = t.team_id
JOIN basketball_game g ON p.game_id = g.game_id;

CREATE TABLE basketball_merged AS
SELECT 
    p.player_id,
    pi.display_first_last AS player_name,
    t.team_name,
    td.season_year,
    g.game_id,
    g.game_date_est AS game_date,
    s.pts AS team_points,
    pbp.comment AS play_commentary
FROM player p
LEFT JOIN common_player_info pi ON p.player_id = pi.player_id
LEFT JOIN draft_history dh ON p.player_id = dh.player_id
LEFT JOIN team t ON p.team_id = t.team_id
LEFT JOIN team_details td ON t.team_id = td.team_id
LEFT JOIN game g ON p.game_id = g.game_id
LEFT JOIN game_summary s ON g.game_id = s.game_id
LEFT JOIN play_by_play pbp ON g.game_id = pbp.game_id;

CREATE DATABASE basketball_data;
SELECT database, name
FROM system.tables
WHERE database IN ('basketball_player', 'basketball_game', 'basketball_team');