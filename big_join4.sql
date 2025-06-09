-- Active: 1742470319960@@34.70.242.157@8123@basketball_united
SELECT
    pbp.*,
    g.*
FROM
    play_by_play pbp
JOIN
    game g ON pbp.game_id = g.game_id;
