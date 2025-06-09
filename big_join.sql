-- Active: 1742470319960@@34.70.242.157@8123@basketball_united
/* ---------------------------------------------
   Players + team‑level final score, one row per
   (game_id, team_id, player_id)
   --------------------------------------------- */
WITH team_points AS (               -- 1️⃣ final score per team
    SELECT
        game_id,
        team_id_home AS team_id,
        pts_home     AS final_score
    FROM line_score
    UNION ALL                       -- add the visiting team
    SELECT
        game_id,
        team_id_away,
        pts_away
    FROM line_score
),
roster AS (                         -- 2️⃣ who actually appeared
    /* player1 columns */
    SELECT DISTINCT
        game_id,
        player1_team_id AS team_id,
        player1_id      AS player_id
    FROM play_by_play
    WHERE player1_id IS NOT NULL

    UNION DISTINCT                  -- player2 columns
    SELECT DISTINCT
        game_id,
        player2_team_id,
        player2_id
    FROM play_by_play
    WHERE player2_id IS NOT NULL

    UNION DISTINCT                  -- player3 columns
    SELECT DISTINCT
        game_id,
        player3_team_id,
        player3_id
    FROM play_by_play
    WHERE player3_id IS NOT NULL
)
SELECT
    tp.game_id,
    tp.team_id,
    tp.final_score,                 -- dependent variable
    r.player_id,                    -- explanatory factor
    cpi.first_name,
    cpi.last_name,
    cpi.position,
    cpi.height,
    cpi.weight
FROM team_points          tp
JOIN roster               r   ON  tp.game_id = r.game_id
                              AND tp.team_id = r.team_id
JOIN common_player_info    cpi ON  cpi.person_id = r.player_id
ORDER BY tp.game_id, tp.team_id, cpi.last_name, cpi.first_name;