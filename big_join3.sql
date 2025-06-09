SELECT
    t.*,
    pbp.*,
    cpi.*,

    IF(
        length(splitByChar('-', assumeNotNull(cpi.height))) = 2,
        CAST(arrayElement(splitByChar('-', assumeNotNull(cpi.height)), 1) AS Int32) * 12 +
        CAST(arrayElement(splitByChar('-', assumeNotNull(cpi.height)), 2) AS Int32),
        NULL
    ) AS height_in,

    CAST(cpi.weight AS Nullable(Float32))     AS weight_cast,
    CAST(cpi.from_year AS Nullable(Int32))    AS from_year_cast,
    CAST(cpi.to_year AS Nullable(Int32))      AS to_year_cast,
    CAST(cpi.draft_year AS Nullable(Int32))   AS draft_year_cast,
    CAST(cpi.draft_round AS Nullable(Int32))  AS draft_round_cast,
    CAST(cpi.draft_number AS Nullable(Int32)) AS draft_number_cast

FROM (
    SELECT
        game_id,
        team_id_home AS team_id,
        pts_home     AS final_score,
        *
    FROM line_score

    UNION ALL

    SELECT
        game_id,
        team_id_away AS team_id,
        pts_away     AS final_score,
        *
    FROM line_score
) AS t

JOIN (
    SELECT DISTINCT
        game_id,
        player1_team_id AS team_id,
        player1_id      AS player_id,
        *
    FROM play_by_play
    WHERE player1_id != 0
) AS pbp
ON pbp.game_id = t.game_id AND pbp.team_id = t.team_id

LEFT JOIN common_player_info AS cpi ON cpi.person_id = pbp.player_id
LIMIT 100;
-- ORDER BY t.game_id, t.team_id, pbp.player_id

