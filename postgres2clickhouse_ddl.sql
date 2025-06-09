CREATE TABLE click_pg_player
(
    id   UUID,
    name String
)
ENGINE = PostgreSQL(
  '${PG_HOST}:${PG_PORT}',
  '${PG_DATABASE}',
  'public',
  'pg_player',
  '${PG_USER}',
  '${PG_PASSWORD}'
);