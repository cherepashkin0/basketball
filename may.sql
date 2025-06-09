-- Active: 1742470319960@@34.70.242.157@8123@basketball_united
SELECT
    table,
    name
FROM system.columns
WHERE database = 'basketball_united'
ORDER BY table, position; 