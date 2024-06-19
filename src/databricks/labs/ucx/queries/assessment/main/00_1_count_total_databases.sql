-- --title 'Total Database Count'
SELECT COUNT(DISTINCT `database`) AS count_total_databases
FROM inventory.tables
