/* --title 'Successfully migrated resources (%)' */
SELECT
    100 * COUNT_IF(size(failures) = 0) / COUNT(*) AS percentage
FROM inventory.historical
