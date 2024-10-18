/* --title 'Overall readiness (%)' --description 'Ready to be migrated' */
SELECT
    100 * COUNT_IF(size(failures) = 0) / COUNT(*) AS percentage
FROM inventory.historical  -- TODO: Rename inventory
WHERE object_type != 'migration_status'
