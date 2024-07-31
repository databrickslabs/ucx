/* --title 'Group Migration Failures' --height 4 --width 4 */
/*
 * Messages that we're looking for are of the form:
 *   failed-group-migration: {name_in_workspace} -> {name_in_account}: {reason}
 */
SELECT
    cast(`timestamp` as timestamp) as `timestamp`,
    job_run_id,
    level,
    message
FROM inventory.logs
WHERE
  workflow_name in ('migrate-groups', 'migrate-groups-experimental')
  and startswith(message, 'failed-group-migration:')
ORDER BY 1
