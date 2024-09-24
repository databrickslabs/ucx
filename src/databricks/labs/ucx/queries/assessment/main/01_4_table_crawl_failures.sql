/*
--title 'Table Crawl Failures'
--height 4
--width 4
*/
WITH table_crawl_failures AS (
SELECT
    timestamp,
    REGEXP_EXTRACT(message, '^failed-table-crawl: (.+?) -> (.+?): (.+)$', 1) AS error_reason,
    REGEXP_EXTRACT(message, '^failed-table-crawl: (.+?) -> (.+?): (.+)$', 2) AS error_entity,
    REGEXP_EXTRACT(message, '^failed-table-crawl: (.+?) -> (.+?): (.+)$', 3) AS error_message,
FROM inventory.logs
  WHERE
    STARTSWITH(message, 'failed-table-crawl: ')
)
SELECT
  timestamp,
  error_entity,
  error_reason,
  error_message
FROM table_crawl_failures
ORDER BY
  1
