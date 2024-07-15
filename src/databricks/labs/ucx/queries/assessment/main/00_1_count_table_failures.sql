/* --title 'Metastore Crawl Failures' */
SELECT
  COUNT(*) AS count_failures
FROM inventory.table_failures