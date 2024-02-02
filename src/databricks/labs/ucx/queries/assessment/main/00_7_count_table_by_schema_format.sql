-- viz type=table, name=Table counts by schema and format, columns=schema,format,count
-- widget title=Table counts by schema and format, row=2, col=4, size_x=2, size_y=5
SELECT
  schema,
  format,
  COUNT(*) count
FROM
  (
    SELECT
      `database` AS schema,
      IF(table_format = 'DELTA', "Delta", "Non Delta") AS format
    FROM
      $inventory.tables
  )
WHERE
  format IS NOT NULL
GROUP BY
  schema,
  format
ORDER BY
  schema,
  format;