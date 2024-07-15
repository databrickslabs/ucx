/* --title 'Assessment Summary' --width 4 --height 6 */
WITH raw AS (
  SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding
  FROM inventory.objects
  WHERE
    failures <> '[]'
)
SELECT
  finding AS `finding`,
  COUNT(*) AS count
FROM raw
GROUP BY
  finding
ORDER BY
  count DESC,
  finding DESC