-- --title 'Data Reconciliation Results'
SELECT
  COUNT(
    CASE
      WHEN schema_matches
      AND data_matches THEN 1
      ELSE NULL
    END
  ) AS success,
  count(*) AS total,
  concat(round(success / total * 100, 2), '%') AS success_rate
FROM
    inventory.reconciliation_results
