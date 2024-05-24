-- viz type=counter, name=Data Reconciliation Results, counter_label=Table migration success rate, value_column=success_rate
-- widget row=2, col=4, size_x=2, size_y=4
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
    $inventory.reconciliation_results