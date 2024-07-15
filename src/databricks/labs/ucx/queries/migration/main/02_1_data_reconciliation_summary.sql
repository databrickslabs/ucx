/* --title 'Data Reconciliation Results' --width 6 */
SELECT
  COUNT(CASE WHEN schema_matches AND data_matches THEN 1 ELSE NULL END) AS success,
  COUNT(*) AS total,
  CONCAT(ROUND(TRY_DIVIDE(success, total) * 100, 2), '%') AS success_rate
FROM inventory.reconciliation_results