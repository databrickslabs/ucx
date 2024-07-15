/* --title 'UDF Summary' --filter name --width 6 */
SELECT
  catalog,
  database,
  name,
  func_type,
  func_input,
  func_returns,
  deterministic,
  data_access,
  body,
  comment
FROM inventory.udfs