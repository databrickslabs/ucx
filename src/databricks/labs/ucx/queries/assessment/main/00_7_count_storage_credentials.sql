/* --title 'Total Storage Credentials'
--overrides '{"spec":{"encodings":{"value":{"style":{"rules":[{"condition":{"operator":">","operand":{"type":"data-value","value":"200"}},"color":"#E92828"}]}}}}}' */
SELECT
  COUNT(*) AS count_total_storage_credentials
FROM inventory.external_locations
