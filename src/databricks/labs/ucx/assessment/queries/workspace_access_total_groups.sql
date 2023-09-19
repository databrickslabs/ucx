-- viz type=table, name=Workspace Groups per Object Type, columns=object_type,groups
-- widget title=Table Types
SELECT
  object_type,
  COUNT(DISTINCT object_id) groups
FROM $inventory.permissions
GROUP BY 1