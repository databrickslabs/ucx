-- viz type=table, name=Workspace Groups per Object Type, columns=object_type,objects
-- widget title=Object Types, col=0, row=3, size_x=1, size_y=12
SELECT
  object_type,
  COUNT(DISTINCT object_id) objects
FROM $inventory.permissions
GROUP BY 1
ORDER BY objects DESC