-- --title 'Incompatible submit runs detected'
SELECT * FROM
(SELECT object_type, object_id, EXPLODE(from_json(failures, 'array<string>')) AS finding
FROM inventory.objects)
WHERE finding = "no data security mode specified"
  AND object_type = "submit_runs"

