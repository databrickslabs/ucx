-- --title 'Pipelines'
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    pipeline_name,
    creator_name
FROM inventory.pipelines
ORDER BY pipeline_name DESC
