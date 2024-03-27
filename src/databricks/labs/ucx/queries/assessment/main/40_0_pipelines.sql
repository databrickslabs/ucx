-- viz type=table, name=Pipelines, columns=finding,pipeline_name,creator_name
-- widget title=Incompatible Delta Live Tables, row=42, col=0, size_x=3, size_y=8
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    pipeline_name,
    creator_name
FROM $inventory.pipelines
ORDER BY pipeline_name DESC