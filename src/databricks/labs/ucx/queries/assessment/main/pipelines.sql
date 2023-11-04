-- viz type=table, name=Pipelines, columns=pipeline_id,pipeline_name,creator_name,Azure_SPN_Present,failures
-- widget title=Pipelines, col=0, row=53, size_x=6, size_y=8
select
       pipeline_id,
       pipeline_name,
       creator_name,
       IF(success = 1, "No", "Yes") AS Azure_SPN_Present,
       failures
from
  $inventory.pipelines