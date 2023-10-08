-- viz type=table, name=Global Init Scripts, columns=script_id,script_name,created_by,enabled,Azure_SPN_Present,failures
-- widget title=Global Init Scripts, col=0, row=57, size_x=6, size_y=8
select
  script_id,
  script_name,
  created_by,
  if(enabled = True, "Yes", "No") enabled,
  if(success = 1, "No", "Yes") Azure_SPN_Present,
  failures
from
  $inventory.global_init_scripts