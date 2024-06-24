-- --title 'Tables to migrate' --height 10 --width 3
select
  catalog,
  database,
  name,
  object_type,
  table_format,
  location,
  view_text,
  upgraded_to,
  storage_properties
from inventory.tables;
