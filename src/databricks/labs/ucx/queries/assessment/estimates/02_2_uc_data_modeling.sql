-- --title 'Tables to migrate'
-- --title Tables to migrate, row=2, col=2, size_x=3, size_y=8
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
