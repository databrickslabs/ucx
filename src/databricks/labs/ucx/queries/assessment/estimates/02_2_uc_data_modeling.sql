-- viz type=table, name=Tables to migrate, columns=catalog,database,name,object_type,table_format,location,view_text,upgraded_to,storage_properties
-- widget title=Tables to migrate, row=2, col=2, size_x=3, size_y=8
select * from $inventory.tables;