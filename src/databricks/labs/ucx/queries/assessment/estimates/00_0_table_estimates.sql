-- viz type=table, name=Table estimates, columns=catalog,database,name,object_type,table_format,estimated_hours
-- widget title=Table estimates, row=0, col=0, size_x=2, size_y=8
WITH table_estimates AS (
    select catalog, database, name, object_type, table_format, case
    when object_type == "MANAGED" and table_format == "DELTA" then 0.5 -- CTAS or recreate as external table, then SYNC
    when object_type == "MANAGED" and table_format != "DELTA" then 2 -- Can vary depending of format
    when object_type == "EXTERNAL" and table_format == "DELTA" and startswith(location, "dbfs:/") then 0.5 -- Must CTAS the target table
    when object_type == "EXTERNAL" and table_format == "DELTA" then 0.2 -- In place SYNC, mostly quick
    when object_type == "EXTERNAL" and table_format in ("SQLSERVER", "MYSQL", "SNOWFLAKE") then 3 -- Must uses Lakehouse Federation
    when object_type == "EXTERNAL" and table_format != "DELTA" then 1 -- Can vary depending of format
    when object_type == "VIEW" then 6 -- Can vary depending of view complexity and number of tables used in the view
    else NULL
    end as estimated_hours from hive_metastore.ucx_wc1017.tables
    where not startswith(name, "__apply_changes")
)
SELECT * FROM table_estimates