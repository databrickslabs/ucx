select concat(catalog, ".", database,".", name) as table_name, object_type, table_format, case
when object_type == "MANAGED" and table_format == "DELTA" then 0.5 -- CTAS or recreate as external table, then SYNC
when object_type == "MANAGED" and table_format != "DELTA" then 2 -- Can vary depending of format
when object_type == "EXTERNAL" and table_format == "DELTA" and startswith(location, "dbfs:/") then 0.5 -- Must CTAS the target table
when object_type == "EXTERNAL" and table_format == "DELTA" and startswith(location, "wasbs:/") then 1 -- Must Offload data to abfss
when object_type == "EXTERNAL" and table_format == "DELTA" and startswith(location, "adl:/") then 1 -- Must Offload data to abfss
when object_type == "EXTERNAL" and table_format == "DELTA" then 0.1 -- In place SYNC, mostly quick
when object_type == "EXTERNAL" and table_format in ("SQLSERVER", "MYSQL", "SNOWFLAKE") then 2 -- Must uses Lakehouse Federation
when object_type == "EXTERNAL" and table_format != "DELTA" then 1 -- Can vary depending of format
when object_type == "VIEW" then 2 -- Can vary depending of view complexity and number of tables used in the view
else NULL
end as estimated_hours from $inventory.tables
where not startswith(name, "__apply_changes")