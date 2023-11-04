-- viz type=table, name=Assessed Objects, columns=cluster_id,cluster_name,creator,compatible,failures
-- widget title=Objects, col=0, row=0, size_x=2, size_y=15
with objects as (select 'clusters' as object_type, count(distinct cluster_id) count from $inventory.clusters
UNION ALL
select 'external_locations' as object_type, count(distinct location) from $inventory.external_locations
UNION ALL
select 'global_init_scripts' as object_type, count(distinct script_id) from $inventory.global_init_scripts
UNION ALL
select 'grants' as object_type, count(distinct t1.principal, t1.action_type, t1.catalog, t1.database,table, t1.view) from $inventory.grants t1
UNION ALL
select 'jobs' as object_type, count(distinct job_id) from $inventory.jobs
UNION ALL
select 'mounts' as object_type, count(distinct name, source) from $inventory.mounts
UNION ALL
select 'permissions' as object_type, count(distinct object_id) from $inventory.permissions
UNION ALL
select 'pipelines' as object_type, count(distinct pipeline_id) from $inventory.pipelines
UNION ALL
select 'tables' as object_type, count(distinct t1.catalog, database, name) from $inventory.tables t1
UNION ALL
select 'schemas' as object_type, count(distinct t1.catalog,database) from $inventory.tables t1
UNION ALL
select 'catalogs' as object_type, count(distinct t1.catalog) from $inventory.tables t1
UNION ALL
select 'workspace objects' as object_type, count(distinct *) from $inventory.workspace_objects
)
select * from objects order by count desc
