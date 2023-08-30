from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs


def create_tacl_job(cluster_id, wsfs_wheel):
    ws = WorkspaceClient()

    created_job = ws.jobs.create(
        tasks=[
            jobs.Task(
                task_key='crawl',
                python_wheel_task=jobs.PythonWheelTask(
                    package_name='databricks.labs.ucx.toolkits.table_acls',
                    entry_point='main',
                    parameters=['inventory_catalog', 'inventory_schema'],
                ),
                libraries=[compute.Library(whl=f"/Workspace{wsfs_wheel}")],
                new_cluster=compute.ClusterSpec(
                    node_type_id=ws.clusters.select_node_type(local_disk=True),
                    spark_version=ws.clusters.select_spark_version(latest=True),
                    num_workers=0,
                ),
            )
        ],
        name='[UCX] Crawl Tables',
    )
