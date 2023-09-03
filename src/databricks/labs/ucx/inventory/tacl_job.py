import logging
from databricks.sdk.service import compute, jobs
import yaml

from databricks.labs.ucx.config import InventoryConfig, TaclConfig

logger = logging.getLogger(__name__)


def create_tacl_job(ws, dbfs_wheel, config_file):

    created_job = ws.jobs.create(
        tasks=[
            jobs.Task(
                task_key='crawl',
                python_wheel_task=jobs.PythonWheelTask(
                    package_name='databricks_labs_ucx',
                    entry_point='tacl',
                    named_parameters={"inventory_catalog": None,
                                      "inventory_schema": None,
                                      "databases": None}
                    ,
                ),
                libraries=[compute.Library(whl=f"dbfs:{dbfs_wheel}")],
                new_cluster=compute.ClusterSpec(
                    node_type_id=ws.clusters.select_node_type(local_disk=True),
                    spark_version=ws.clusters.select_spark_version(latest=True),
                    num_workers=1,
                    spark_conf={"spark.databricks.acl.sqlOnly": True}
                ),
            )
        ],
        name='[UCX] Crawl Tables',
    )

    logger.debug(f"Job created {created_job.job_id}")

    return created_job
