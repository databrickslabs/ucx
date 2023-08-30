import datetime
import logging
import time

import databricks.sdk.service.jobs as j
from databricks.sdk import WorkspaceClient

# logger = logging.getLogger(__name__)
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")


def crawl_tacl(cluster_id, ws, inventory_catalog, inventory_schema):
    w = WorkspaceClient()

    logging.info("Crawler started")

    # trigger one-time-run job and get waiter object
    waiter = w.jobs.submit(
        run_name=f"sdk-{time.time_ns()}",
        tasks=[
            j.SubmitTask(
                existing_cluster_id=cluster_id,
                python_wheel_task=j.PythonWheelTask(
                    "crawler", package_name="databricks-labs-ucx", parameters=[inventory_catalog, inventory_schema]
                ),
                task_key=f"sdk-{time.time_ns()}",
            )
        ],
    )

    logging.info(f"starting to poll: {waiter.run_id}")

    def print_status(run: j.Run):
        statuses = [f"{t.task_key}: {t.state.life_cycle_state}" for t in run.tasks]
        logging.info(f'workflow intermediate status: {", ".join(statuses)}')

    run = waiter.result(timeout=datetime.timedelta(minutes=15), callback=print_status)

    logging.info(f"job finished: {run.run_page_url}")
