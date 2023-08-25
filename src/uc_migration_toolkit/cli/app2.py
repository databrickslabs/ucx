import os, time
from databricks.sdk.service import jobs
from pathlib import Path
from typing import Annotated

import typer
from typer import Typer

app = Typer(name="UC Migration Toolkit", pretty_exceptions_show_locals=True)


@app.command()
def migrate_groups(config_file: Annotated[Path, typer.Argument(help="Path to config file")] = "migration_config.yml"):
    from uc_migration_toolkit.cli.utils import get_migration_config
    from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit

    config = get_migration_config(config_file)
    toolkit = GroupMigrationToolkit(config)
    toolkit.prepare_environment()

    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()
    toolkit.apply_permissions_to_backup_groups()
    toolkit.replace_workspace_groups_with_account_groups()
    toolkit.apply_permissions_to_account_groups()
    toolkit.delete_backup_groups()
    toolkit.cleanup_inventory_table()


@app.command()
def generate_assessment_report():
    from uc_migration_toolkit.toolkits.assessment import AssessmentToolkit
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    toolkit = AssessmentToolkit(ws)
    report = toolkit.generate_report()
    print(report)


@app.command()
def generate_assessment_report2():
    from uc_migration_toolkit.toolkits.assessment import AssessmentToolkit
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    for cluster in ws.clusters.list():
        print(cluster.cluster_name)
    # mounts=ws.dbutils.fs.mounts()
    # for mount in mounts:
    #     print(mount)
    # ws.dbfs.mkdirs("/tmp/uc_assessment/")
    # notebook_path = f"{os.getcwd()}/../toolkits/assessment/Step_0_HMS_Inventory.py"
    notebook_path = "/tmp/uc_assessment/Step_0_HMS_Inventory.py"
    # print(os.listdir(f"{os.getcwd()}/../toolkits/assessment"))
    # print(notebook_source)
    # for item in ws.dbfs.list("dbfs:/tmp/uc_assessment/"):
    #     print(item)
    # ws.dbfs.mkdirs("dbfs:/tmp/uc_assessment/")
    # ws.dbfs.copy(f"{os.getcwd()}/../toolkits/assessment/Step_0_HMS_Inventory.py","dbfs:/tmp/uc_assessment/Step_0_HMS_Inventory.py")
    for item in ws.dbfs.list("/tmp/uc_assessment/"):
        print(item)
    # cluster_id = \
    #     ws.clusters.ensure_cluster_is_running(os.environ["DATABRICKS_CLUSTER_ID"]) \
    #     and os.environ["DATABRICKS_CLUSTER_ID"]
    cluster_id = "0825-124924-sbf1334d"

    run = ws.jobs.submit(run_name=f'sdk-{time.time_ns()}',
                         tasks=[
                             jobs.SubmitTask(existing_cluster_id=cluster_id,
                                                notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
                                                task_key=f'sdk-{time.time_ns()}')
                         ]).result()

    # cleanup
    ws.jobs.delete_run(run_id=run.run_id)


if __name__ == "__main__":
    app()
