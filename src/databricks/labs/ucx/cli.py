import json
import logging
import sys
import webbrowser

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.account import AccountWorkspaces, WorkspaceInfo
from databricks.labs.ucx.config import AccountConfig, ConnectConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.framework.tui import Prompts
from databricks.labs.ucx.hive_metastore import ExternalLocations, TablesCrawler
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.installer import InstallationManager

logger = logging.getLogger("databricks.labs.ucx")

CANT_FIND_UCX_MSG = (
    "Couldn't find UCX configuration in the user's home folder. "
    "Make sure the current user has configured and installed UCX."
)


def workflows():
    ws = WorkspaceClient()
    installer = WorkspaceInstaller(ws)
    logger.info("Fetching deployed jobs...")
    print(json.dumps(installer.latest_job_status()))


def open_remote_config():
    ws = WorkspaceClient()
    installer = WorkspaceInstaller(ws)

    ws_file_url = installer.notebook_link(installer.config_file)
    webbrowser.open(ws_file_url)


def list_installations():
    ws = WorkspaceClient()
    installation_manager = InstallationManager(ws)
    logger.info("Fetching installations...")
    all_users = [_.as_summary() for _ in installation_manager.user_installations()]
    print(json.dumps(all_users))


def skip(schema: str, table: str | None = None):
    logger.info("Running skip command")
    if not schema:
        logger.error("--Schema is a required parameter.")
        return None
    ws = WorkspaceClient()
    installation_manager = InstallationManager(ws)
    installation = installation_manager.for_user(ws.current_user.me())
    if not installation:
        logger.error(CANT_FIND_UCX_MSG)
        return None
    warehouse_id = installation.config.warehouse_id
    sql_backend = StatementExecutionBackend(ws, warehouse_id)
    mapping = TableMapping(ws)
    if table:
        mapping.skip_table(sql_backend, schema, table)
    else:
        mapping.skip_schema(sql_backend, schema)


def sync_workspace_info():
    workspaces = AccountWorkspaces(AccountConfig(connect=ConnectConfig()))
    workspaces.sync_workspace_info()


def manual_workspace_info():
    ws = WorkspaceClient()
    prompts = Prompts()
    workspace_info = WorkspaceInfo(ws)
    workspace_info.manual_workspace_info(prompts)


def create_table_mapping():
    ws = WorkspaceClient()
    table_mapping = TableMapping(ws)
    workspace_info = WorkspaceInfo(ws)
    installation_manager = InstallationManager(ws)
    installation = installation_manager.for_user(ws.current_user.me())
    sql_backend = StatementExecutionBackend(ws, installation.config.warehouse_id)
    tables_crawler = TablesCrawler(sql_backend, installation.config.inventory_database)
    path = table_mapping.save(tables_crawler, workspace_info)
    webbrowser.open(f"{ws.config.host}/#workspace{path}")


def validate_external_locations():
    ws = WorkspaceClient()
    prompts = Prompts()
    installation_manager = InstallationManager(ws)
    installation = installation_manager.for_user(ws.current_user.me())
    sql_backend = StatementExecutionBackend(ws, installation.config.warehouse_id)
    location_crawler = ExternalLocations(ws, sql_backend, installation.config.inventory_database)
    path = location_crawler.save_as_terraform_definitions_on_workspace(installation.path)
    if path and prompts.confirm(f"external_locations.tf file written to {path}. Do you want to open it?"):
        webbrowser.open(f"{ws.config.host}/#workspace{path}")


def ensure_assessment_run():
    ws = WorkspaceClient()
    installation_manager = InstallationManager(ws)
    installation = installation_manager.for_user(ws.current_user.me())
    if not installation:
        logger.error(CANT_FIND_UCX_MSG)
        return None
    else:
        workspace_installer = WorkspaceInstaller(ws)
        workspace_installer.validate_and_run("assessment")


MAPPING = {
    "open-remote-config": open_remote_config,
    "installations": list_installations,
    "workflows": workflows,
    "sync-workspace-info": sync_workspace_info,
    "manual-workspace-info": manual_workspace_info,
    "create-table-mapping": create_table_mapping,
    "validate-external-locations": validate_external_locations,
    "ensure-assessment-run": ensure_assessment_run,
    "skip": skip,
}


def main(raw):
    payload = json.loads(raw)
    command = payload["command"]
    if command not in MAPPING:
        msg = f"cannot find command: {command}"
        raise KeyError(msg)
    flags = payload["flags"]
    log_level = flags.pop("log_level")
    if log_level == "disabled":
        log_level = "info"
    databricks_logger = logging.getLogger("databricks")
    databricks_logger.setLevel(log_level.upper())
    kwargs = {k.replace("-", "_"): v for k, v in flags.items()}
    MAPPING[command](**kwargs)


if __name__ == "__main__":
    main(*sys.argv[1:])
