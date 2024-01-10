from databricks.labs.blueprint.commands import CommandExecutor

from databricks.labs.ucx.framework.crawlers import SchemaDeployer
from databricks.labs.ucx.hive_metastore.grants import Grant


def test_deploys_database(sql_backend, inventory_schema):
    from databricks.labs import ucx

    deployer = SchemaDeployer(sql_backend, inventory_schema, ucx)
    deployer.deploy_schema()
    deployer.deploy_table("grants", Grant)
    deployer.deploy_view("grant_detail", "queries/views/grant_detail.sql")


def test_runtime_backend_incorrect_schema_and_table_handled(ws, wsfs_wheel):
    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    query_response_incorrect_schema = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import NotFound
        backend = RuntimeBackend()
        try:
            backend.execute("USE ABCDEF")
            return "FAILED"
        except NotFound as e:
            return "PASSED"
        """
    )
    assert query_response_incorrect_schema == "PASSED"

    query_response_incorrect_table = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import NotFound
        backend = RuntimeBackend()
        try:
            backend.execute("SELECT * FROM default.ABCDEF")
            return "FAILED"
        except NotFound as e:
            return "PASSED"
        """
    )
    assert query_response_incorrect_table == "PASSED"


def test_runtime_backend_incorrect_syntax_handled(ws, wsfs_wheel):
    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    query_response_incorrect_syntax = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import BadRequest
        backend = RuntimeBackend()
        try:
            backend.execute("SHWO DTABASES")
            return "FAILED"
        except BadRequest:
            return "PASSED"
        """
    )
    assert query_response_incorrect_syntax == "PASSED"


def test_runtime_backend_permission_denied_handled(
    ws,
    wsfs_wheel,
):
    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    query_response_permission_denied = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import PermissionDenied, BadRequest
        backend = RuntimeBackend()
        try:
            current_user = backend.fetch("SELECT current_user()")
            backend.execute(f"GRANT CREATE EXTERNAL LOCATION ON METASTORE TO {current_user}")
            #return "FAILED"
        except BadRequest:
            return "PASSED"
        """
    )
    assert query_response_permission_denied == "PASSED"
