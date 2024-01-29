from databricks.labs.blueprint.commands import CommandExecutor

from databricks.labs.ucx.framework.crawlers import SchemaDeployer
from databricks.labs.ucx.hive_metastore.grants import Grant


def test_deploys_database(sql_backend, inventory_schema):
    from databricks.labs import ucx

    deployer = SchemaDeployer(sql_backend, inventory_schema, ucx)
    deployer.deploy_schema()
    deployer.deploy_table("grants", Grant)
    deployer.deploy_view("grant_detail", "queries/views/grant_detail.sql")


def test_runtime_backend_incorrect_schema_and_table_handled(ws, wsfs_wheel, make_random):
    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    query_response_incorrect_schema_execute = commands.run(
        f"""
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import NotFound
        backend = RuntimeBackend()
        try:
            backend.execute("USE {make_random()}")
            return "FAILED"
        except NotFound as e:
            return "PASSED"
        """
    )
    assert query_response_incorrect_schema_execute == "PASSED"

    query_response_incorrect_table_execute = commands.run(
        f"""
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import NotFound
        backend = RuntimeBackend()
        try:
            backend.execute("SELECT * FROM default.{make_random()}")
            return "FAILED"
        except NotFound as e:
            return "PASSED"
        """
    )
    assert query_response_incorrect_table_execute == "PASSED"

    query_response_incorrect_schema_fetch = commands.run(
        f"""
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import NotFound
        backend = RuntimeBackend()
        try:
            query_response = backend.fetch("DESCRIBE {make_random()}")
            return "FAILED"
        except NotFound as e:
            return "PASSED"
        """
    )
    assert query_response_incorrect_schema_fetch == "PASSED"

    query_response_incorrect_table_fetch = commands.run(
        f"""
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import NotFound
        backend = RuntimeBackend()
        try:
            query_response = backend.fetch("SELECT * FROM default.{make_random()}")
            return "FAILED"
        except NotFound as e:
            return "PASSED"
        """
    )
    assert query_response_incorrect_table_fetch == "PASSED"


def test_runtime_backend_incorrect_syntax_handled(ws, wsfs_wheel):
    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    query_response_incorrect_syntax_execute = commands.run(
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
    assert query_response_incorrect_syntax_execute == "PASSED"

    query_response_incorrect_syntax_fetch = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import BadRequest
        backend = RuntimeBackend()
        try:
            query_response = backend.fetch("SHWO DTABASES")
            return "FAILED"
        except BadRequest:
            return "PASSED"
        """
    )
    assert query_response_incorrect_syntax_fetch == "PASSED"


def test_runtime_backend_permission_denied_handled(ws, wsfs_wheel):
    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    query_response_permission_denied_execute = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import PermissionDenied
        backend = RuntimeBackend()
        try:
            current_user = backend.fetch("SELECT current_user()")
            backend.execute(f"GRANT CREATE EXTERNAL LOCATION ON METASTORE TO {current_user}")
            return "FAILED"
        except PermissionDenied:
            return "PASSED"
        """
    )
    assert query_response_permission_denied_execute == "PASSED"

    query_response_permission_denied_fetch = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import PermissionDenied
        backend = RuntimeBackend()
        try:
            current_user = backend.fetch(f"SELECT current_user()")
            grants = backend.fetch(f"GRANT CREATE EXTERNAL LOCATION ON METASTORE TO {current_user}")
            return "FAILED"
        except PermissionDenied:
            return "PASSED"
        """
    )
    assert query_response_permission_denied_fetch == "PASSED"


def test_runtime_backend_unknown_error_handled(ws, wsfs_wheel):
    commands = CommandExecutor(ws.clusters, ws.command_execution, lambda: ws.config.cluster_id)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    query_response_unknown_execute = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import Unknown
        backend = RuntimeBackend()
        try:
            backend.execute("SHOW GRANTS ON METASTORE")
            return "FAILED"
        except Unknown:
            return "PASSED"
        """
    )
    assert query_response_unknown_execute == "PASSED"

    query_response_unknown_fetch = commands.run(
        """
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        from databricks.sdk.errors import Unknown
        backend = RuntimeBackend()
        try:
            grants = backend.fetch("SHOW GRANTS ON METASTORE")
            return "FAILED"
        except Unknown:
            return "PASSED"
        """
    )
    assert query_response_unknown_fetch == "PASSED"
