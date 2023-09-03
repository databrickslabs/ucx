import logging
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from databricks.sdk.service.workspace import ImportFormat
from databricks.labs.ucx.providers.mixins.compute import CommandExecutor
from databricks.labs.ucx.inventory.tacl_job import create_tacl_job

logging.getLogger('databricks.sdk').setLevel('DEBUG')


@pytest.fixture
def fresh_wheel_file(tmp_path) -> Path:
    this_file = Path(__file__)
    project_root = this_file.parent.parent.parent.absolute()
    # TODO: we can dynamically determine this with python -m build .
    wheel_name = "databricks_labs_ucx"

    build_root = tmp_path / fresh_wheel_file.__name__
    shutil.copytree(project_root, build_root)
    try:
        completed_process = subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "."],
            capture_output=True,
            cwd=build_root,
            check=True,
        )
        if completed_process.returncode != 0:
            raise RuntimeError(completed_process.stderr)

        found_wheels = list(build_root.glob(f"{wheel_name}-*.whl"))
        if not found_wheels:
            msg = f"cannot find {wheel_name}-*.whl"
            raise RuntimeError(msg)
        if len(found_wheels) > 1:
            conflicts = ", ".join(str(whl) for whl in found_wheels)
            msg = f"more than one wheel match: {conflicts}"
            raise RuntimeError(msg)
        wheel_file = found_wheels[0]

        return wheel_file
    except subprocess.CalledProcessError as e:
        raise RuntimeError(e.stderr) from None


@pytest.fixture
def wsfs_wheel(ws, fresh_wheel_file, make_random):
    my_user = ws.current_user.me().user_name
    workspace_location = f"/Users/{my_user}/wheels/{make_random(10)}"
    ws.workspace.mkdirs(workspace_location)

    wsfs_wheel = f"{workspace_location}/{fresh_wheel_file.name}"
    with fresh_wheel_file.open("rb") as f:
        ws.workspace.upload(wsfs_wheel, f, format=ImportFormat.AUTO)

    yield wsfs_wheel

    ws.workspace.delete(workspace_location, recursive=True)

@pytest.fixture
def dbfs_wheel(ws, fresh_wheel_file, make_random):
    my_user = ws.current_user.me().user_name
    dbfs_location = f"/FileStore/jars/{make_random(10)}"
    ws.workspace.mkdirs(dbfs_location)

    dbfs_wheel = f"{dbfs_location}/{fresh_wheel_file.name}"
    with fresh_wheel_file.open("rb") as f:
        ws.dbfs.upload(dbfs_wheel, f)

    yield dbfs_wheel

    ws.dbfs.delete(dbfs_location, recursive=True)
    ws.workspace.delete(dbfs_location)


def test_this_wheel_installs(ws, wsfs_wheel):
    commands = CommandExecutor(ws)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    installed_version = commands.run(
        """
        from databricks.labs.ucx.__about__ import __version__
        print(__version__)
        """
    )

    assert installed_version is not None


def test_sql_backend_works(ws, wsfs_wheel):
    commands = CommandExecutor(ws)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    database_names = commands.run(
        """
        from databricks.labs.ucx.tacl._internal import RuntimeBackend
        backend = RuntimeBackend()
        return backend.fetch("SHOW DATABASES")
        """
    )

    assert len(database_names) > 0


def test_job_creation(ws, dbfs_wheel, sql_exec, sql_fetch_all, make_catalog, make_schema, make_table, make_group):

    group_a = make_group(display_name='sdk_group_a')
    group_b = make_group(display_name='sdk_group_b')
    schema_a = make_schema()
    schema_b = make_schema()
    managed_table = make_table(schema=schema_a)
    tmp_table = make_table(schema=schema_b, ctas="SELECT 2+2 AS four")
    view = make_table(schema=schema_a, ctas="SELECT 2+2 AS four", view=True)

    logging.info(
        f"managed_table={managed_table}, "
        f"tmp_table={tmp_table}, "
        f"view={view}"
    )

    sql_exec(f"GRANT USAGE ON SCHEMA default TO {group_a.display_name}")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO {group_b.display_name}")
    sql_exec(f"GRANT SELECT ON TABLE {managed_table} TO {group_a.display_name}")
    sql_exec(f"GRANT SELECT ON TABLE {tmp_table} TO {group_b.display_name}")
    sql_exec(f"GRANT SELECT ON TABLE {view} TO {group_a.display_name}")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema_b} TO {group_b.display_name}")

    inventory_schema = make_schema(catalog=make_catalog())
    inventory_catalog, inventory_schema = inventory_schema.split(".")

    created_job = create_tacl_job(ws, dbfs_wheel)

    databases = [schema_a.split(".")[1], schema_b.split(".")[1]]

    ws.jobs.run_now(created_job.job_id, python_named_params={"inventory_catalog": inventory_catalog,
                                                             "inventory_schema": inventory_schema,
                                                             "databases": ",".join(databases)}).result()

    tacl_tables = f"{inventory_catalog}.{inventory_schema}.tables"
    tacl_grants = f"{inventory_catalog}.{inventory_schema}.grants"

    tables = sql_fetch_all(f"SELECT * FROM {tacl_tables}")
    grants = sql_fetch_all(f"SELECT * FROM {tacl_grants}")

    all_tables = {}
    for t in tables:
        print(t.as_dict())
        all_tables[t.as_dict()["name"]] = t.as_dict()

    all_grants = {}
    for grant in grants:
        print(grant.as_dict())
        all_grants[grant.as_dict()["principal"]] = grant


    print(all_grants)
    print(all_tables)
    assert len(all_tables) == 3
    assert all_tables[managed_table].object_type == "MANAGED"
    assert all_tables[tmp_table].object_type == "MANAGED"
    assert all_tables[view].object_type == "VIEW"
    assert all_tables[view].view_text == "SELECT 2+2 AS four"
    assert len(all_grants) >= 2, "must have at least two grants"
    #assert all_grants[f"{group_a.display_name}.{managed_table}"] == "SELECT"
    #assert all_grants[f"{group_a.display_name}.{view}"] == "SELECT"
    #assert all_grants[f"{group_b.display_name}.{tmp_table}"] == "SELECT"
    #assert all_grants[f"{group_b.display_name}.{schema_b}"] == "MODIFY"
