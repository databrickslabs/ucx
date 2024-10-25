import json
from collections.abc import Callable, Generator
import functools
import collections
import os
import logging
from datetime import timedelta
import shutil
import subprocess

import pytest  # pylint: disable=wrong-import-order
from databricks.labs.blueprint.commands import CommandExecutor
from databricks.labs.blueprint.entrypoint import is_in_debug
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.catalog import SchemaInfo, TableInfo
from databricks.sdk.service.dashboards import Dashboard as SDKDashboard
from databricks.sdk.service.sql import Dashboard, WidgetPosition, WidgetOptions, LegacyQuery

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.labs.ucx.workspace_access.groups import MigratedGroup
from tests.integration.contexts.azure import MockLocalAzureCli
from tests.integration.contexts.installation import MockInstallationContext
from tests.integration.contexts.runtime import MockRuntimeContext

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


@pytest.fixture
def debug_env_name():
    return "ucws"


@pytest.fixture
def product_info():
    return "ucx", __version__


@pytest.fixture
def inventory_schema(make_schema):
    return make_schema(catalog_name="hive_metastore").name


@pytest.fixture
def make_lakeview_dashboard(ws, make_random, env_or_skip, watchdog_purge_suffix):
    """Create a lakeview dashboard."""
    warehouse_id = env_or_skip("TEST_DEFAULT_WAREHOUSE_ID")
    serialized_dashboard = {
        "datasets": [{"name": "fourtytwo", "displayName": "count", "query": "SELECT 42 AS count"}],
        "pages": [
            {
                "name": "count",
                "displayName": "Counter",
                "layout": [
                    {
                        "widget": {
                            "name": "counter",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "fourtytwo",
                                        "fields": [{"name": "count", "expression": "`count`"}],
                                        "disaggregated": True,
                                    },
                                }
                            ],
                            "spec": {
                                "version": 2,
                                "widgetType": "counter",
                                "encodings": {"value": {"fieldName": "count", "displayName": "count"}},
                            },
                        },
                        "position": {"x": 0, "y": 0, "width": 1, "height": 3},
                    }
                ],
            }
        ],
    }

    def create(display_name: str = "") -> SDKDashboard:
        if display_name:
            display_name = f"{display_name} ({make_random()})"
        else:
            display_name = f"created_by_ucx_{make_random()}_{watchdog_purge_suffix}"
        dashboard = ws.lakeview.create(
            display_name,
            serialized_dashboard=json.dumps(serialized_dashboard),
            warehouse_id=warehouse_id,
        )
        ws.lakeview.publish(dashboard.dashboard_id)
        return dashboard

    def delete(dashboard: SDKDashboard) -> None:
        ws.lakeview.trash(dashboard.dashboard_id)

    yield from factory("dashboard", create, delete)


@pytest.fixture
def make_dashboard(
    ws: WorkspaceClient,
    make_random: Callable[[int], str],
    make_query,
    watchdog_purge_suffix,
):
    """Create a legacy dashboard.
    This fixture is used to test migrating legacy dashboards to Lakeview.
    """

    def create(query: LegacyQuery | None = None) -> Dashboard:
        if not query:
            query = make_query()
        assert query
        assert query.id
        viz = ws.query_visualizations_legacy.create(
            type="table",
            query_id=query.id,
            options={
                "itemsPerPage": 1,
                "condensed": True,
                "withRowNumber": False,
                "version": 2,
                "columns": [
                    {"name": "id", "title": "id", "allowSearch": True},
                ],
            },
        )

        dashboard_name = f"ucx_D{make_random(4)}_{watchdog_purge_suffix}"
        dashboard = ws.dashboards.create(name=dashboard_name, tags=["original_dashboard_tag"])
        assert dashboard.id is not None
        ws.dashboard_widgets.create(
            dashboard_id=dashboard.id,
            visualization_id=viz.id,
            width=1,
            options=WidgetOptions(
                title="",
                position=WidgetPosition(
                    col=0,
                    row=0,
                    size_x=3,
                    size_y=3,
                ),
            ),
        )
        logger.info(f"Dashboard Created {dashboard_name}: {ws.config.host}/sql/dashboards/{dashboard.id}")
        return dashboard

    def remove(dashboard: Dashboard) -> None:
        try:
            assert dashboard.id is not None
            ws.dashboards.delete(dashboard_id=dashboard.id)
        except RuntimeError as e:
            logger.info(f"Can't delete dashboard {e}")

    yield from factory("dashboard", create, remove)


@pytest.fixture
def make_dbfs_data_copy(ws, make_cluster, env_or_skip):
    _ = make_cluster  # Need cluster to copy data
    if ws.config.is_aws:
        cmd_exec = CommandExecutor(ws.clusters, ws.command_execution, lambda: env_or_skip("TEST_WILDCARD_CLUSTER_ID"))

    def create(*, src_path: str, dst_path: str, wait_for_provisioning=True):
        @retried(on=[NotFound], timeout=timedelta(minutes=2))
        def _wait_for_provisioning(path) -> None:
            if not ws.dbfs.exists(path):
                raise NotFound(f"Location not found: {path}")

        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.cp('{src_path}', '{dst_path}', recurse=True)")
        else:
            ws.dbfs.copy(src_path, dst_path, recursive=True)
            if wait_for_provisioning:
                _wait_for_provisioning(dst_path)
        return dst_path

    def remove(dst_path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.rm('{dst_path}', recurse=True)")
        else:
            ws.dbfs.delete(dst_path, recursive=True)

    yield from factory("make_dbfs_data_copy", create, remove)


@pytest.fixture
def make_mounted_location(make_random, make_dbfs_data_copy, env_or_skip, watchdog_purge_suffix):
    """Make a copy of source data to a new location

    Use the fixture to avoid overlapping UC table path that will fail other external table migration tests.

    Note:
        This fixture is different to the other `make_` fixtures as it does not return a `Callable` to make the mounted
        location; the mounted location is made with fixture setup already.
    """
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    # DBFS locations are not purged; no suffix necessary.
    new_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{make_random(4)}-{watchdog_purge_suffix}'
    make_dbfs_data_copy(src_path=existing_mounted_location, dst_path=new_mounted_location)
    return new_mounted_location


@pytest.fixture
def make_storage_dir(ws, env_or_skip):
    if ws.config.is_aws:
        cmd_exec = CommandExecutor(ws.clusters, ws.command_execution, lambda: env_or_skip("TEST_WILDCARD_CLUSTER_ID"))

    def create(*, path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.mkdirs('{path}')")
        else:
            ws.dbfs.mkdirs(path)
        return path

    def remove(path: str):
        if ws.config.is_aws:
            cmd_exec.run(f"dbutils.fs.rm('{path}', recurse=True)")
        else:
            ws.dbfs.delete(path, recursive=True)

    yield from factory("make_storage_dir", create, remove)


def get_workspace_membership(workspace_client, res_type: str = "WorkspaceGroup"):
    membership = collections.defaultdict(set)
    for group in workspace_client.groups.list(attributes="id,displayName,meta,members"):
        if group.display_name in {"users", "admins", "account users"}:
            continue
        if group.meta.resource_type != res_type:
            continue
        if group.members is None:
            continue
        for member in group.members:
            membership[group.display_name].add(member.display)
    return membership


@pytest.fixture
def migrated_group(acc, ws, make_group, make_acc_group):
    """Create a pair of groups in workspace and account. Assign account group to workspace."""
    ws_group = make_group()
    acc_group = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), acc_group.id, permissions=[iam.WorkspacePermission.USER])
    return MigratedGroup.partial_info(ws_group, acc_group)


@pytest.fixture
def make_ucx_group(make_random, make_group, make_acc_group, make_user, watchdog_purge_suffix):
    def inner(workspace_group_name=None, account_group_name=None, **kwargs):
        if not workspace_group_name:
            workspace_group_name = f"ucx_G{make_random(4)}-{watchdog_purge_suffix}"  # noqa: F405
        if not account_group_name:
            account_group_name = workspace_group_name
        user = make_user()
        members = [user.id]
        ws_group = make_group(
            display_name=workspace_group_name,
            members=members,
            entitlements=["allow-cluster-create"],
            **kwargs,
        )
        acc_group = make_acc_group(display_name=account_group_name, members=members, **kwargs)
        return ws_group, acc_group

    return inner


@pytest.fixture
def make_group_pair(make_random, make_group):
    def inner() -> MigratedGroup:
        suffix = make_random(4)
        ws_group = make_group(display_name=f"old_{suffix}", entitlements=["allow-cluster-create"])
        acc_group = make_group(display_name=f"new_{suffix}")
        return MigratedGroup.partial_info(ws_group, acc_group)

    return inner


def get_azure_spark_conf():
    return {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]",
        "fs.azure.account.auth.type.labsazurethings.dfs.core.windows.net": "OAuth",
        "fs.azure.account.oauth.provider.type.labsazurethings.dfs.core.windows.net": "org.apache.hadoop.fs"
        ".azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id.labsazurethings.dfs.core.windows.net": "dummy_application_id",
        "fs.azure.account.oauth2.client.secret.labsazurethings.dfs.core.windows.net": "dummy",
        "fs.azure.account.oauth2.client.endpoint.labsazurethings.dfs.core.windows.net": "https://login"
        ".microsoftonline.com/directory_12345/oauth2/token",
    }


# pylint: disable=too-many-arguments


@pytest.fixture
def runtime_ctx(
    ws,
    make_catalog,
    make_schema,
    make_table,
    make_udf,
    make_group,
    make_job,
    make_query,
    make_dashboard,
    env_or_skip,
    make_random,
) -> MockRuntimeContext:
    return MockRuntimeContext(
        ws,
        make_catalog,
        make_schema,
        make_table,
        make_udf,
        make_group,
        make_job,
        make_query,
        make_dashboard,
        env_or_skip,
        make_random,
    )


@pytest.fixture
def az_cli_ctx(
    ws,
    make_catalog,
    make_schema,
    make_table,
    make_udf,
    make_group,
    make_job,
    make_query,
    make_dashboard,
    env_or_skip,
    make_random,
):
    return MockLocalAzureCli(
        ws,
        make_catalog,
        make_schema,
        make_table,
        make_udf,
        make_group,
        make_job,
        make_query,
        make_dashboard,
        env_or_skip,
        make_random,
    )


@pytest.fixture
def aws_cli_ctx(installation_ctx, env_or_skip):
    if not installation_ctx.is_aws:
        pytest.skip("Aws only")
    if not shutil.which("aws"):
        pytest.skip("Local test only")
    return installation_ctx.replace(aws_profile=env_or_skip("AWS_PROFILE"))


@pytest.fixture
def installation_ctx(
    make_acc_group,
    make_user,
    watchdog_purge_suffix,
    ws,
    make_catalog,
    make_schema,
    make_table,
    make_udf,
    make_group,
    make_job,
    make_query,
    make_dashboard,
    env_or_skip,
    make_random,
) -> Generator[MockInstallationContext, None, None]:
    ctx = MockInstallationContext(
        make_acc_group,
        make_user,
        watchdog_purge_suffix,
        ws,
        make_catalog,
        make_schema,
        make_table,
        make_udf,
        make_group,
        make_job,
        make_query,
        make_dashboard,
        env_or_skip,
        make_random,
    )
    yield ctx.replace(workspace_client=ws)
    ctx.workspace_installation.uninstall()


def prepare_hiveserde_tables(context, random, schema, table_base_dir) -> dict[str, TableInfo]:
    tables: dict[str, TableInfo] = {}

    parquet_table_name = f"parquet_serde_{random}"
    parquet_ddl = f"CREATE TABLE hive_metastore.{schema.name}.{parquet_table_name} (id INT, region STRING) PARTITIONED BY (region) STORED AS PARQUETFILE LOCATION '{table_base_dir}/{parquet_table_name}'"
    tables[parquet_table_name] = context.make_table(
        schema_name=schema.name,
        name=parquet_table_name,
        hiveserde_ddl=parquet_ddl,
        storage_override=f"{table_base_dir}/{parquet_table_name}",
    )

    orc_table_name = f"orc_serde_{random}"
    orc_ddl = f"CREATE TABLE hive_metastore.{schema.name}.{orc_table_name} (id INT, region STRING) PARTITIONED BY (region) STORED AS ORC LOCATION '{table_base_dir}/{orc_table_name}'"
    tables[orc_table_name] = context.make_table(
        schema_name=schema.name,
        name=orc_table_name,
        hiveserde_ddl=orc_ddl,
        storage_override=f"{table_base_dir}/{orc_table_name}",
    )

    avro_table_name = f"avro_serde_{random}"
    avro_ddl = f"""CREATE TABLE hive_metastore.{schema.name}.{avro_table_name} (id INT, region STRING)
                        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
                        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
                                  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
                        TBLPROPERTIES ('avro.schema.literal'='{{
                            "namespace": "org.apache.hive",
                            "name": "first_schema",
                            "type": "record",
                            "fields": [
                                {{ "name":"id", "type":"int" }},
                                {{ "name":"region", "type":"string" }}
                            ] }}')
                        LOCATION '{table_base_dir}/{avro_table_name}'
                    """
    tables[avro_table_name] = context.make_table(
        schema_name=schema.name,
        name=avro_table_name,
        hiveserde_ddl=avro_ddl,
        storage_override=f"{table_base_dir}/{avro_table_name}",
    )
    return tables


def prepare_regular_tables(context, external_csv, schema) -> dict[str, TableInfo]:
    tables: dict[str, TableInfo] = {
        "src_managed_table": context.make_table(schema_name=schema.name),
        "src_managed_non_delta_table": context.make_table(
            catalog_name=schema.catalog_name, non_delta=True, schema_name=schema.name
        ),
        "src_external_table": context.make_table(schema_name=schema.name, external_csv=external_csv),
    }
    src_view1_text = f"SELECT * FROM {tables['src_managed_table'].full_name}"
    tables["src_view1"] = context.make_table(
        catalog_name=schema.catalog_name,
        schema_name=schema.name,
        ctas=src_view1_text,
        view=True,
    )
    src_view2_text = f"SELECT * FROM {tables['src_view1'].full_name}"
    tables["src_view2"] = context.make_table(
        catalog_name=schema.catalog_name,
        schema_name=schema.name,
        ctas=src_view2_text,
        view=True,
    )
    return tables


@pytest.fixture
def prepare_tables_for_migration(
    ws, installation_ctx, make_catalog, make_random, make_mounted_location, env_or_skip, make_storage_dir, request
) -> tuple[dict[str, TableInfo], SchemaInfo]:
    # Here we use pytest indirect parametrization, so the test function can pass arguments to this fixture and the
    # arguments will be available in the request.param. If the argument is "hiveserde", we will prepare hiveserde
    # tables, otherwise we will prepare regular tables.
    # see documents here for details https://docs.pytest.org/en/8.1.x/example/parametrize.html#indirect-parametrization
    scenario = request.param
    is_hiveserde = scenario == "hiveserde"
    random = make_random(5).lower()
    # create external and managed tables to be migrated
    if is_hiveserde:
        schema = installation_ctx.make_schema(catalog_name="hive_metastore", name=f"hiveserde_in_place_{random}")
        table_base_dir = make_storage_dir(
            path=f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/hiveserde_in_place_{random}'
        )
        tables = prepare_hiveserde_tables(installation_ctx, random, schema, table_base_dir)
    else:
        schema = installation_ctx.make_schema(catalog_name="hive_metastore", name=f"migrate_{random}")
        tables = prepare_regular_tables(installation_ctx, make_mounted_location, schema)

    # create destination catalog and schema
    dst_catalog = make_catalog()
    dst_schema = installation_ctx.make_schema(catalog_name=dst_catalog.name, name=schema.name)
    migrate_rules = [Rule.from_src_dst(table, dst_schema) for _, table in tables.items()]
    installation_ctx.with_table_mapping_rules(migrate_rules)
    installation_ctx.with_dummy_resource_permission()
    installation_ctx.save_tables(is_hiveserde=is_hiveserde)
    installation_ctx.save_mounts()
    installation_ctx.with_dummy_grants_and_tacls()
    return tables, dst_schema


@pytest.fixture
def prepared_principal_acl(runtime_ctx, make_mounted_location, make_catalog, make_schema):
    src_schema = make_schema(catalog_name="hive_metastore")
    src_external_table = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        external_csv=make_mounted_location,
    )
    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)
    rules = [Rule.from_src_dst(src_external_table, dst_schema)]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    return (
        runtime_ctx,
        f"{dst_catalog.name}.{dst_schema.name}.{src_external_table.name}",
        f"{dst_catalog.name}.{dst_schema.name}",
        dst_catalog.name,
    )


def modified_or_skip(package: str):
    info = ProductInfo.from_class(WorkspaceConfig)
    checkout_root = info.checkout_root()

    def _run(command: str) -> str:
        with subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=checkout_root,
        ) as process:
            output, error = process.communicate()
            if process.returncode != 0:
                pytest.fail(f"Command failed: {command}\n{error.decode('utf-8')}", pytrace=False)
            return output.decode("utf-8").strip()

    def check():
        if is_in_debug():
            return True  # not skipping, as we're debugging
        if 'TEST_NIGHTLY' in os.environ:
            return True  # or during nightly runs
        current_branch = _run("git branch --show-current")
        changed_files = _run(f"git diff origin/main..{current_branch} --name-only")
        if package in changed_files:
            return True
        return False

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not check():
                pytest.skip(f"Skipping long test as {package} was not modified")
            return func(*args, **kwargs)

        return wrapper

    return decorator


def pytest_ignore_collect(path):
    if not os.path.isdir(path):
        logger.debug(f"pytest_ignore_collect: not a dir: {path}")
        return False
    if is_in_debug():
        logger.debug(f"pytest_ignore_collect: in debug: {path}")
        return False  # not skipping, as we're debugging
    if 'TEST_NIGHTLY' in os.environ:
        logger.debug(f"pytest_ignore_collect: nightly: {path}")
        return False  # or during nightly runs

    checkout_root = ProductInfo.from_class(WorkspaceConfig).checkout_root()

    def _run(command: str) -> str:
        with subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=checkout_root,
        ) as process:
            output, error = process.communicate()
            if process.returncode != 0:
                raise ValueError(f"Command failed: {command}\n{error.decode('utf-8')}")
            return output.decode("utf-8").strip()

    try:  # pylint: disable=too-many-try-statements
        target_branch = os.environ.get('GITHUB_BASE_REF', 'main')
        current_branch = os.environ.get('GITHUB_HEAD_REF', _run("git branch --show-current"))
        changed_files = _run(f"git diff {target_branch}..{current_branch} --name-only")
        if path.basename in changed_files:
            logger.debug(f"pytest_ignore_collect: in changed files: {path} - {changed_files}")
            return False
        logger.debug(f"pytest_ignore_collect: skip: {path}")
        return True
    except ValueError as err:
        logger.debug(f"pytest_ignore_collect: error: {err}")
        return False
