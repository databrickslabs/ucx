import string
from typing import BinaryIO
import io
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service import pipelines, jobs, compute, iam
from databricks.sdk.service.catalog import SchemaInfo, TableInfo, TableType, DataSourceFormat
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType, AlertOptions, QueryInfo
import json
import logging
import random

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

logger = logging.getLogger(__name__)

#TODO: FIXTURES ARE TOO DANGEROUS BECAUSE iF THERE'S ANY FAILURE WE HAVE TO WAIL A FULL DAY TO RECREATE OBJECTS

def create_user(ws:WorkspaceClient):
    return ws.users.create(user_name=f"sdk-{make_random(4)}@example.com".lower())

def _scim_values(ids: list[str]) -> list[iam.ComplexValue]:
    return [iam.ComplexValue(value=x) for x in ids]


def create_group(ws:WorkspaceClient, acc:AccountClient, nb_of_users, entitlements):
    workspace_group_name = f"ucx_{make_random(4)}"
    account_group_name = workspace_group_name
    members = []
    for i in range(nb_of_users):
        user = create_user(ws)
        members.append(user.id)

    ws_group = create_ws_group(ws, display_name=workspace_group_name, members=members, entitlements=entitlements)
    acc_group = create_acc_group(acc, display_name=account_group_name, members=members)
    return ws_group, acc_group


def create_ws_group(ws, display_name, members, entitlements):
    return _make_group("workspace group", ws.config, ws.groups, members=members, entitlements=entitlements, display_name=display_name)


def create_acc_group(acc, display_name, members):
    return _make_group("account group", acc.config, acc.groups, members=members, display_name=display_name)


def _make_group(name, cfg, interface, members: list[str] | None = None,
    roles: list[str] | None = None,
    entitlements: list[str] | None = None,
    display_name: str | None = None, **kwargs):

    kwargs["display_name"] = f"sdk-{make_random(4)}" if display_name is None else display_name
    if members is not None:
        kwargs["members"] = _scim_values(members)
    if roles is not None:
        kwargs["roles"] = _scim_values(roles)
    if entitlements is not None:
        kwargs["entitlements"] = _scim_values(entitlements)
    # TODO: REQUEST_LIMIT_EXCEEDED: GetUserPermissionsRequest RPC token bucket limit has been exceeded.
    group = interface.create(**kwargs)
    if cfg.is_account_client:
        logger.info(f"Account group {group.display_name}: {cfg.host}/users/groups/{group.id}/members")
    else:
        logger.info(f"Workspace group {group.display_name}: {cfg.host}#setting/accounts/groups/{group.id}")
    return group


def make_random(k=16):
    charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
    return "".join(random.choices(charset, k=int(k)))


def create_scope(ws, **kwargs):
    name = f"sdk-{make_random(4)}"
    ws.secrets.create_scope(name, **kwargs)
    return name


def create_dlt(ws, **kwargs) -> pipelines.CreatePipelineResponse:
    if "name" not in kwargs:
        kwargs["name"] = f"sdk-{make_random(4)}"
    if "libraries" not in kwargs:
        kwargs["libraries"] = [pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=create_notebook(ws)))]
    if "clusters" not in kwargs:
        kwargs["clusters"] = [
            pipelines.PipelineCluster(
                node_type_id=ws.clusters.select_node_type(local_disk=True),
                label="default",
                num_workers=1,
                custom_tags={
                    "cluster_type": "default",
                },
            )
        ]
    return ws.pipelines.create(continuous=False, **kwargs)


def create_job(ws, **kwargs):
    task_spark_conf = None
    if "name" not in kwargs:
        kwargs["name"] = f"sdk-{make_random(4)}"
    if "spark_conf" in kwargs:
        task_spark_conf = kwargs["spark_conf"]
        kwargs.pop("spark_conf")
    if "tasks" not in kwargs:
        if task_spark_conf:
            kwargs["tasks"] = [
                jobs.Task(
                    task_key=make_random(4),
                    description=make_random(4),
                    new_cluster=compute.ClusterSpec(
                        num_workers=1,
                        node_type_id=ws.clusters.select_node_type(local_disk=True),
                        spark_version=ws.clusters.select_spark_version(latest=True),
                        spark_conf=task_spark_conf,
                    ),
                    notebook_task=jobs.NotebookTask(notebook_path=create_notebook(ws)),
                    timeout_seconds=0,
                )
            ]
        else:
            kwargs["tasks"] = [
                jobs.Task(
                    task_key=make_random(4),
                    description=make_random(4),
                    new_cluster=compute.ClusterSpec(
                        num_workers=1,
                        node_type_id=ws.clusters.select_node_type(local_disk=True),
                        spark_version=ws.clusters.select_spark_version(latest=True),
                    ),
                    notebook_task=jobs.NotebookTask(notebook_path=create_notebook(ws)),
                    timeout_seconds=0,
                )
            ]
    job = ws.jobs.create(**kwargs)
    logger.info(f"Job: {ws.config.host}#job/{job.job_id}")
    return job


def create_experiment(
    ws: WorkspaceClient,
    path: str | None = None,
    experiment_name: str | None = None,
    **kwargs,
):
    if path is None:
        path = f"/Users/{ws.current_user.me().user_name}/{make_random(4)}"
    if experiment_name is None:
        experiment_name = f"sdk-{make_random(4)}"

    try:
        ws.workspace.mkdirs(path)
    except DatabricksError:
        pass

    return ws.experiments.create_experiment(name=f"{path}/{experiment_name}", **kwargs)


def create_model(
    ws:WorkspaceClient,
    model_name: str | None = None,
    **kwargs,
):
    if model_name is None:
        model_name = f"sdk-{make_random(4)}"

    created_model = ws.model_registry.create_model(model_name, **kwargs)
    model = ws.model_registry.get_model(created_model.registered_model.name)
    return model.registered_model_databricks


def create_pool(ws:WorkspaceClient, instance_pool_name=None, node_type_id=None, **kwargs):
    if instance_pool_name is None:
        instance_pool_name = f"sdk-{make_random(4)}"
    if node_type_id is None:
        node_type_id = ws.clusters.select_node_type(local_disk=True)
    return ws.instance_pools.create(instance_pool_name, node_type_id, **kwargs)


def create_warehouse(
    ws:WorkspaceClient,
    warehouse_name: str | None = None,
    warehouse_type: CreateWarehouseRequestWarehouseType | None = None,
    cluster_size: str | None = None,
    max_num_clusters: int = 1,
    enable_serverless_compute: bool = False,
    **kwargs,
):
    if warehouse_name is None:
        warehouse_name = f"sdk-{make_random(4)}"
    if warehouse_type is None:
        warehouse_type = CreateWarehouseRequestWarehouseType.PRO
    if cluster_size is None:
        cluster_size = "2X-Small"

    return ws.warehouses.create(
        name=warehouse_name,
        cluster_size=cluster_size,
        warehouse_type=warehouse_type,
        max_num_clusters=max_num_clusters,
        enable_serverless_compute=enable_serverless_compute,
        **kwargs,
    )


def create_cluster(
    ws:WorkspaceClient,
    single_node: bool = False,
    cluster_name: str | None = None,
    spark_version: str | None = None,
    autotermination_minutes=10,
    **kwargs,
):
    if cluster_name is None:
        cluster_name = f"sdk-{make_random(4)}"
    if spark_version is None:
        spark_version = ws.clusters.select_spark_version(latest=True)
    if single_node:
        kwargs["num_workers"] = 0
        if "spark_conf" in kwargs:
            kwargs["spark_conf"] = kwargs["spark_conf"] | {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            }
        else:
            kwargs["spark_conf"] = {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"}
        kwargs["custom_tags"] = {"ResourceClass": "SingleNode"}
    if "instance_pool_id" not in kwargs:
        kwargs["node_type_id"] = ws.clusters.select_node_type(local_disk=True)

    return ws.clusters.create(
        cluster_name=cluster_name,
        spark_version=spark_version,
        autotermination_minutes=autotermination_minutes,
        **kwargs,
    )

def create_policy(ws:WorkspaceClient, name: str | None = None, **kwargs):
    if name is None:
        name = f"sdk-{make_random(4)}"
    if "definition" not in kwargs:
        kwargs["definition"] = json.dumps(
            {"spark_conf.spark.databricks.delta.preview.enabled": {"type": "fixed", "value": "true"}}
        )
    cluster_policy = ws.cluster_policies.create(name, **kwargs)
    logger.info(
        f"Cluster policy: {ws.config.host}#setting/clusters/cluster-policies/view/{cluster_policy.policy_id}"
    )
    return cluster_policy


def create_query(ws:WorkspaceClient, query_name: str | None = None, query: str | None = None) -> QueryInfo:
    if query_name is None:
        query_name = f"ucx_query_Q{make_random(4)}"

    dbsql_query = ws.queries.create(
        name=f"{query_name}",
        description="TEST QUERY FOR UCX",
        query="SELECT 1+1;",
    )
    logger.info(f"Query Created {query_name}: {ws.config.host}/sql/editor/{dbsql_query.id}")
    return dbsql_query


def create_alert(query_id, ws:WorkspaceClient, name: str | None = None):
    if name is None:
        name = f"ucx_T{make_random(4)}"
    return ws.alerts.create(options=AlertOptions(column="1", op="==", value="1"), name=name, query_id=query_id)


def create_dashboard(ws:WorkspaceClient, name: str | None = None):
    if name is None:
        name = f"ucx_T{make_random(4)}"
    return ws.dashboards.create(name=name)


def create_repo(ws:WorkspaceClient, url=None, provider=None, path=None, **kwargs):
    if path is None:
        path = f"/Repos/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
    if url is None:
        url = "https://github.com/shreyas-goenka/empty-repo.git"
    if provider is None:
        provider = "github"
    return ws.repos.create(url, provider, path=path, **kwargs)


def create_dir(ws:WorkspaceClient, path: str | None = None):
    if path is None:
        path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
    ws.workspace.mkdirs(path)
    return path


def create_notebook(ws:WorkspaceClient, path: str | None = None, content: BinaryIO | None = None, **kwargs):
    if path is None:
        path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}.py"
    if content is None:
        content = io.BytesIO(b"print(1)")
    ws.workspace.upload(path, content, **kwargs)
    return path


def create_schema(sql_backend, ws:WorkspaceClient, catalog_name: str = "hive_metastore", name: str | None = None) -> SchemaInfo:
    if name is None:
        name = f"ucx_S{make_random(4)}".lower()
    full_name = f"{catalog_name}.{name}".lower()
    sql_backend.execute(f"CREATE SCHEMA {full_name}")
    schema_info = SchemaInfo(catalog_name=catalog_name, name=name, full_name=full_name)
    logger.info(
        f"Schema {schema_info.full_name}: "
        f"{ws.config.host}/explore/data/{schema_info.catalog_name}/{schema_info.name}"
    )
    return schema_info

def create_table(
    sql_backend,
    ws:WorkspaceClient,
    catalog_name="hive_metastore",
    schema_name: str | None = None,
    name: str | None = None,
    ctas: str | None = None,
    non_delta: bool = False,
    external: bool = False,
    external_csv: str | None = None,
    view: bool = False,
    tbl_properties: dict[str, str] | None = None,
    function: bool = False,
) -> TableInfo:
    if schema_name is None:
        schema = create_schema(sql_backend, ws, catalog_name=catalog_name)
        catalog_name = schema.catalog_name
        schema_name = schema.name
    if name is None:
        name = f"ucx_T{make_random(4)}".lower()
    table_type = None
    data_source_format = None
    storage_location = None
    full_name = f"{catalog_name}.{schema_name}.{name}".lower()

    ddl = f'CREATE {"VIEW" if view else "TABLE"} {full_name}'
    if view:
        table_type = TableType.VIEW
    if ctas is not None:
        # temporary (if not view)
        ddl = f"{ddl} AS {ctas}"
    elif function:
        ddl = f"{ddl}() RETURNS INT NOT DETERMINISTIC CONTAINS SQL RETURN (rand() * 6)::INT + 1;"
    elif non_delta:
        table_type = TableType.MANAGED
        data_source_format = DataSourceFormat.JSON
        storage_location = "dbfs:/databricks-datasets/iot-stream/data-device"
        ddl = f"{ddl} USING json LOCATION '{storage_location}'"
    elif external_csv is not None:
        table_type = TableType.EXTERNAL
        data_source_format = DataSourceFormat.CSV
        storage_location = external_csv
        ddl = f"{ddl} USING CSV OPTIONS (header=true) LOCATION '{storage_location}'"
    elif external:
        # external table
        table_type = TableType.EXTERNAL
        data_source_format = DataSourceFormat.DELTASHARING
        url = "s3a://databricks-datasets-oregon/delta-sharing/share/open-datasets.share"
        storage_location = f"{url}#delta_sharing.default.lending_club"
        ddl = f"{ddl} USING deltaSharing LOCATION '{storage_location}'"
    else:
        # managed table
        table_type = TableType.MANAGED
        data_source_format = DataSourceFormat.DELTA
        storage_location = f"dbfs:/user/hive/warehouse/{schema_name}/{name}"
        ddl = f"{ddl} (id INT, value STRING)"
    if tbl_properties:
        tbl_properties = ",".join([f" '{k}' = '{v}' " for k, v in tbl_properties.items()])
        ddl = f"{ddl} TBLPROPERTIES ({tbl_properties})"

    sql_backend.execute(ddl)
    table_info = TableInfo(
        catalog_name=catalog_name,
        schema_name=schema_name,
        name=name,
        full_name=full_name,
        properties=tbl_properties,
        storage_location=storage_location,
        table_type=table_type,
        data_source_format=data_source_format,
    )
    logger.info(
        f"Table {table_info.full_name}: "
        f"{ws.config.host}/explore/data/{table_info.catalog_name}/{table_info.schema_name}/{table_info.name}"
    )
    return table_info