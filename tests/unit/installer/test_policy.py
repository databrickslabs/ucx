import json

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.errors import NotFound
from databricks.sdk.service import iam
from databricks.sdk.service.compute import ClusterSpec, GetInstancePool, Policy
from databricks.sdk.service.jobs import Job, JobCluster, JobSettings
from databricks.sdk.service.sql import (
    EndpointConfPair,
    GetWorkspaceWarehouseConfigResponse,
)

from databricks.labs.ucx.installer.policy import ClusterPolicyInstaller
from tests.unit import workspace_client_mock


def common():
    w = workspace_client_mock(policy_ids=["ext-hms"])
    w.cluster_policies.create.return_value = Policy(
        policy_id="foo1",
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps({}),
    )

    w.clusters.select_spark_version = lambda **_: "14.2.x-scala2.12"
    w.clusters.select_node_type = lambda local_disk: "Standard_F4s"
    w.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    prompts = MockPrompts(
        {
            r".*We have identified one or more cluster.*": "Yes",
            r".*Choose a cluster policy.*": "0",
            r".*Instance pool id to be set in cluster policy.*": "",
        }
    )
    return w, prompts


def test_cluster_policy_definition_present_reuse():
    ws, prompts = common()
    policy = Policy(
        policy_id="foo1",
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps({}),
    )
    ws.cluster_policies.create.return_value = policy

    ws.cluster_policies.list.return_value = [policy]
    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    policy_id, _, _, _ = policy_installer.create('ucx')
    assert policy_id is not None
    ws.cluster_policies.create.assert_not_called()


def test_cluster_policy_definition_azure_hms():
    ws, prompts = common()
    ws.config.is_aws = False
    ws.config.is_azure = True
    ws.config.is_gcp = False

    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    policy_id, _, _, _ = policy_installer.create('ucx')
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"type": "fixed", "value": "url"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName": {"type": "fixed", "value": "user1"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword": {"type": "fixed", "value": "pwd"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName": {"type": "fixed", "value": "SQLServerDriver"},
        "spark_conf.spark.sql.hive.metastore.version": {"type": "fixed", "value": "0.13"},
        "spark_conf.spark.sql.hive.metastore.jars": {"type": "fixed", "value": "jar1"},
        "azure_attributes.availability": {"type": "fixed", "value": "ON_DEMAND_AZURE"},
    }
    assert policy_id == "foo1"

    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_cluster_policy_definition_aws_glue():
    ws, prompts = common()
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False
    policy_definition = {
        "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {"type": "fixed", "value": "true"},
        "aws_attributes.instance_profile_arn": {"value": "role_arn_1"},
    }

    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="id1",
            name="foo",
            definition=json.dumps(policy_definition),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]

    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    policy_id, instance_profile, _, _ = policy_installer.create('ucx')
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {"type": "fixed", "value": "true"},
        "aws_attributes.instance_profile_arn": {"type": "fixed", "value": "role_arn_1"},
        "aws_attributes.availability": {"type": "fixed", "value": "ON_DEMAND"},
    }
    assert policy_id == "foo1"
    assert instance_profile == "role_arn_1"
    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_cluster_policy_definition_gcp():
    ws, prompts = common()
    ws.config.is_aws = False
    ws.config.is_azure = False
    ws.config.is_gcp = True

    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    policy_id, instance_profile, _, _ = policy_installer.create('ucx')
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"type": "fixed", "value": "url"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName": {"type": "fixed", "value": "user1"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword": {"type": "fixed", "value": "pwd"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName": {"type": "fixed", "value": "SQLServerDriver"},
        "spark_conf.spark.sql.hive.metastore.version": {"type": "fixed", "value": "0.13"},
        "spark_conf.spark.sql.hive.metastore.jars": {"type": "fixed", "value": "jar1"},
        "gcp_attributes.availability": {"type": "fixed", "value": "ON_DEMAND_GCP"},
    }
    assert policy_id == "foo1"
    assert instance_profile is None
    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_update_job_policy_no_states():
    ws, prompts = common()
    installation = MockInstallation({'state.json': {'resources': {'dashboards': {'assessment_main': 'abc'}}}})
    policy_installer = ClusterPolicyInstaller(installation, ws, prompts)
    states = InstallState.from_installation(installation)
    policy_installer.update_job_policy(states, 'foo')
    ws.jobs.update.assert_not_called()


def test_update_job_policy_job_not_found():
    ws, prompts = common()
    installation = MockInstallation(
        {'state.json': {'resources': {'jobs': {"assessment": "123"}, 'dashboards': {'assessment_main': 'abc'}}}}
    )
    policy_installer = ClusterPolicyInstaller(installation, ws, prompts)
    states = InstallState.from_installation(installation)
    ws.jobs.get.side_effect = NotFound()
    policy_installer.update_job_policy(states, 'foo')
    ws.jobs.update.assert_not_called()


def test_update_job_policy():
    ws, prompts = common()
    installation = MockInstallation(
        {'state.json': {'resources': {'jobs': {"assessment": "123"}, 'dashboards': {'assessment_main': 'abc'}}}}
    )
    policy_installer = ClusterPolicyInstaller(installation, ws, prompts)
    states = InstallState.from_installation(installation)
    job_setting = JobSettings(
        job_clusters=[
            JobCluster(
                '123',
                new_cluster=ClusterSpec(
                    num_workers=1,
                    node_type_id=ws.clusters.select_node_type(local_disk=True),
                    spark_version=ws.clusters.select_spark_version(latest=True),
                ),
            )
        ]
    )
    job = Job(job_id=123, settings=job_setting)
    ws.jobs.get.return_value = job
    policy_installer.update_job_policy(states, 'foobar')
    ws.jobs.update.assert_called_with(123, new_settings=job_setting)


def test_cluster_policy_definition_azure_hms_warehouse():
    ws, _ = common()
    ws.config.is_aws = False
    ws.config.is_azure = True
    ws.config.is_gcp = False
    endpoint_conf = [
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionURL", "url"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionUserName", "user1"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionPassword", "pwd"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionDriverName", "SQLServerDriver"),
        EndpointConfPair("spark.sql.hive.metastore.version", "0.13"),
        EndpointConfPair("spark.sql.hive.metastore.jars", "jar1"),
    ]

    ws.warehouses.get_workspace_warehouse_config.return_value = GetWorkspaceWarehouseConfigResponse(
        data_access_config=endpoint_conf
    )

    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="id1",
            name="foo",
            definition=json.dumps({}),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]

    prompts = MockPrompts(
        {
            r".*We have identified one or more cluster.*": "No",
            r".*We have identified the workspace warehouse.*": "Yes",
            r".*Instance pool id to be set in cluster policy.*": "",
        }
    )
    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    policy_id, _, _, _ = policy_installer.create('ucx')
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"type": "fixed", "value": "url"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName": {"type": "fixed", "value": "user1"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword": {"type": "fixed", "value": "pwd"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName": {"type": "fixed", "value": "SQLServerDriver"},
        "spark_conf.spark.sql.hive.metastore.version": {"type": "fixed", "value": "0.13"},
        "spark_conf.spark.sql.hive.metastore.jars": {"type": "fixed", "value": "jar1"},
        "azure_attributes.availability": {"type": "fixed", "value": "ON_DEMAND_AZURE"},
    }
    assert policy_id == "foo1"

    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_cluster_policy_definition_aws_glue_warehouse():
    ws, _ = common()
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False
    endpoint_conf = [
        EndpointConfPair("spark.databricks.hive.metastore.glueCatalog.enabled", "true"),
    ]

    ws.warehouses.get_workspace_warehouse_config.return_value = GetWorkspaceWarehouseConfigResponse(
        data_access_config=endpoint_conf,
        instance_profile_arn="role_arn_1",
    )

    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="id1",
            name="foo",
            definition=json.dumps({}),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]

    prompts = MockPrompts(
        {
            r".*We have identified one or more cluster.*": "No",
            r".*We have identified the workspace warehouse.*": "Yes",
            r".*Instance pool id to be set in cluster policy.*": "",
        }
    )
    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    policy_id, instance_profile, _, _ = policy_installer.create('ucx')
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {"type": "fixed", "value": "true"},
        "aws_attributes.instance_profile_arn": {"type": "fixed", "value": "role_arn_1"},
        "aws_attributes.availability": {"type": "fixed", "value": "ON_DEMAND"},
    }
    assert policy_id == "foo1"
    assert instance_profile == "role_arn_1"

    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_cluster_policy_definition_gcp_hms_warehouse():
    ws, _ = common()
    ws.config.is_aws = False
    ws.config.is_azure = False
    ws.config.is_gcp = True
    endpoint_conf = [
        EndpointConfPair(None, None),
        EndpointConfPair("random", None),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionURL", "url"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionUserName", "user1"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionPassword", "pwd"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionDriverName", "SQLServerDriver"),
        EndpointConfPair("spark.sql.hive.metastore.version", "0.13"),
        EndpointConfPair("spark.sql.hive.metastore.jars", "jar1"),
    ]

    ws.warehouses.get_workspace_warehouse_config.return_value = GetWorkspaceWarehouseConfigResponse(
        data_access_config=endpoint_conf
    )

    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="id1",
            name="foo",
            definition=json.dumps({}),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]

    prompts = MockPrompts(
        {
            r".*We have identified one or more cluster.*": "No",
            r".*We have identified the workspace warehouse.*": "Yes",
            r".*Instance pool id to be set in cluster policy.*": "",
        }
    )
    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    policy_id, _, _, _ = policy_installer.create('ucx')
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"type": "fixed", "value": "url"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionUserName": {"type": "fixed", "value": "user1"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionPassword": {"type": "fixed", "value": "pwd"},
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionDriverName": {"type": "fixed", "value": "SQLServerDriver"},
        "spark_conf.spark.sql.hive.metastore.version": {"type": "fixed", "value": "0.13"},
        "spark_conf.spark.sql.hive.metastore.jars": {"type": "fixed", "value": "jar1"},
        "gcp_attributes.availability": {"type": "fixed", "value": "ON_DEMAND_GCP"},
    }
    assert policy_id == "foo1"

    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_cluster_policy_definition_empty_config():
    ws, prompts = common()
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False

    ws.warehouses.get_workspace_warehouse_config.return_value = GetWorkspaceWarehouseConfigResponse(
        data_access_config=None
    )

    ws.cluster_policies.list.return_value = [
        Policy(
            policy_id="id1",
            name="foo",
            definition=json.dumps({}),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        )
    ]

    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    policy_id, _, _, _ = policy_installer.create('ucx')
    policy_definition_actual = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "aws_attributes.availability": {"type": "fixed", "value": "ON_DEMAND"},
    }
    assert policy_id == "foo1"

    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_definition_actual),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_cluster_policy_instance_pool():
    ws, prompts = common()
    prompts = prompts.extend({r".*Instance pool id to be set in cluster policy.*": "instance_pool_1"})

    ws.instance_pools.get.return_value = GetInstancePool("instance_pool_1")
    ws.cluster_policies.list.return_value = []
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False

    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    _, _, _, instance_pool_id = policy_installer.create('ucx')

    assert instance_pool_id == "instance_pool_1"

    policy_expected = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "instance_pool_id": {"type": "fixed", "value": "instance_pool_1"},
    }
    # test the instance pool is added to the cluster policy
    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_expected),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )

    # test the instance pool is not found
    ws.instance_pools.get.side_effect = NotFound()
    policy_expected = {
        "spark_version": {"type": "fixed", "value": "14.2.x-scala2.12"},
        "node_type_id": {"type": "fixed", "value": "Standard_F4s"},
        "aws_attributes.availability": {"type": "fixed", "value": "ON_DEMAND"},
    }
    _, _, _, instance_pool_id = policy_installer.create('ucx')
    assert instance_pool_id is None
    ws.cluster_policies.create.assert_called_with(
        name="Unity Catalog Migration (ucx) (me@example.com)",
        definition=json.dumps(policy_expected),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    )


def test_has_ext_hms():
    ws, prompts = common()
    ws.config.is_aws = False
    ws.config.is_azure = True
    ws.config.is_gcp = False

    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    assert policy_installer.has_ext_hms() is True

    ws.cluster_policies.list.return_value = []
    endpoint_conf = [
        EndpointConfPair(None, None),
        EndpointConfPair("random", None),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionURL", "url"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionUserName", "user1"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionPassword", "pwd"),
        EndpointConfPair("spark.hadoop.javax.jdo.option.ConnectionDriverName", "SQLServerDriver"),
        EndpointConfPair("spark.sql.hive.metastore.version", "0.13"),
        EndpointConfPair("spark.sql.hive.metastore.jars", "jar1"),
    ]

    ws.warehouses.get_workspace_warehouse_config.return_value = GetWorkspaceWarehouseConfigResponse(
        data_access_config=endpoint_conf
    )
    policy_installer = ClusterPolicyInstaller(MockInstallation(), ws, prompts)
    assert policy_installer.has_ext_hms() is True
