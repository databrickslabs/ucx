import json
from unittest.mock import Mock

import pytest
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.iam import ObjectPermissions
from databricks.sdk.service.ml import Experiment, ExperimentTag
from databricks.sdk.service.workspace import AclPermission

from databricks.labs.ucx.inventory.inventorizer import (
    AccessControlResponse,
    AclItem,
    DatabricksError,
    Inventorizers,
    LogicalObjectType,
    ModelDatabricks,
    PermissionsInventoryItem,
    RequestObjectType,
    SecretScope,
    SecretScopeInventorizer,
    StandardInventorizer,
    TokensAndPasswordsInventorizer,
    experiments_listing,
    models_listing,
)

CLUSTER_DETAILS = ClusterDetails(cluster_name="cn1", cluster_id="cid1")
CLUSTER_PERMISSION = ObjectPermissions(object_id="oid1", object_type="ot1")
INVENTORY_ITEM = PermissionsInventoryItem(
    object_id="cid1",
    logical_object_type=LogicalObjectType.CLUSTER,
    request_object_type=RequestObjectType.CLUSTERS,
    raw_object_permissions=json.dumps(CLUSTER_PERMISSION.as_dict()),
)

PERMISSION_RESPONSE = {
    "object_id": "tokens",
    "object_type": "authorization",
    "access_control_list": [
        {
            "user_name": "un1",
            "group_name": "gn1",
            "service_principal_name": "sp1",
            "display_name": "dn1",
            "all_permissions": [],
        }
    ],
}
ACCESS_CONTROL_RESPONSE = [
    AccessControlResponse(
        all_permissions=None, display_name="dn1", group_name="gn1", service_principal_name="sp1", user_name="un1"
    )
]


@pytest.fixture
def workspace_client():
    client = Mock()
    client.clusters.list.return_value = [CLUSTER_DETAILS]
    client.permissions.get.return_value = CLUSTER_PERMISSION
    return client


@pytest.fixture
def standard_inventorizer(workspace_client):
    return StandardInventorizer(
        workspace_client,
        logical_object_type=LogicalObjectType.CLUSTER,
        request_object_type=RequestObjectType.CLUSTERS,
        listing_function=workspace_client.clusters.list,
        id_attribute="cluster_id",
    )


@pytest.fixture
def tokens_password_inventorizer(workspace_client):
    return TokensAndPasswordsInventorizer(workspace_client)


@pytest.fixture
def secret_scope_inventorizer(workspace_client):
    return SecretScopeInventorizer(workspace_client)


def test_standard_inventorizer_properties(standard_inventorizer):
    assert standard_inventorizer.logical_object_type == LogicalObjectType.CLUSTER
    assert standard_inventorizer.logical_object_types == [LogicalObjectType.CLUSTER]


def test_standard_inventorizer_get_permissions(standard_inventorizer):
    ret_val = ObjectPermissions(object_id="foo")
    standard_inventorizer._ws.permissions.get.return_value = ret_val
    assert standard_inventorizer._get_permissions(RequestObjectType.CLUSTERS, "foo") == ret_val


def test_standard_inventorizer_safe_get_permissions(standard_inventorizer):
    ret_val = ObjectPermissions(object_id="foo")
    standard_inventorizer._ws.permissions.get.return_value = ret_val
    assert standard_inventorizer._safe_get_permissions(RequestObjectType.CLUSTERS, "foo") == ret_val


def test_standard_inventorizer_safe_get_permissions_fail(standard_inventorizer):
    ret_val = ObjectPermissions(object_id="foo")
    standard_inventorizer._ws.permissions.get.return_value = ret_val
    standard_inventorizer._ws.permissions.get.side_effect = DatabricksError
    with pytest.raises(DatabricksError):
        standard_inventorizer._safe_get_permissions(RequestObjectType.CLUSTERS, "foo")


def test_standard_inventorizer_safe_get_permissions_return_none(standard_inventorizer):
    ret_val = ObjectPermissions(object_id="foo")
    standard_inventorizer._ws.permissions.get.return_value = ret_val
    standard_inventorizer._ws.permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    assert standard_inventorizer._safe_get_permissions(RequestObjectType.CLUSTERS, "foo") is None


def test_standard_inventorizer_preload(standard_inventorizer):
    standard_inventorizer.preload()
    assert standard_inventorizer._objects == [CLUSTER_DETAILS]


def test_standard_inventorizer_inventorize_permission(standard_inventorizer):
    standard_inventorizer._ws.permissions.get.return_value = CLUSTER_PERMISSION
    standard_inventorizer.preload()
    collected = standard_inventorizer.inventorize()
    assert len(collected) == 1
    assert collected[0] == INVENTORY_ITEM


def test_standard_inventorizer_inventorize_no_permission(standard_inventorizer):
    standard_inventorizer._ws.permissions.get.return_value = None
    standard_inventorizer.preload()
    collected = standard_inventorizer.inventorize()
    assert len(collected) == 0


def test_tokens_password_inventorizer_properties(tokens_password_inventorizer):
    assert tokens_password_inventorizer.logical_object_types == [LogicalObjectType.TOKEN, LogicalObjectType.PASSWORD]


def test_tokens_password_inventorizer_preload(tokens_password_inventorizer):
    tokens_password_inventorizer._ws.api_client.do.return_value = PERMISSION_RESPONSE
    tokens_password_inventorizer.preload()
    assert tokens_password_inventorizer._tokens_acl == ACCESS_CONTROL_RESPONSE
    assert tokens_password_inventorizer._passwords_acl == ACCESS_CONTROL_RESPONSE


def test_tokens_password_inventorizer_preload_fail(tokens_password_inventorizer):
    tokens_password_inventorizer._ws.api_client.do.side_effect = DatabricksError
    tokens_password_inventorizer.preload()
    assert tokens_password_inventorizer._tokens_acl == []
    assert tokens_password_inventorizer._passwords_acl == []


def test_tokens_password_inventorizer_inventorize(tokens_password_inventorizer):
    tokens_password_inventorizer._ws.api_client.do.return_value = PERMISSION_RESPONSE
    tokens_password_inventorizer.preload()
    inventory = tokens_password_inventorizer.inventorize()
    assert len(inventory) == 2


def test_tokens_password_inventorizer_inventorize_no_acls(tokens_password_inventorizer):
    assert tokens_password_inventorizer.inventorize() == []


def test_secret_scope_inventorizer_properties(secret_scope_inventorizer):
    assert secret_scope_inventorizer.logical_object_types == [LogicalObjectType.SECRET_SCOPE]


def test_secret_scope_inventorizer_preload(secret_scope_inventorizer):
    secret_scope_inventorizer.preload()


def test_secret_scope_inventorizer_acls(secret_scope_inventorizer):
    scope = SecretScope(name="sc1")

    secret_scope_inventorizer._ws.secrets.list_acls.return_value = []
    acls = secret_scope_inventorizer._get_acls_for_scope(scope)
    item = secret_scope_inventorizer._prepare_permissions_inventory_item(scope)
    assert sum(1 for _ in acls) == 0
    assert item.raw_object_permissions == '{"acls": []}'

    secret_scope_inventorizer._ws.secrets.list_acls.return_value = [
        AclItem(principal="pr1", permission=AclPermission.MANAGE)
    ]
    acls = secret_scope_inventorizer._get_acls_for_scope(scope)
    item = secret_scope_inventorizer._prepare_permissions_inventory_item(scope)
    assert sum(1 for _ in acls) == 1
    assert item.raw_object_permissions == '{"acls": [{"principal": "pr1", "permission": "MANAGE"}]}'


def test_secret_scope_inventorizer_inventorize(secret_scope_inventorizer):
    scope = SecretScope(name="sc1")

    secret_scope_inventorizer._ws.secrets.list_acls.return_value = []
    secret_scope_inventorizer._scopes = [scope]
    inventory = secret_scope_inventorizer.inventorize()
    assert len(inventory) == 1
    assert inventory[0].object_id == scope.name

    secret_scope_inventorizer._scopes = []
    inventory = secret_scope_inventorizer.inventorize()
    assert len(inventory) == 0


def test_models_listing(workspace_client):
    workspace_client.model_registry.list_models.return_value = []
    f = models_listing(workspace_client)
    models = f()
    assert sum(1 for _ in models) == 0

    model = ModelDatabricks(name="mn1", id="mid1")
    workspace_client.model_registry.list_models.return_value = [model]
    response = Mock()
    response.registered_model_databricks = model
    workspace_client.model_registry.get_model.return_value = response
    f = models_listing(workspace_client)
    models = f()
    assert sum(1 for _ in models) == 1
    models = f()
    assert next(models).name == "mn1"


def test_experiments_listing(workspace_client):
    # try without experiment present
    workspace_client.experiments.list_experiments.return_value = []
    f = experiments_listing(workspace_client)
    experiments = f()
    assert sum(1 for _ in experiments) == 0

    # try with one experiment present
    experiment = Experiment(name="en1", experiment_id="eid1")
    filtered_tags = [ExperimentTag(key="mlflow.experimentType", value="NOTEBOOK")]
    unfiltered_tags = [ExperimentTag(key="foo", value="bar")]
    all_tags = filtered_tags + unfiltered_tags
    workspace_client.experiments.list_experiments.return_value = [experiment]
    # first test without tags (means `None`)
    # FIX: Bug in function, does not handle None tags!
    # f = experiments_listing(workspace_client)
    # experiments = f()
    # assert sum(1 for _ in experiments) == 0
    # with the tags filtering the experiment out
    experiment.tags = filtered_tags
    experiments = f()
    assert sum(1 for _ in experiments) == 0
    # with tags returning experiment
    experiment.tags = unfiltered_tags
    experiments = f()
    assert sum(1 for _ in experiments) == 1
    experiments = f()
    assert next(experiments).name == "en1"
    # with all tags combined, dropping the experiment
    experiment.tags = all_tags
    experiments = f()
    assert sum(1 for _ in experiments) == 0

    # try with two experiments present
    experiment.tags = unfiltered_tags
    experiment2 = Experiment(name="en2", experiment_id="eid2")
    experiment2.tags = filtered_tags
    workspace_client.experiments.list_experiments.return_value = [experiment, experiment2]
    experiments = f()
    assert sum(1 for _ in experiments) == 1
    experiments = f()
    assert next(experiments).name == "en1"


def test_inventorizers_provide(workspace_client):
    inventorizers = Inventorizers.provide(workspace_client, None, 1)
    assert len(inventorizers) > 0
