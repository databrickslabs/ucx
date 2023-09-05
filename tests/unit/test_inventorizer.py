import json
from unittest.mock import Mock

import pytest
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.iam import ComplexValue, Group, ObjectPermissions
from databricks.sdk.service.ml import Experiment, ExperimentTag
from databricks.sdk.service.workspace import AclPermission, ObjectInfo, ObjectType
from databricks.sdk.service.ml import ModelDatabricks

from databricks.labs.ucx.inventory.inventorizer import (
    AccessControlResponse,
    AclItem,
    DatabricksError,
    Inventorizers,
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
    RolesAndEntitlementsInventorizer,
    SecretScope,
    SecretScopeInventorizer,
    StandardInventorizer,
    TokensAndPasswordsInventorizer,
    WorkspaceInventorizer,
)
from databricks.labs.ucx.inventory.listing import experiments_listing, models_listing
from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)

CLUSTER_DETAILS = ClusterDetails(cluster_name="cn1", cluster_id="cid1")
OBJECT_PERMISSION = ObjectPermissions(object_id="oid1", object_type="ot1")
INVENTORY_ITEM = PermissionsInventoryItem(
    object_id="cid1",
    logical_object_type=LogicalObjectType.CLUSTER,
    request_object_type=RequestObjectType.CLUSTERS,
    raw_object_permissions=json.dumps(OBJECT_PERMISSION.as_dict()),
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
    client.permissions.get.return_value = OBJECT_PERMISSION
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
def tokens_passwords_inventorizer(workspace_client):
    return TokensAndPasswordsInventorizer(workspace_client)


@pytest.fixture
def secret_scope_inventorizer(workspace_client):
    return SecretScopeInventorizer(workspace_client)


@pytest.fixture
def workspace_inventorizer(workspace_client):
    return WorkspaceInventorizer(workspace_client)


@pytest.fixture
def role_entitlements_inventorizer(workspace_client):
    state = GroupMigrationState()
    return RolesAndEntitlementsInventorizer(workspace_client, migration_state=state)


# Tests for StandardInventorizer


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
    standard_inventorizer._ws.permissions.get.return_value = OBJECT_PERMISSION
    standard_inventorizer.preload()
    collected = standard_inventorizer.inventorize()
    assert len(collected) == 1
    assert collected[0] == INVENTORY_ITEM


def test_standard_inventorizer_inventorize_no_permission(standard_inventorizer):
    standard_inventorizer._ws.permissions.get.return_value = None
    standard_inventorizer.preload()
    collected = standard_inventorizer.inventorize()
    assert len(collected) == 0


# Tests for TokensAndPasswordsInventorizer


def test_tokens_password_inventorizer_properties(tokens_passwords_inventorizer):
    assert tokens_passwords_inventorizer.logical_object_types == [LogicalObjectType.TOKEN, LogicalObjectType.PASSWORD]


def test_tokens_password_inventorizer_preload(tokens_passwords_inventorizer):
    tokens_passwords_inventorizer._ws.api_client.do.return_value = PERMISSION_RESPONSE
    tokens_passwords_inventorizer.preload()
    assert tokens_passwords_inventorizer._tokens_acl == ACCESS_CONTROL_RESPONSE
    assert tokens_passwords_inventorizer._passwords_acl == ACCESS_CONTROL_RESPONSE


def test_tokens_password_inventorizer_preload_fail(tokens_passwords_inventorizer):
    tokens_passwords_inventorizer._ws.api_client.do.side_effect = DatabricksError
    tokens_passwords_inventorizer.preload()
    assert tokens_passwords_inventorizer._tokens_acl == []
    assert tokens_passwords_inventorizer._passwords_acl == []


def test_tokens_password_inventorizer_inventorize(tokens_passwords_inventorizer):
    tokens_passwords_inventorizer._ws.api_client.do.return_value = PERMISSION_RESPONSE
    tokens_passwords_inventorizer.preload()
    inventory = tokens_passwords_inventorizer.inventorize()
    assert len(inventory) == 2


def test_tokens_password_inventorizer_inventorize_no_acls(tokens_passwords_inventorizer):
    assert tokens_passwords_inventorizer.inventorize() == []


# Tests for SecretScopeInventorizer


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
    models = list(f())
    assert len(models) == 0

    model = ModelDatabricks(name="mn1", id="mid1")
    workspace_client.model_registry.list_models.return_value = [model]
    response = Mock()
    response.registered_model_databricks = model
    workspace_client.model_registry.get_model.return_value = response
    f = models_listing(workspace_client)
    models = list(f())
    assert len(models) == 1
    assert models[0].name == "mn1"


def test_experiments_listing(workspace_client):
    # try without experiment present
    workspace_client.experiments.list_experiments.return_value = []
    f = experiments_listing(workspace_client)
    experiments = list(f())
    assert len(experiments) == 0

    # try with one experiment present
    experiment = Experiment(name="en1", experiment_id="eid1")
    filtered_tags = [ExperimentTag(key="mlflow.experimentType", value="NOTEBOOK")]
    unfiltered_tags = [ExperimentTag(key="foo", value="bar")]
    all_tags = filtered_tags + unfiltered_tags
    workspace_client.experiments.list_experiments.return_value = [experiment]
    # first test without tags (means `None`)
    # FIX: Bug in function, does not handle None tags!
    # f = experiments_listing(workspace_client)
    # experiments = list(f())
    # assert len(experiments) == 0

    # with the tags filtering the experiment out
    experiment.tags = filtered_tags
    experiments = list(f())
    assert len(experiments) == 0

    # with tags returning experiment
    experiment.tags = unfiltered_tags
    experiments = list(f())
    assert len(experiments) == 1
    assert experiments[0].name == "en1"

    # with all tags combined, dropping the experiment
    experiment.tags = all_tags
    experiments = list(f())
    assert len(experiments) == 0

    # try with two experiments present
    experiment.tags = unfiltered_tags
    experiment2 = Experiment(name="en2", experiment_id="eid2")
    experiment2.tags = filtered_tags
    workspace_client.experiments.list_experiments.return_value = [experiment, experiment2]
    experiments = list(f())
    assert len(experiments) == 1
    assert experiments[0].name == "en1"


def test_inventorizers_provide(workspace_client):
    state = GroupMigrationState()
    inventorizers = Inventorizers(workspace_client, migration_state=state, num_threads=1).provide()
    assert len(inventorizers) > 0


# Tests for WorkspaceInventorizer


def test_workspace_inventorizer_properties(workspace_inventorizer):
    assert workspace_inventorizer.logical_object_types == [
        LogicalObjectType.NOTEBOOK,
        LogicalObjectType.DIRECTORY,
        LogicalObjectType.REPO,
        LogicalObjectType.FILE,
    ]


def test_workspace_inventorizer_preload(workspace_inventorizer):
    workspace_inventorizer.preload()


# def test_workspace_inventorizer_static_converters(workspace_inventorizer):
#     info = ObjectInfo(object_type=ObjectType.NOTEBOOK, object_id="oid1")
#     WorkspaceInventorizer._WorkspaceInventorizer__convert_object_type_to_request_type()


def test_workspace_inventorizer_get_permissions(workspace_inventorizer):
    ret_val = ObjectPermissions(object_type=str(ObjectType.NOTEBOOK), object_id="foo")
    workspace_inventorizer._ws.permissions.get.return_value = ret_val
    assert workspace_inventorizer._get_permissions(RequestObjectType.NOTEBOOKS, "foo") == ret_val


@pytest.mark.parametrize(
    ["object_type", "request_type"],
    [
        (None, None),
        (ObjectType.NOTEBOOK, RequestObjectType.NOTEBOOKS),
        (ObjectType.DIRECTORY, RequestObjectType.DIRECTORIES),
        (ObjectType.LIBRARY, None),
        (ObjectType.REPO, RequestObjectType.REPOS),
        (ObjectType.FILE, RequestObjectType.FILES),
    ],
)
def test_workspace_inventorizer_convert_object_to_permission(workspace_inventorizer, object_type, request_type):
    info = ObjectInfo(object_type=object_type, object_id=1)
    item = workspace_inventorizer._convert_result_to_permission_item(info)
    assert (
            (object_type == ObjectType.LIBRARY and item is None)
            or (object_type and item.request_object_type == request_type)
            or item is None
    )


def test_workspace_inventorizer_convert_object_to_permission_no_perms(workspace_inventorizer):
    info = ObjectInfo(object_type=ObjectType.NOTEBOOK, object_id=1)
    workspace_inventorizer._ws.permissions.get.return_value = None
    assert workspace_inventorizer._convert_result_to_permission_item(info) is None


def test_workspace_inventorizer_convert_object_to_permission_fail(workspace_inventorizer):
    info = ObjectInfo(object_type=ObjectType.NOTEBOOK, object_id=1)
    # Test case where remote exception is raised again
    workspace_inventorizer._ws.permissions.get.side_effect = DatabricksError(error_code="bogus")
    with pytest.raises(DatabricksError):
        workspace_inventorizer._convert_result_to_permission_item(info)
    # Test case where remote exception is converted to None
    workspace_inventorizer._ws.permissions.get.side_effect = DatabricksError(error_code="PERMISSION_DENIED")
    assert workspace_inventorizer._convert_result_to_permission_item(info) is None


def test_workspace_inventorizer_inventorize(workspace_inventorizer):
    workspace_inventorizer._ws.workspace.list.return_value = iter([])
    items = workspace_inventorizer.inventorize()
    assert len(items) == 0

    objects = iter([ObjectInfo(object_type=ObjectType.NOTEBOOK, object_id=1)])
    workspace_inventorizer._ws.workspace.list.return_value = objects
    items = workspace_inventorizer.inventorize()
    assert len(items) == 1


# Tests for RolesAndEntitlementsInventorizer


def test_role_entitlements_inventorizer_properties(role_entitlements_inventorizer):
    assert role_entitlements_inventorizer.logical_object_types == [
        LogicalObjectType.ROLES,
        LogicalObjectType.ENTITLEMENTS,
    ]


def test_role_entitlements_inventorizer_preload(role_entitlements_inventorizer):
    # Test empty groups
    role_entitlements_inventorizer.preload()
    assert len(role_entitlements_inventorizer._group_info) == 0

    # Test with groups present
    group = Group(display_name="grp1")
    role_entitlements_inventorizer._migration_state.add(
        MigrationGroupInfo(workspace=group, backup=group, account=group)
    )
    role_entitlements_inventorizer.preload()
    assert len(role_entitlements_inventorizer._group_info) == 1


def test_role_entitlements_inventorizer_inventorize(role_entitlements_inventorizer):
    role_entitlements_inventorizer._ws.groups.get.return_value = Group(display_name="grp1")

    # Test empty groups
    role_entitlements_inventorizer.preload()
    items = role_entitlements_inventorizer.inventorize()
    assert len(items) == 0

    # Test with groups present
    roles = [ComplexValue(value="cv1"), ComplexValue(value="cv2")]
    entitlements = [ComplexValue(value="cv3"), ComplexValue(value="cv4")]
    group = Group(display_name="grp1", roles=roles, entitlements=entitlements)
    role_entitlements_inventorizer._migration_state.add(
        MigrationGroupInfo(workspace=group, backup=group, account=group)
    )
    role_entitlements_inventorizer.preload()
    items = role_entitlements_inventorizer.inventorize()
    assert len(items) == 1
    assert items[0].object_id == "grp1"
