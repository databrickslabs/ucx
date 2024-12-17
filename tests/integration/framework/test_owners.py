import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.owners import AdministratorLocator, WorkspacePathOwnership


def _find_admins_group_id(ws: WorkspaceClient) -> str:
    for group in ws.groups.list(attributes="id,displayName,meta", filter='displayName eq "admins"'):
        if group.id and group.display_name == "admins" and group.meta and group.meta.resource_type == "WorkspaceGroup":
            return group.id
    msg = f"Could not locate workspace group in {ws.get_workspace_id()}: admins"
    raise RuntimeError(msg)


def _find_user_with_name(ws: WorkspaceClient, user_name: str) -> iam.User:
    for user in ws.users.list(attributes="active,groups,roles,userName", filter=f"userName eq {json.dumps(user_name)}"):
        if user.user_name == user_name:
            return user
    # Use debugger if this is not working to avoid internal usernames in public issues or CI logs.
    msg = f"Could not locate user in workspace {ws.get_workspace_id()}: **REDACTED**"
    raise RuntimeError(msg)


def _user_is_member_of_group(user: iam.User, group_id: str) -> bool:
    assert user.groups
    return any(g for g in user.groups if g.value == group_id)


def _user_has_role(user: iam.User, role_name: str) -> bool:
    assert user.roles
    return any(r for r in user.roles if r.value == role_name)


def test_fallback_admin_user(ws, installation_ctx: RuntimeContext) -> None:
    """Verify that an administrator can be found for our integration environment."""
    an_admin = installation_ctx.administrator_locator.get_workspace_administrator()

    # The specific admin username that we get here depends on the set of current admins in the integration environment,
    # so that can't be checked directly. Instead we check that either:
    #   a) they're a member of the 'admins' workspace; or
    #   b) are an account admin (with the `account_admin` role assigned).
    # They must also be an active user.
    #
    # References:
    #   https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/groups#account-admin
    #   https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/groups#account-vs-workspace-group
    admins_group_id = _find_admins_group_id(ws)
    the_user = _find_user_with_name(ws, an_admin)

    assert an_admin == the_user.user_name and the_user.active
    assert _user_is_member_of_group(the_user, admins_group_id) or _user_has_role(the_user, "account_admin")


def test_notebook_owner(make_notebook, make_notebook_permissions, make_group, ws):
    notebook = make_notebook()
    new_group = make_group()
    make_notebook_permissions(
        object_id=notebook,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=new_group.display_name,
    )

    admin_locator = AdministratorLocator(ws)
    notebook_ownership = WorkspacePathOwnership(admin_locator, ws)

    name = notebook_ownership.owner_of(notebook)

    my_user = ws.current_user.me()
    assert name == my_user.user_name


def test_file_owner(make_workspace_file, ws):
    ws_file = make_workspace_file()

    admin_locator = AdministratorLocator(ws)
    notebook_ownership = WorkspacePathOwnership(admin_locator, ws)

    name = notebook_ownership.owner_of(ws_file)

    my_user = ws.current_user.me()
    assert name == my_user.user_name


def test_home_directory_owner_is_me(runtime_ctx) -> None:
    current_user = runtime_ctx.workspace_client.current_user.me()
    home = f"/Users/{current_user.user_name}"

    name = runtime_ctx.workspace_path_ownership.owner_of_path(home)

    assert name == current_user.user_name


def test_workspace_path_owner_of_invalid_path(runtime_ctx) -> None:
    owner = runtime_ctx.workspace_path_ownership.owner_of_path("invalid/path")
    assert owner is None
