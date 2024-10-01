import functools
import logging
import subprocess
from collections.abc import Iterable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User


logger = logging.getLogger(__name__)


def escape_sql_identifier(path: str, *, maxsplit: int = 2) -> str:
    """
    Escapes the path components to make them SQL safe.

    Args:
        path (str): The dot-separated path of a catalog object.
        maxsplit (int): The maximum number of splits to perform.

    Returns:
         str: The path with all parts escaped in backticks.
    """
    if not path:
        return path
    parts = path.split(".", maxsplit=maxsplit)
    escaped = [f"`{part.strip('`').replace('`', '``')}`" for part in parts]
    return ".".join(escaped)


def _has_role(user: User, role: str) -> bool:
    return user.roles is not None and any(r.value == role for r in user.roles)


def find_workspace_admins(ws: WorkspaceClient) -> Iterable[User]:
    """Enumerate the active workspace administrators in a given workspace.

    Arguments:
        ws (WorkspaceClient): The client for the workspace whose administrators should be enumerated.
    Returns:
        Iterable[User]: The active workspace administrators, if any.
    """
    all_users = ws.users.list(attributes="id,active,userName,roles")
    return (user for user in all_users if user.active and _has_role(user, "workspace_admin"))


def find_account_admins(ws: WorkspaceClient) -> Iterable[User]:
    """Enumerate the active account administrators associated with a given workspace.

    Arguments:
        ws (WorkspaceClient): The client for the workspace whose account administrators should be enumerated.
    Returns:
        Iterable[User]: The active account administrators, if any.
    """
    response = ws.api_client.do(
        "GET", "/api/2.0/account/scim/v2/Users", query={"attributes": "id,active,userName,roles"}
    )
    assert isinstance(response, dict)
    all_users = (User.from_dict(resource) for resource in response.get("Resources", []))
    return (user for user in all_users if user.active and _has_role(user, "account_admin"))


def find_an_admin(ws: WorkspaceClient) -> User | None:
    """Locate an active administrator for the current workspace.

    If an active workspace administrator can be located, this is returned. When there are multiple, they are sorted
    alphabetically by user-name and the first is returned. If there are no workspace administrators then an active
    account administrator is sought, again returning the first alphabetically by user-name if there is more than one.

    Arguments:
        ws (WorkspaceClient): The client for the workspace for which an administrator should be located.
    Returns:
        the first (alphabetically by user-name) active workspace or account administrator, or `None` if neither can be
        found.
    """
    first_user = functools.partial(min, default=None, key=lambda user: user.name)
    return first_user(find_workspace_admins(ws)) or first_user(find_account_admins(ws))


def run_command(command: str | list[str]) -> tuple[int, str, str]:
    args = command.split() if isinstance(command, str) else command
    logger.info(f"Invoking command: {args!r}")
    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        output, error = process.communicate()
        return process.returncode, output.decode("utf-8"), error.decode("utf-8")
