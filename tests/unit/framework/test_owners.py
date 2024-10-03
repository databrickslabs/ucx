from collections.abc import Callable

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.owners import Ownership, Record


class _OwnershipFixture(Ownership[Record]):
    def __init__(
        self,
        ws: WorkspaceClient,
        *,
        owner_fn: Callable[[Record], str | None] = lambda _: None,
    ):
        super().__init__(ws)
        self._owner_fn = owner_fn

    def _get_owner(self, record: Record) -> str | None:
        return self._owner_fn(record)


def test_ownership_prefers_record_owner(ws) -> None:
    """Verify that if an owner for the record can be found, that is used."""
    ownership = _OwnershipFixture[str](ws, owner_fn=lambda _: "bob")

    assert ownership.owner_of("school") == "bob"
    ws.get_workspace_id.assert_not_called()

def test_ownership_admin_user_fallback(ws) -> None:
    """Verify that if no owner for the record can be found, an admin user is returned instead."""
    ownership = _OwnershipFixture[str](ws)
    pytest.xfail("Not yet implemented")


def test_ownership_workspace_admin_preferred_over_account_admin(ws) -> None:
    """Verify that when both workspace and account administrators are configured, the workspace admin is preferred."""
    pytest.xfail("Not yet implemented")



def test_ownership_workspace_admin_prefer_first_alphabetically(ws) -> None:
    """Verify that when multiple workspace administrators can found, the first alphabetically preferred is used."""
    pytest.xfail("Not yet implemented")



def test_ownership_account_admin_prefer_first_alphabetically(ws) -> None:
    """Verify that when multiple account administrators can found, the first alphabetically preferred is used."""
    pytest.xfail("Not yet implemented")



def test_ownership_error_when_no_owner_can_be_located(ws) -> None:
    """Verify that an error is raised when no workspace or account administrators can be found."""
    pytest.xfail("Not yet implemented")



def test_ownership_fallback_instance_cache(ws) -> None:
    """Verify that the fallback owner is cached on each instance to avoid many REST calls."""
    pytest.xfail("Not yet implemented")



def test_ownership_fallback_class_cache(ws) -> None:
    """Verify that the fallback owner for a workspace is cached at class level to avoid many REST calls."""
    pytest.xfail("Not yet implemented")



def test_ownership_fallback_class_cache_multiple_workspaces(ws) -> None:
    """Verify that cache of workspace administrators supports multiple workspaces."""
    pytest.xfail("Not yet implemented")



def test_ownership_fallback_error_handling(ws) -> None:
    """Verify that the class-level owner-cache and tracks errors to avoid many REST calls."""
    pytest.xfail("Not yet implemented")
