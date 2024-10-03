from collections.abc import Callable

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


def test_fallback_workspace_admin(ws) -> None:
    """Verify that a workspace administrator can be found for our integration environment."""
    ownership = _OwnershipFixture[str](ws)
    owner = ownership.owner_of("anything")

    assert owner
