from databricks.labs.ucx.framework.owners import Ownership, Record


class _OwnershipFixture(Ownership[Record]):
    def _get_owner(self, record: Record) -> str | None:
        return None


def test_fallback_workspace_admin(installation_ctx, ws) -> None:
    """Verify that a workspace administrator can be found for our integration environment."""
    ownership = _OwnershipFixture[str](ws, installation_ctx.administrator_locator)
    owner = ownership.owner_of("anything")

    assert owner
