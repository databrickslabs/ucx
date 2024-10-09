from databricks.labs.ucx.contexts.workflow_task import RuntimeContext


def test_fallback_workspace_admin(installation_ctx: RuntimeContext) -> None:
    """Verify that a workspace administrator can be found for our integration environment."""
    an_admin = installation_ctx.administrator_locator.get_workspace_administrator()

    assert "@" in an_admin
