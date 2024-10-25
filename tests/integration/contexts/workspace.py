from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from tests.integration.contexts.common import IntegrationContext


class MockWorkspaceContext(IntegrationContext, WorkspaceContext):
    def __init__(self, workspace_client, *args):
        super().__init__(workspace_client, *args)
        WorkspaceContext.__init__(self, workspace_client)
