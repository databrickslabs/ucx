from databricks.sdk import WorkspaceClient


class WorkflowLinter:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws
