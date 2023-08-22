from databricks.sdk import WorkspaceClient


class AssessmentToolkit:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def generate_report(self):
        catalogs = self._ws.catalogs.list()