from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectInfo

from databricks.labs.ucx.code.languages import Languages


class Notebooks:
    def __init__(self, ws: WorkspaceClient, languages: Languages):
        self._ws = ws
        self._languages = languages

    def revert(self, object_info: ObjectInfo):
        with self._ws.workspace.download(object_info.path + ".bak", format=ExportFormat.SOURCE) as f:
            code = f.read().decode("utf-8")
        with self._ws.workspace.upload(object_info.path, code.encode("utf-8")) as f:
            f.write(code)

    def apply(self, object_info: ObjectInfo):
        fixer = self._languages.fixer(object_info.language)
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            code = f.read().decode("utf-8")
        # create backup
        with self._ws.workspace.upload(object_info.path + ".bak", code.encode("utf-8")) as f:
            f.write(code)
        if fixer.match(code):
            code = fixer.apply(code)
            with self._ws.workspace.upload(object_info.path, code.encode("utf-8")) as f:
                f.write(code)
