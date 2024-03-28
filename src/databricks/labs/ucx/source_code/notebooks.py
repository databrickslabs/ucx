from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectInfo

from databricks.labs.ucx.source_code.languages import Languages


class Notebooks:
    def __init__(self, ws: WorkspaceClient, languages: Languages):
        self._ws = ws
        self._languages = languages

    def revert(self, object_info: ObjectInfo):
        if not object_info.path:
            return False
        with self._ws.workspace.download(object_info.path + ".bak", format=ExportFormat.SOURCE) as f:
            code = f.read().decode("utf-8")
        self._ws.workspace.upload(object_info.path, code.encode("utf-8"))
        return True

    def apply(self, object_info: ObjectInfo) -> bool:
        if not object_info.language or not object_info.path:
            return False
        if not self._languages.is_supported(object_info.language):
            return False
        with self._ws.workspace.download(object_info.path, format=ExportFormat.SOURCE) as f:
            original_code = f.read().decode("utf-8")
            new_code = self._languages.apply_fixes(object_info.language, original_code)
            if new_code == original_code:
                return False
            self._ws.workspace.upload(object_info.path + ".bak", original_code.encode("utf-8"))
            self._ws.workspace.upload(object_info.path, new_code.encode("utf-8"))
            return True
