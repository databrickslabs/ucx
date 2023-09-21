from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectInfo

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.mixins.workspace import workspace_list

__all__ = ["WorkspaceObjects"]


class WorkspaceObjects(CrawlerBase):
    def __init__(
        self,
        backend: SqlBackend,
        ws: WorkspaceClient,
        schema: str,
        *,
        start_path: str = "/",
        num_threads: int = 10,
        max_depth: int | None = None,
    ):
        super().__init__(backend, "hive_metastore", schema, "workspace")
        self._num_threads = num_threads
        self._start_path = start_path
        self._max_depth = max_depth
        self._ws = ws

    def snapshot(self) -> list[ObjectInfo]:
        return self._snapshot(self._loader, self._crawler)

    def _loader(self):
        for row in self._fetch(f"SELECT * FROM {self._full_name}"):
            yield ObjectInfo.from_dict(row.as_dict())

    def _crawler(self):
        yield from workspace_list(
            self._ws.workspace.list,
            self._start_path,
            yield_folders=True,
            threads=self._num_threads,
            max_depth=self._max_depth,
        )
