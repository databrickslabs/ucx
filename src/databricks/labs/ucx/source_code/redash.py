from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Query

from databricks.labs.ucx.source_code.base import Fixer


class Redash:
    def __init__(self, fixer: Fixer, ws: WorkspaceClient):
        self._fixer = fixer
        self._ws = ws

    def fix(self, query: Query):
        assert query.id is not None
        assert query.query is not None
        query.query = self._fixer.apply(query.query)
        self._ws.queries.update(
            query.id,
            data_source_id=query.data_source_id,
            description=query.description,
            name=query.name,
            options=query.options,
            query=query.query,
            run_as_role=query.run_as_role,
        )
