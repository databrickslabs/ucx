from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Query

from databricks.labs.ucx.code.base import Fixer


class Redash:
    def __init__(self, fixer: Fixer, ws: WorkspaceClient):
        self._fixer = fixer
        self._ws = ws

    def fix(self, query: Query):
        if self._fixer.match(query.query):
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
