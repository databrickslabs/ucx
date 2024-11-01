from dataclasses import replace

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.owners import LegacyQueryOwnership
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.source_code.queries import QueryProblem


class QueryProblemProgressEncoder(ProgressEncoder[QueryProblem]):
    """Encoder class:QueryProblem to class:History.

    A query problem is by definition a failure as it contains problems found while linting code compatibility with Unity
    Catalog.
    """

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: LegacyQueryOwnership,
        run_id: int,
        workspace_id: int,
        catalog: str,
        schema: str = "multiworkspace",
        table: str = "historical",
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            QueryProblem,
            run_id,
            workspace_id,
            catalog,
            schema,
            table,
        )

    def _encode_record_as_historical(self, record: QueryProblem) -> Historical:
        """Encode record as historical.

        A query problem is by definition a failure as it contains problems found while linting code compatibility with
        Unity Catalog.
        """
        # For recreating the original objects, we keep the code and message on the record even though they are
        # duplicated into the failure
        historical = super()._encode_record_as_historical(record)
        failure = f"[{record.code}] {record.message}"
        return replace(historical, failures=historical.failures + [failure])
