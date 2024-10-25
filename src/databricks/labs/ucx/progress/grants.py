from dataclasses import replace

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.hive_metastore.grants import Grant, GrantOwnership
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical


class GrantProgressEncoder(ProgressEncoder[Grant]):
    """Encoder class:Grant to class:History.

    A failure for a grants implies it cannot be mapped to Unity Catalog.
    """

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: GrantOwnership,
        run_id: int,
        workspace_id: int,
        catalog: str,
        schema: str = "multiworkspace",
        table: str = "historical",
    ) -> None:
        super().__init__(sql_backend, ownership, Grant, run_id, workspace_id, catalog, schema, table)

    def _encode_record_as_historical(self, record: Grant) -> Historical:
        historical = super()._encode_record_as_historical(record)
        failures = []
        if not record.uc_grant_sql():
            type_, key = record.this_type_and_key()
            failures = [f"Action '{record.action_type}' on {type_} '{key}' unmappable to Unity Catalog"]
        return replace(historical, failures=historical.failures + failures)
