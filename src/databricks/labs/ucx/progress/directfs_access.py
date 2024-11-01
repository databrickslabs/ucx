from dataclasses import replace

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.source_code.base import DirectFsAccess
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessOwnership


class DirectFsAccessProgressEncoder(ProgressEncoder[DirectFsAccess]):
    """Encoder class:DirectFsAccess to class:History.

    A direct filesystem is by definition a failure as it is not supported in Unity Catalog.
    """

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: DirectFsAccessOwnership,
        run_id: int,
        workspace_id: int,
        catalog: str,
        schema: str = "multiworkspace",
        table: str = "historical",
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            DirectFsAccess,
            run_id,
            workspace_id,
            catalog,
            schema,
            table,
        )

    def _encode_record_as_historical(self, record: DirectFsAccess) -> Historical:
        """Encode record as historical.

        A direct filesystem is by definition a failure as it is not supported in Unity Catalog.
        """
        historical = super()._encode_record_as_historical(record)
        failure = "Direct filesystem access is not supported in Unity Catalog"
        return replace(historical, failures=historical.failures + [failure])
