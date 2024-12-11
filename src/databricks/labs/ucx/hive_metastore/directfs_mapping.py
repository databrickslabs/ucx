import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler

logger = logging.getLogger(__name__)


@dataclass
class DirectFsRule:
    """
    A rule for direct filesystem access to UC table mapping.
    """

    workspace_name: str
    path: str
    is_read: bool
    is_write: bool
    catalog_name: str
    dst_schema: str
    dst_table: str

    @classmethod
    def initial(
        cls,
        workspace_name: str,
        path: str,
        is_read: bool,
        is_write: bool,
        catalog_name: str,
        dst_schema: str,
        dst_table: str,
    ) -> "DirectFsRule":
        return cls(
            workspace_name=workspace_name,
            path=path,
            is_read=is_read,
            is_write=is_write,
            catalog_name=catalog_name,
            dst_schema=dst_schema,
            dst_table=dst_table,
        )


class DirectFsMapping:
    FILENAME = 'directfs_mapping.csv'
    UCX_SKIP_PROPERTY = "databricks.labs.ucx.skip"

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
    ) -> None:
        self._installation = installation
        self._ws = ws
        self._sql_backend = sql_backend

    def directfs_list(
        self,
        directfs_crawler: DirectFsAccessCrawler,
        tables_crawler: TablesCrawler,
        workspace_name: str,
        catalog_name: str,
    ) -> Iterable["DirectFsRule"]:
        """
        List all direct filesystem access records.
        """
        directfs_snapshot = []
        directfs_crawler.dump_all(directfs_snapshot)
        tables_snapshot = list(tables_crawler.snapshot())
        if not tables_snapshot:
            msg = "No tables found. Please run: databricks labs ucx ensure-assessment-run"
            raise ValueError(msg)
        if not directfs_snapshot:
            msg = "No directfs references found in code"
            raise ValueError(msg)

        # TODO: very inefficient search, just for initial testing
        for table in tables_snapshot:
            for directfs_record in directfs_snapshot:
                if table.location:
                    if directfs_record.path in table.location:
                        yield DirectFsRule.initial(
                            workspace_name=workspace_name,
                            path=directfs_record.path,
                            is_read=directfs_record.is_read,
                            is_write=directfs_record.is_write,
                            catalog_name=catalog_name,
                            dst_schema=table.database,
                            dst_table=table.name,
                        )

    def save(
        self, directfs_crawler: DirectFsAccessCrawler, tables_crawler: TablesCrawler, workspace_info: WorkspaceInfo
    ) -> str:
        """
        Save direct filesystem access records to a CSV file.
        """
        workspace_name = workspace_info.current()
        default_catalog_name = re.sub(r"\W+", "_", workspace_name)
        directfs_records = []


        directfs_records = self.directfs_list(directfs_crawler,  tables_crawler, workspace_name, default_catalog_name)
        return self._installation.save(list(directfs_records), filename=self.FILENAME)
