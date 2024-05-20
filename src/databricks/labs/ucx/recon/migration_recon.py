import logging
from collections.abc import Iterable
from dataclasses import dataclass
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.hive_metastore.migration_status import MigrationStatusRefresher
from databricks.labs.ucx.recon.base import (
    DataComparator,
    SchemaComparator,
    TableIdentifier,
)

logger = logging.getLogger(__name__)


@dataclass
class ReconResult:
    schema_matches: bool
    data_matches: bool
    schema_comparison: str
    data_comparison: str


class MigrationRecon(CrawlerBase[ReconResult]):
    def __init__(
        self,
        sbe: SqlBackend,
        schema,
        migration_status_refresher: MigrationStatusRefresher,
        schema_comparator: SchemaComparator,
        data_comparator: DataComparator,
    ):
        super().__init__(sbe, "hive_metastore", schema, "recon_result", ReconResult)
        self._migration_status_refresher = migration_status_refresher
        self._schema_comparator = schema_comparator
        self._data_comparator = data_comparator

    def snapshot(self) -> Iterable[ReconResult]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _crawl(self) -> Iterable[ReconResult]:
        self._migration_status_refresher.reset()
        for migration_status in self._migration_status_refresher.snapshot():
            source = TableIdentifier(
                "hive_metastore",
                migration_status.src_schema,
                migration_status.src_table,
            )
            if not self._migration_status_refresher.is_migrated(
                migration_status.src_schema, migration_status.src_table
            ):
                continue
            if not migration_status.dst_catalog or not migration_status.dst_schema or not migration_status.dst_table:
                continue
            target = TableIdentifier(
                migration_status.dst_catalog,
                migration_status.dst_schema,
                migration_status.dst_table,
            )
            schema_comparison = self._schema_comparator.compare_schema(source, target)
            data_comparison = self._data_comparator.compare_data(source, target)
            recon_result = ReconResult(
                schema_comparison.is_matching,
                (
                    data_comparison.source_row_count == data_comparison.target_row_count
                    and data_comparison.num_missing_records_in_target == 0
                    and data_comparison.num_missing_records_in_source == 0
                ),
                schema_comparison.as_dict(),
                data_comparison.as_dict(),
            )
            yield recon_result

    def _try_fetch(self) -> Iterable[ReconResult]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield ReconResult(*row)
