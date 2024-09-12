import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial

from databricks.sdk.errors import DatabricksError
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatusRefresher
from databricks.labs.ucx.recon.base import (
    DataComparator,
    SchemaComparator,
    TableIdentifier,
    DataComparisonResult,
)

logger = logging.getLogger(__name__)


@dataclass
class ReconResult:
    src_schema: str
    src_table: str
    dst_catalog: str
    dst_schema: str
    dst_table: str
    schema_matches: bool = False
    data_matches: bool = False
    schema_comparison: str | None = None
    data_comparison: str | None = None
    error_message: str | None = None


class MigrationRecon(CrawlerBase[ReconResult]):
    def __init__(
        self,
        sbe: SqlBackend,
        schema: str,
        migration_status_refresher: TableMigrationStatusRefresher,
        table_mapping: TableMapping,
        schema_comparator: SchemaComparator,
        data_comparator: DataComparator,
        default_threshold: float,
    ):
        super().__init__(sbe, "hive_metastore", schema, "recon_results", ReconResult)
        self._migration_status_refresher = migration_status_refresher
        self._table_mapping = table_mapping
        self._schema_comparator = schema_comparator
        self._data_comparator = data_comparator
        self._default_threshold = default_threshold

    def _crawl(self) -> Iterable[ReconResult]:
        self._migration_status_refresher.reset()
        migration_index = self._migration_status_refresher.index()
        migration_rules = self._table_mapping.load()
        tasks = []
        for source in migration_index.snapshot():
            migration_status = migration_index.get(*source)
            if migration_status is None:
                continue
            if not migration_status.dst_catalog or not migration_status.dst_schema or not migration_status.dst_table:
                continue
            source_table = TableIdentifier(
                "hive_metastore",
                migration_status.src_schema,
                migration_status.src_table,
            )
            target_table = TableIdentifier(
                migration_status.dst_catalog,
                migration_status.dst_schema,
                migration_status.dst_table,
            )
            compare_rows = False
            recon_tolerance_percent = self._default_threshold
            for rule in migration_rules:
                if rule.match(source_table):
                    compare_rows = rule.compare_rows
                    recon_tolerance_percent = rule.recon_tolerance_percent
                    break
            tasks.append(
                partial(
                    self._recon,
                    source_table,
                    target_table,
                    compare_rows,
                    recon_tolerance_percent,
                )
            )
        recon_results, errors = Threads.gather("reconciling data", tasks)
        if len(errors) > 0:
            logger.error(f"Detected {len(errors)} while reconciling data")
        return recon_results

    def _recon(
        self,
        source: TableIdentifier,
        target: TableIdentifier,
        compare_rows: bool,
        recon_tolerance_percent: int,
    ) -> ReconResult | None:
        try:
            schema_comparison = self._schema_comparator.compare_schema(source, target)
            data_comparison = self._data_comparator.compare_data(source, target, compare_rows)
        except DatabricksError as e:
            logger.warning(f"Error while comparing {source.fqn_escaped} and {target.fqn_escaped}: {e}")
            return ReconResult(
                source.schema,
                source.table,
                target.catalog,
                target.schema,
                target.table,
                error_message=str(e),
            )
        recon_results = ReconResult(
            source.schema,
            source.table,
            target.catalog,
            target.schema,
            target.table,
            schema_comparison.is_matching,
            (
                self._percentage_difference(data_comparison) <= recon_tolerance_percent
                and data_comparison.target_missing_count == 0
                and data_comparison.source_missing_count == 0
            ),
            json.dumps(schema_comparison.as_dict()),
            json.dumps(data_comparison.as_dict()),
        )
        return recon_results

    @staticmethod
    def _percentage_difference(result: DataComparisonResult) -> int:
        source_row_count = result.source_row_count
        target_row_count = result.target_row_count
        if source_row_count == 0 and target_row_count == 0:
            return 0
        if source_row_count == 0 and target_row_count > 0:
            return 100
        return round(abs(source_row_count - target_row_count) / source_row_count * 100)

    def _try_fetch(self) -> Iterable[ReconResult]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield ReconResult(*row)
