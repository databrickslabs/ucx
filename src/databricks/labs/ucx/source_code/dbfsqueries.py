from collections.abc import Iterable

import sqlglot
from sqlglot.expressions import Table

from databricks.labs.ucx.source_code.base import Advice, Deprecation, Linter


class DbfsFromTable(Linter):
    def __init__(self):
        self._dbfs_prefixes = ["/dbfs/mnt", "dbfs:/", "/mnt/", "/dbfs/", "/"]

    def name(self) -> str:
        return 'dbfs-query'

    def lint(self, code: str) -> Iterable[Advice]:
        for statement in sqlglot.parse(code):
            if not statement:
                continue
            for table in statement.find_all(Table):
                # Check table names for deprecated DBFS table names
                yield from self._check_table_name(table)

    def _check_table_name(self, table: Table) -> Iterable[Advice]:
        """
        Check if the table is a DBFS table or reference in some way
        and yield a deprecation message if it is
        """
        if any(table.name.startswith(prefix) for prefix in self._dbfs_prefixes):
            yield Deprecation(
                code='dbfs-query',
                message=f"The use of table {table.db}.{table.name} as a source is deprecated",
                # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
                start_line=0,
                start_col=0,
                end_line=0,
                end_col=1024,
            )
