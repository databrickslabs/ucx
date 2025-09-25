"""Workspace-wide linter for table usage detection.

This module provides functionality to scan all notebooks and files in a workspace
path and collect table usage information using the UCX linting framework.
"""

import logging
from collections.abc import Iterable
from functools import partial
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, Language
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.source_code.base import (
    UsedTable,
    CurrentSessionState,
    LineageAtom,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.files import NotebookLinter
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler
from databricks.labs.ucx.workspace_access.generic import WorkspaceObjectInfo

logger = logging.getLogger(__name__)


class WorkspaceTablesLinter:
    """Linter for extracting table usage from all notebooks and files in workspace paths.

    This class scans workspace paths recursively to find all notebooks and files,
    then uses the UCX linting framework to extract table usage information.
    """

    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        inventory_database: str,
        path_lookup: PathLookup,
        used_tables_crawler: UsedTablesCrawler,
        max_workers: int = 10,
    ):
        """Initialize the WorkspaceTablesLinter.

        Args:
            ws: Databricks WorkspaceClient for API access
            sql_backend: SQL backend for storing results
            inventory_database: Database name for storing inventory
            path_lookup: Path lookup for resolving dependencies
            used_tables_crawler: Crawler for storing used table results
            max_workers: Maximum number of parallel workers for processing
        """
        self._ws = ws
        self._sql_backend = sql_backend
        self._inventory_database = inventory_database
        self._path_lookup = path_lookup
        self._used_tables_crawler = used_tables_crawler
        self._max_workers = max_workers

    def _get_language_from_path(self, path: str) -> Language | None:
        """Determine language from file path extension.

        Args:
            path: File path

        Returns:
            Language enum or None if not supported
        """

        extension = path.lower().split('.')[-1] if '.' in path else ''

        language_map = {
            'py': Language.PYTHON,
            'sql': Language.SQL,
            'scala': Language.SCALA,
            'r': Language.R,
        }

        return language_map.get(extension)
