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
from databricks.labs.ucx.workspace_access.listing import WorkspaceListing

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

    def _discover_workspace_objects(self, workspace_path: str) -> list[WorkspaceObjectInfo]:
        """Discover all relevant workspace objects in the given path.

        Args:
            workspace_path: Workspace path to scan

        Returns:
            List of workspace objects (notebooks and files)
        """
        ws_listing = WorkspaceListing(self._ws, num_threads=self._max_workers, with_directories=False)
        workspace_objects = []

        for obj in ws_listing.walk(workspace_path):
            if obj is None or obj.object_type is None:
                continue

            # Only process notebooks and files that can contain code
            if obj.object_type in (ObjectType.NOTEBOOK, ObjectType.FILE):
                raw = obj.as_dict()
                obj_path = raw.get("path")
                if obj_path:  # Only include objects with valid paths
                    workspace_objects.append(WorkspaceObjectInfo(
                        object_type=raw.get("object_type", None),
                        object_id=str(raw.get("object_id", None)),
                        path=obj_path,
                        language=raw.get("language", None),
                    ))

        logger.info(f"Discovered {len(workspace_objects)} workspace objects in {workspace_path}")
        return workspace_objects

    def _extract_tables_from_objects(
        self, workspace_objects: list[WorkspaceObjectInfo]
    ) -> list[UsedTable]:
        """Extract table usage from workspace objects using parallel processing.

        Args:
            workspace_objects: List of workspace objects to process

        Returns:
            List of used tables found in the objects
        """
        if not workspace_objects:
            return []

        tasks = []
        for obj in workspace_objects:
            if obj.path:
                tasks.append(partial(self._extract_tables_from_object, obj))

        logger.info(f"Processing {len(tasks)} workspace objects in parallel...")
        results, errors = Threads.gather('extracting tables from workspace objects', tasks)

        if errors:
            logger.warning(f"Encountered {len(errors)} errors during processing")
            for error in errors[:5]:  # Log first 5 errors
                logger.warning("Logging first 5 errors:")
                logger.warning(f"Processing error: {error}")

        all_tables = []
        for tables in results:
            all_tables.extend(tables)

        return all_tables

    def _extract_tables_from_object(self, obj: WorkspaceObjectInfo) -> list[UsedTable]:
        """Extract table usage from a single workspace object.

        Args:
            obj: Workspace object to process

        Returns:
            List of used tables found in the object
        """
        logger.info(f"Processing {obj.path}...")
        return []


    def scan_workspace_for_tables(
        self,
        workspace_paths: list[str] | None = None
    ) -> None:
        """Scan workspace paths for table usage and store results.

        Args:
            workspace_paths: List of workspace paths to scan. If None, scans entire workspace.
        """
        if workspace_paths is None:
            workspace_paths = ["/"]

        for workspace_path in workspace_paths:
            logger.info(f"Scanning workspace path: {workspace_path}")
            workspace_objects = self._discover_workspace_objects(workspace_path)
            logger.info(f"Found {len(workspace_objects)} workspace objects in {workspace_path}")
            tables_from_path = self._extract_tables_from_objects(workspace_objects)
            logger.info(f"Extracted {len(tables_from_path)} used tables from {workspace_path}")







