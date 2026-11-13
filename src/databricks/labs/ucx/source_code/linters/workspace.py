"""Workspace-wide linter for table usage detection.

This module provides functionality to scan all notebooks and files in a workspace
path and collect table usage information using the UCX linting framework.
"""

import ast
import base64
import logging
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType, Language
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex

from databricks.labs.ucx.source_code.base import (
    UsedTable,
    CurrentSessionState,
    LineageAtom,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler
from databricks.labs.ucx.workspace_access.generic import WorkspaceObjectInfo
from databricks.labs.ucx.workspace_access.listing import WorkspaceListing

logger = logging.getLogger(__name__)


class WorkspaceCodeLinter:
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
                    workspace_objects.append(
                        WorkspaceObjectInfo(
                            path=obj_path,
                            object_type=raw.get("object_type"),
                            object_id=str(raw.get("object_id")),
                            language=raw.get("language"),
                        )
                    )

        logger.info(f"Discovered {len(workspace_objects)} workspace objects in {workspace_path}")
        return workspace_objects

    def _extract_tables_from_objects(self, workspace_objects: list[WorkspaceObjectInfo]) -> list[UsedTable]:
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
        if not obj.path:
            return []

        # Create a source lineage for the object
        source_lineage = [
            LineageAtom(
                object_type=obj.object_type or "UNKNOWN",
                object_id=obj.path or "UNKNOWN",
                other={
                    "language": obj.language or "UNKNOWN",
                },
            )
        ]

        # Determine if this is a notebook or file based on object type and path
        # For now, let's be more conservative and only treat explicit NOTEBOOK types as notebooks
        # We can enhance this later with content-based detection if needed
        if obj.object_type == ("NOTEBOOK"):
            return self._extract_tables_from_notebook(obj, source_lineage)
        if obj.object_type == ("FILE"):
            return self._extract_tables_from_file(obj, source_lineage)
        logger.warning(f"Unsupported object type: {obj.object_type}")
        return []

    def _get_str_content_from_path(self, path: str) -> str:
        """Download and decode content from a workspace path.

        Args:
            path: Path to the workspace path

        Returns:
                Decoded content as string
        """
        # Download file content
        export_response = self._ws.workspace.export(path)
        if isinstance(export_response.content, bytes):
            return export_response.content.decode('utf-8')
        try:
            # If content is a string representation of bytes, convert it back to bytes
            # Try to evaluate the string as a bytes literal
            content_bytes = ast.literal_eval(str(export_response.content))
            return content_bytes.decode('utf-8')
        except (ValueError, SyntaxError):
            # If that fails, try base64 decoding
            try:
                return base64.b64decode(str(export_response.content)).decode('utf-8')
            except ValueError:
                # If that also fails, treat it as a regular string
                return str(export_response.content)

    def _extract_tables_from_notebook(
        self, obj: WorkspaceObjectInfo, source_lineage: list[LineageAtom]
    ) -> list[UsedTable]:
        """Extract table usage from a notebook.


        Args:
            obj: Notebook object
            source_lineage: Source lineage for tracking

        Returns:
            List of used tables found in the notebook
        """
        # Download notebook content
        content = self._get_str_content_from_path(obj.path)
        return self._extract_tables_from_notebook_content(obj, content, source_lineage)

    def _extract_tables_from_file(self, obj: WorkspaceObjectInfo, source_lineage: list[LineageAtom]) -> list[UsedTable]:
        """Extract table usage from a file.

        Args:
            obj: File object
            source_lineage: Source lineage for tracking

        Returns:
            List of used tables found in the file
        """
        if not obj.path:
            return []
        content = self._get_str_content_from_path(obj.path)

        # Check if this is actually a Databricks notebook stored as a file
        if "# Databricks notebook source" in content:
            logger.info(f"Detected notebook content in file {obj.path}, treating as notebook")
            return self._extract_tables_from_notebook_content(obj, content, source_lineage)

        return self._process_file_content_for_tables(obj, content, source_lineage)

    @staticmethod
    def _get_clean_cell_content(cell_content: str) -> str:
        """Clean up cell content by removing magic commands and leading/trailing whitespace.

        Args:
            cell_content: Raw cell content

        Returns:
            Cleaned cell content
        """
        if not cell_content.strip().startswith('# MAGIC'):
            return cell_content

        # Remove MAGIC prefixes and clean up
        clean_lines = []
        for line in cell_content.split('\n'):
            if line.strip().startswith('# MAGIC'):
                # Remove the # MAGIC prefix
                clean_line = line.replace('# MAGIC ', '')
                # For SQL magic commands, also remove the %sql part
                if clean_line.strip() != '%sql':
                    clean_lines.append(clean_line)
            else:
                clean_lines.append(line)
        return '\n'.join(clean_lines)

    def _get_language_from_content(self, cell_content: str) -> Language:
        """Determine the language of a notebook cell based on magic commands.

        Args:
            cell_content: Raw cell content

        Returns:
            Detected Language enum (default to Python)
        """

        if cell_content.strip().startswith('# MAGIC %sql'):
            return Language.SQL
        if cell_content.strip().startswith('# MAGIC %scala'):
            return Language.SCALA
        if cell_content.strip().startswith('# MAGIC %r'):
            return Language.R
        return Language.PYTHON

    def _extract_tables_from_notebook_content(
        self, obj: WorkspaceObjectInfo, content: str, source_lineage: list[LineageAtom]
    ) -> list[UsedTable]:
        """Extract table usage from notebook content without using Notebook.parse().

        This method handles notebook content that might not parse correctly with Notebook.parse()
        by manually extracting Python/SQL code from the notebook cells.

        Args:
            obj: Workspace object
            content: Notebook content as string
            source_lineage: Source lineage for tracking

        Returns:
            List of used tables found in the notebook
        """
        # Split content into lines and extract cells manually
        lines = content.split('\n')
        if not lines[0].startswith("# Databricks notebook source"):
            logger.warning(f"Content doesn't start with notebook header: {obj.path}")
            return []

        # Extract cells by looking for # COMMAND ---------- separators
        cells = []
        current_cell: list[str] = []

        for line in lines[1:]:  # Skip the header line
            if line.strip() == "# COMMAND ----------":
                if current_cell:
                    cells.append('\n'.join(current_cell))
                    current_cell = []
            else:
                current_cell.append(line)

        # Add the last cell if it exists
        if current_cell:
            cells.append('\n'.join(current_cell))

        logger.info(f"Extracted {len(cells)} cells from notebook {obj.path}")

        return self._process_cells_for_tables(obj, cells, source_lineage)

    def _process_cells_for_tables(
        self, obj: WorkspaceObjectInfo, cells: list[str], source_lineage: list[LineageAtom]
    ) -> list[UsedTable]:
        """Process notebook cells to extract table usage.

        Args:
            obj: Workspace object
            cells: List of cell contents
            source_lineage: Source lineage for tracking

        Returns:
            List of used tables found in the cells
        """
        # Process each cell to extract tables
        all_tables = []
        dummy_index = TableMigrationIndex([])
        linter_context = LinterContext(dummy_index, CurrentSessionState())

        for i, cell_content in enumerate(cells):
            if not cell_content.strip():
                continue

            # Determine cell language (default to Python for now)
            # Check if cell has magic commands that indicate language
            cell_language = self._get_language_from_content(cell_content)

            # Get appropriate collector for the cell language
            try:
                collector = linter_context.tables_collector(cell_language)
            except ValueError as e:
                logger.warning(f"Failed to get collector for language {cell_language}: {e}")
                continue

            # Clean up the cell content (remove MAGIC prefixes)
            clean_content = self._get_clean_cell_content(cell_content)

            cell_tables = list(collector.collect_tables(clean_content))
            logger.info(f"Found {len(cell_tables)} tables in cell {i}")

            # Add source lineage to each table
            for table in cell_tables:
                all_tables.append(
                    table.replace_source(
                        source_id=obj.path,
                        source_lineage=source_lineage,
                    )
                )
        return all_tables

    def _process_file_content_for_tables(
        self, obj: WorkspaceObjectInfo, content: str, source_lineage: list[LineageAtom]
    ) -> list[UsedTable]:
        """Process file content to extract table usage.

        Args:
            obj: Workspace object
            content: File content as string
            source_lineage: Source lineage for tracking

        Returns:
            List of used tables found in the file content
        """
        # Determine language from file extension
        language = self._get_language_from_path(obj.path)
        if not language:
            logger.debug(f"Unsupported file type: {obj.path}")
            return []

        # Create linter context with dummy migration index to use full collectors
        dummy_index = TableMigrationIndex([])
        linter_context = LinterContext(dummy_index, CurrentSessionState())

        # Get appropriate collector for the language
        # At this point language is guaranteed to be not None
        assert language is not None
        collector = linter_context.tables_collector(language)
        tables = list(collector.collect_tables(str(content)))

        # Add source lineage to each table
        result_tables = []
        for table in tables:
            if hasattr(table, 'replace_source'):
                result_tables.append(
                    table.replace_source(
                        source_id=obj.path,
                        source_lineage=source_lineage,
                    )
                )
            else:
                result_tables.append(table)

        return result_tables

    def scan_workspace_for_tables(self, workspace_paths: list[str] | None = None) -> None:
        """Scan workspace paths for table usage and store results.

        Args:
            workspace_paths: List of workspace paths to scan. If None, scans entire workspace.
        """
        if workspace_paths is None:
            workspace_paths = ["/"]

        all_tables = []
        for workspace_path in workspace_paths:
            logger.info(f"Scanning workspace path: {workspace_path}")
            workspace_objects = self._discover_workspace_objects(workspace_path)
            logger.info(f"Found {len(workspace_objects)} workspace objects in {workspace_path}")
            tables_from_path = self._extract_tables_from_objects(workspace_objects)
            logger.info(f"Extracted {len(tables_from_path)} used tables from {workspace_path}")
            all_tables.extend(tables_from_path)

        # Store all discovered tables in the database
        if all_tables:
            logger.info(f"Storing {len(all_tables)} discovered tables in database")
            self._used_tables_crawler.dump_all(all_tables)
            logger.info(f"Successfully stored {len(all_tables)} tables")
        else:
            logger.info("No tables found to store")
