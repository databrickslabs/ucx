import os
import re
import csv
import logging
from pathlib import Path
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor

from databricks.labs.blueprint.tui import Prompts
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext

logger = logging.getLogger(__name__)


class Exporter:
    # File and Path Constants
    _ZIP_FILE_NAME = "ucx_asseassment_results.zip"
    _UCX_MAIN_QUERIES_PATH = "src/databricks/labs/ucx/queries/assessment/main"

    def __init__(self, ctx: WorkspaceContext):
        self._ctx = ctx

    def _get_ucx_main_queries(self) -> list[dict[str, str]]:
        """Retrieve and construct the main UCX queries."""
        pattern = r"\b.inventory\b"
        schema = self._ctx.inventory_database

        # Create Path object for the UCX_MAIN_QUERIES_PATH
        ucx_main_queries_path = Path(self._UCX_MAIN_QUERIES_PATH)

        # List all SQL files in the directory, excluding those with 'count' in their names
        sql_files = [
            file for file in ucx_main_queries_path.iterdir() if file.suffix == ".sql" and "count" not in file.name
        ]

        ucx_main_queries = [
            {
                "name": "01_1_permissions",
                "query": f"SELECT * FROM {schema}.permissions",
            },
            {"name": "02_2_ucx_grants", "query": f"SELECT * FROM {schema}.grants;"},
            {"name": "03_3_groups", "query": f"SELECT * FROM {schema}.groups;"},
        ]

        for sql_file in sql_files:
            content = sql_file.read_text()
            modified_content = re.sub(pattern, f" {schema}", content, flags=re.IGNORECASE)
            query_name = sql_file.stem
            ucx_main_queries.append({"name": query_name, "query": modified_content})

        return ucx_main_queries

    @staticmethod
    def _extract_target_name(name: str, pattern: str) -> str:
        """Extract target name from the file name using the provided pattern."""
        match = re.search(pattern, name)
        return match.group(1) if match else ""

    @staticmethod
    def _cleanup(path: Path, target_name: str) -> None:
        """Remove a specific CSV file in the given path that matches the target name."""
        target_file = path.joinpath(target_name)

        if target_file.exists():
            target_file.unlink()

    def _execute_query(self, path: Path, result: dict[str, str]) -> None:
        """Execute a SQL query and write the result to a CSV file."""
        pattern = r"^\d+_\d+_(.*)"
        match = re.search(pattern, result["name"])
        if match:
            file_name = f"{match.group(1)}.csv"
            csv_path = os.path.join(path, file_name)

            query_results = list(self._ctx.sql_backend.fetch(result["query"]))

            if query_results:
                headers = query_results[0].asDict().keys()
                with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=headers)
                    writer.writeheader()
                    for row in query_results:
                        writer.writerow(row.asDict())
                # Add the CSV file to the ZIP archive
                self._add_to_zip(path, file_name)

    def _add_to_zip(self, path: Path, file_name) -> None:
        """Create a ZIP file containing all the CSV files."""
        zip_path = path / self._ZIP_FILE_NAME
        file_path = path / file_name

        try:
            with ZipFile(zip_path, 'a') as zipf:
                zipf.write(file_path, arcname=file_name)

        except FileNotFoundError:
            print(f"File {file_path} not found.")
        except PermissionError:
            print(f"Permission denied for {file_path} or {zip_path}.")

        # Clean up the file if it was successfully added
        if file_path.exists():
            self._cleanup(path, file_name)

    def export_results(self, prompts: Prompts, path: Path | None) -> None:
        """Main method to export results to CSV files inside a ZIP archive."""
        results = self._get_ucx_main_queries()
        if path is None:
            response = prompts.question(
                "Choose a path to save the UCX Assessment results",
                default=Path.cwd().as_posix(),
                validate=lambda p_: Path(p_).exists(),
            )
            path = Path(response)
        try:
            logger.info(f"Exporting UCX Assessment (Main) results to {path}")
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(self._execute_query, path, result) for result in results]
                for future in futures:
                    future.result()

        except TimeoutError as e:
            print("A thread execution timed out. Check the query execution logic.")
            print(f"Error exporting results: {e}")
        finally:
            logger.info(f"UCX Assessment (Main) results exported to {path}")
