"""Integration tests for WorkspaceTablesLinter functionality."""

import logging
from datetime import timedelta
from pathlib import Path

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.workspace import ImportFormat, Language

logger = logging.getLogger(__name__)


# @retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_workspace_tables_linter_python_notebook(ws, simple_ctx, make_random):
    """Test that WorkspaceTablesLinter correctly identifies table usage in Databricks notebooks."""

    # Create a test Databricks notebook with table references
    notebook_content = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Test Notebook for Table Discovery
# MAGIC
# MAGIC This notebook contains various table references for testing.

# COMMAND ----------

# Read from a table
df1 = spark.table("sales.customers")
df2 = spark.sql("SELECT * FROM marketing.campaigns")

# COMMAND ----------

# Write to a table using DataFrame method chaining
df1.write.mode("overwrite").saveAsTable("analytics.customer_analysis")

# COMMAND ----------

# PySpark table operations
spark.read.table("warehouse.products").createOrReplaceTempView("temp_products")

# COMMAND ----------
'''


    try:
        # Upload the Databricks notebook to workspace
        ws.workspace.mkdirs("/tmp")
        notebook_path = f"/tmp/test_workspace_linting_{make_random()}.py"
        ws.workspace.upload(
            path=notebook_path,
            content=notebook_content,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )

        # Run the workspace tables linter on the uploaded notebook
        workspace_linter = simple_ctx.workspace_tables_linter
        logger.info(f"Starting workspace scan for path: /tmp")
        workspace_linter.scan_workspace_for_tables(["/tmp"])
        logger.info(f"Workspace scan completed")

        # Verify results in used_tables_in_workspace table
        cursor = simple_ctx.sql_backend.fetch(
            f"""
            SELECT catalog_name, schema_name, table_name, source_id, is_write
            FROM {simple_ctx.inventory_database}.used_tables_in_workspace
            WHERE source_id LIKE '/tmp/test_workspace_linting_%'
            ORDER BY schema_name, table_name
            """
        )
        results = list(cursor)
        logger.info(f"Found {len(results)} table references in database:")
        for result in results:
            logger.info(f"  - {result['schema_name']}.{result['table_name']} (is_write: {result['is_write']})")

        # Expected tables to be found
        expected_tables = {
            ('sales', 'customers', False),          # spark.table("sales.customers")
            ('marketing', 'campaigns', False),     # FROM marketing.campaigns
            ('warehouse', 'products', False),     # spark.read.table
            ('analytics', 'customer_analysis', True)  # saveAsTable("analytics.customer_analysis")
        }

        # Verify we found the expected tables
        assert len(results) == len(expected_tables), (f"Expected at least "
                                                      f"{expected_tables} table references, got {len(results)}")

        # Convert to a set for easier checking
        found_tables = {(r['schema_name'], r['table_name'], r['is_write']) for r in results}

        # Check that all expected tables were found
        for expected in expected_tables:
            assert expected in found_tables, f"Expected table {expected} not found in {found_tables}"

        logger.info(f"Successfully detected {len(results)} table references in notebook")
    finally:
        # Clean up the uploaded notebook
        try:
            ws.workspace.delete(notebook_path, recursive=False)
        except NotFound:
            pass
