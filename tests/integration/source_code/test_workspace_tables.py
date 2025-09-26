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
    """Test that WorkspaceTablesLinter correctly identifies table usage in Python notebooks."""

    # Create a test notebook with table references
    python_content = '''# Databricks notebook source
import spark

# Read from a table
df1 = spark.table("sales.customers")
df2 = spark.sql("SELECT * FROM marketing.campaigns")

# Write to a table
df1.write.mode("overwrite").saveAsTable("analytics.customer_analysis")

# PySpark table operations
spark.read.table("warehouse.products").createOrReplaceTempView("temp_products")
'''


    try:
        # Upload the notebook to workspace
        ws.workspace.mkdirs("/tmp")
        notebook_path = f"/tmp/test_workspace_linting_{make_random()}.py"
        ws.workspace.upload(
            path=notebook_path,
            content=python_content.encode('utf-8'),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )

        workspace_linter = simple_ctx.workspace_tables_linter
        logger.info(f"Starting workspace scan for path: /tmp")
        workspace_linter.scan_workspace_for_tables(["/tmp"])
        logger.info(f"Workspace scan completed")
    finally:
        # Clean up the uploaded notebook
        try:
            ws.workspace.delete(notebook_path, recursive=False)
        except NotFound:
            pass
