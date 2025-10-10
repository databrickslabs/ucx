# Workspace Table Scanning

UCX now supports comprehensive table usage detection across your entire Databricks workspace, beyond just workflows and dashboards. This expanded capability allows you to discover all table references in notebooks and files within specified workspace paths.

## Overview

The new workspace scanning feature expands this to:
- **Workspace**: Tables used in any notebook or file within specified workspace paths

**Key Benefits:**
- **Discovery-first approach**: Runs as standalone workflow before assessment
- **Scope optimization**: Can limit Hive Metastore scanning to only databases that are referenced
- **Complete coverage**: Finds table usage beyond just workflows and dashboards
- **Independent execution**: Run on-demand without full assessment cycle

## How It Works

The workspace table scanner:

1. **Discovers Objects**: Recursively scans workspace paths to find all notebooks and supported files
2. **Analyzes Content**: Uses UCX's linting framework to extract table usage from each object
3. **Tracks Lineage**: Maintains detailed source lineage information for each table reference
4. **Stores Results**: Saves findings to the `used_tables_in_workspace` inventory table

## Supported File Types

The scanner supports:
- **Notebooks**: Python, SQL
- **Files**: Python (.py), SQL (.sql)

## Configuration

### Via Standalone Workflow

UCX now includes a dedicated `workspace-table-scanner` workflow that runs independently:


**Workflow Parameters:**
- `workspace_paths`: JSON list of workspace paths to scan (default: `["/"]`)

### Programmatic Usage

```python
from databricks.labs.ucx.source_code.linters.workspace import WorkspaceTablesLinter
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler

# Initialize components
workspace_linter = WorkspaceTablesLinter(
    ws=workspace_client,
    sql_backend=sql_backend,
    inventory_database="ucx_inventory",
    path_lookup=path_lookup,
    used_tables_crawler=UsedTablesCrawler.for_workspace(sql_backend, "ucx_inventory")
)

# Scan specific paths
workspace_paths = ["/Users/data_team", "/Shared/analytics"]
workspace_linter.scan_workspace_for_tables(workspace_paths)
```

## Typical Workflow Sequence

For optimal UCX assessment with scope optimization:

```bash
# 1. Run workspace-table-scanner first (standalone)

# 2. Use results to configure scope-limited assessment
# The scanner workflow will log suggested include_databases configuration

# 3. Update your UCX config with discovered databases
# include_databases: ["database1", "database2", "database3"]

# 4. Run assessment with optimized scope
databricks workflows run assessment


**Scope Optimization Example:**
```sql
-- Query to get databases for config
SELECT DISTINCT schema_name
FROM ucx_inventory.used_tables_in_workspace
WHERE catalog_name = 'hive_metastore'
ORDER BY schema_name;
```

## Results and Analysis

### Inventory Table

Results are stored in `{inventory_database}.used_tables_in_workspace` with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `catalog_name` | string | Catalog containing the table |
| `schema_name` | string | Schema containing the table |
| `table_name` | string | Name of the table |
| `source_id` | string | Path to the workspace object |
| `source_lineage` | array | Detailed lineage information |
| `is_write` | boolean | Whether this is a write operation |

### Example Queries

**Most used tables across workspace:**
```sql
SELECT
    catalog_name,
    schema_name,
    table_name,
    COUNT(*) as usage_count
FROM ucx_inventory.used_tables_in_workspace
GROUP BY catalog_name, schema_name, table_name
ORDER BY usage_count DESC;
```

**Table usage by workspace area:**
```sql
SELECT
    CASE
        WHEN source_id LIKE '/Users/%' THEN 'User Notebooks'
        WHEN source_id LIKE '/Shared/%' THEN 'Shared Notebooks'
        WHEN source_id LIKE '/Repos/%' THEN 'Repository Code'
        ELSE 'Other'
    END as workspace_area,
    COUNT(DISTINCT CONCAT(catalog_name, '.', schema_name, '.', table_name)) as unique_tables,
    COUNT(*) as total_references
FROM ucx_inventory.used_tables_in_workspace
GROUP BY workspace_area;
```

**Files with the most table dependencies:**
```sql
SELECT
    source_id,
    COUNT(DISTINCT CONCAT(catalog_name, '.', schema_name, '.', table_name)) as table_count
FROM ucx_inventory.used_tables_in_workspace
GROUP BY source_id
ORDER BY table_count DESC
LIMIT 20;
```

## Best Practices

### Path Selection
- Start with critical paths like `/Shared/production` or specific team directories
- Avoid scanning entire workspace initially to gauge performance impact
- Exclude test/scratch directories to focus on production code

### Regular Scanning
- Run workspace scans weekly or monthly to track evolving dependencies
- Compare results over time to identify new table dependencies

### Result Analysis
- Combine workspace results with workflow and dashboard results for complete picture
- Use the lineage information to understand code relationships
