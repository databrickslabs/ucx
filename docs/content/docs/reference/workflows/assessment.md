## Assessment workflow

![ucx_assessment_workflow](docs/ucx_assessment_workflow.png)

The assessment workflow can be triggered using the Databricks UI or via the
[`ensure-assessment-run` command](#ensure-assessment-run-command).

The assessment workflow retrieves - or *crawls* - details of [workspace assets](https://docs.databricks.com/en/workspace/workspace-assets.html)
and [securable objects in the Hive metastore](https://docs.databricks.com/en/data-governance/table-acls/object-privileges.html#securable-objects-in-the-hive-metastore)
relevant for upgrading to UC to assess the compatibility with UC. The `crawl_` tasks retrieve assess and objects. The
`assess_` tasks assess the compatibility with UC. The output of each task is stored in the
[inventory database](#installation-resources) so that it can be used for further analysis and decision-making through
the [assessment report](docs/assessment.md).

1. `crawl_tables`: This task retrieves table definitions from the Hive metastore and persists the definitions in
   the `tables` table. The definitions include information such as:
   - Database/schema name
   - Table name
   - Table type
   - Table location
2. `crawl_udfs`: This task retrieves UDF definitions from the Hive metastore and persists the definitions in
   the `udfs` table.
3. `setup_tacl`: (Optimization) This task starts the `tacl` job cluster in parallel to other tasks.
4. `crawl_grants`: This task retrieves [privileges you can grant on Hive objects](https://docs.databricks.com/en/data-governance/table-acls/object-privileges.html#privileges-you-can-grant-on-hive-metastore-objects)
   and persists the privilege definitions in the `grants` table
   The retrieved permission definitions include information such as:
   - Securable object: schema, table, view, (anonymous) function or any file.
   - Principal: user, service principal or group
   - Action type: grant, revoke or deny
5. `estimate_table_size_for_migration`: This task analyzes the Delta table retrieved by `crawl_tables` to retrieve
   an estimate of their size and persists the table sizes in the `table_size` table. The table size support the decision
   for using the `SYNC` or `CLONE` [table migration strategy](#step-3-upgrade-the-metastore).
6. `crawl_mounts`: This task retrieves mount point definitions and persists the definitions in the `mounts` table.
7. `guess_external_locations`: This task guesses shared mount path prefixes of [external tables](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-tables.html)
   retrieved by `crawl_tables` that use mount points and persists the locations in the `external_locations` table. The
   goal is to identify the to-be created UC [external locations](#step-23-create-external-locations).
8. `assess_jobs`: This task retrieves the job definitions and persists the definitions in the `jobs` table. Job
   definitions may require updating to become UC compatible.
9. `assess_clusters`: This task retrieves the clusters definitions and persists the definitions in the `clusters` table.
    Cluster definitions may require updating to become UC compatible.
10. `assess_pipelines`: This task retrieves the [Delta Live Tables](https://www.databricks.com/product/delta-live-tables)
    (DLT) pipelines definitions and persists the definitions in the `pipelines` table. DLT definitions may require
    updating to become UC compatible.
11. `assess_incompatible_submit_runs` : This task retrieves
    [job runs](https://docs.databricks.com/en/jobs/monitor.html#view-runs-for-a-job), also known as
    [job submit runs](https://docs.databricks.com/api/workspace/jobs/submit), and persists the definitions in the
    `submit_runs` table. Incompatibility with UC is assessed:
    - [Databricks runtime](https://www.databricks.com/glossary/what-is-databricks-runtime) should be version 11.3 or above
    - [Access mode](https://docs.databricks.com/en/compute/configure.html#access-modes) should be set.
12. `crawl_cluster_policies` : This tasks retrieves
    [cluster policies](https://docs.databricks.com/en/admin/clusters/policies.html) and persists the policies in the
    `policies` table. Incompatibility with UC is assessed:
    - [Databricks runtime](https://www.databricks.com/glossary/what-is-databricks-runtime) should be version 11.3 or above
13. `assess_azure_service_principals`: This tasks retrieves Azure service principal
    [authentications information](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage#connect-adls)
    from Spark configurations to access storage accounts and persists these configuration information
    in `azure_service_principals` table. The Spark configurations from the following places are retrieved:
    - Clusters configurations
    - Cluster policies
    - Job cluster configurations
    - Pipeline configurations
    - Warehouse configuration
14. `assess_global_init_scripts`: This task retrieves
    [global init scripts](https://docs.databricks.com/en/init-scripts/global.html) and persists references to the
    scripts in the `global_init_scripts` table. Again, Azure service principal authentication information might be given
    in those scripts.
15. `workspace_listing` : This tasks lists [workspace files](https://docs.databricks.com/en/workspace/workspace-assets.html#files)
    recursively to compile a collection of directories, notebooks, files, repos and libraries. The task uses multi-threading
    to parallelize the listing process for speeding up execution on big workspaces.
16. `crawl_permissions` : This tasks retrieves workspace-local groups permissions and persists these permissions in the `permissions` table.
17. `crawl_groups`: This tasks retrieves workspace-local groups and persists these permissions in the `groups` table.
18. `assess_dashboards`: This task retrieves the dashboards to analyze their queries for
    [migration problems](#linter-message-codes)
19. `assess_workflows`: This task retrieves the jobs to analyze their notebooks and files for
    [migration problems](#linter-message-codes).

After UCX assessment workflow finished, see the assessment dashboard for findings and recommendations.
See [this guide](docs/assessment.md) for more details.

![report](docs/assessment-report.png)

Proceed to the [group migration workflow](#group-migration-workflow) below or go back to the
[migration process diagram](#migration-process).


