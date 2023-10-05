# Version changelog

## 0.3.0

* Added `inventory_database` name check during installation ([#275](https://github.com/databricks/ucx/pull/275)).
* Added a column to `$inventory.tables` to specify if a table might have been synchronised to Unity Catalog already or not ([#306](https://github.com/databricks/ucx/pull/306)).
* Added a migration state to skip already migrated tables ([#325](https://github.com/databricks/ucx/pull/325)).
* Fixed appending to tables by adding filtering of `None` rows ([#356](https://github.com/databricks/ucx/pull/356)).
* Fixed handling of missing but linked cluster policies. ([#361](https://github.com/databricks/ucx/pull/361)).
* Ignore errors for Redash widgets and queries redeployment during installation ([#367](https://github.com/databricks/ucx/pull/367)).
* Remove exception and added proper logging for groups in the list that… ([#357](https://github.com/databricks/ucx/pull/357)).
* Skip group migration when no groups are available after preparation step. ([#363](https://github.com/databricks/ucx/pull/363)).
* Update databricks-sdk requirement from ~=0.9.0 to ~=0.10.0 ([#362](https://github.com/databricks/ucx/pull/362)).

## 0.2.0

* Added retrieving for all account-level groups with matching names to workspace-level groups in case no explicit configuration ([#277](https://github.com/databricks/ucx/pull/277)).
* Added crawler for Azure Service principals used for direct storage access ([#305](https://github.com/databricks/ucx/pull/305)).
* Added more SQL queries to the assessment step dashboard ([#269](https://github.com/databricks/ucx/pull/269)).
* Added filtering out for job clusters in the clusters crawler ([#298](https://github.com/databricks/ucx/pull/298)).
* Added recording errors from `crawl_tables` step in `$inventory.table_failures` table and display counter on the dashboard ([#300](https://github.com/databricks/ucx/pull/300)).
* Added comprehensive introduction user manual ([#273](https://github.com/databricks/ucx/pull/273)).
* Added interactive tutorial for local group migration readme ([#291](https://github.com/databricks/ucx/pull/291)).
* Added tutorial links to the landing page of documentation ([#290](https://github.com/databricks/ucx/pull/290)).
* Added (internal) support for account-level configuration and multi-cloud workspace list ([#264](https://github.com/databricks/ucx/pull/264)).
* Improved order of tasks in the README notebook ([#286](https://github.com/databricks/ucx/pull/286)).
* Improved installation script to run in a Windows Git Bash terminal ([#282](https://github.com/databricks/ucx/pull/282)).
* Improved installation script by setting log level to uppercase by default ([#271](https://github.com/databricks/ucx/pull/271)).
* Improved installation finish messages within installer script ([#267](https://github.com/databricks/ucx/pull/267)).
* Improved automation for `MANAGED` table migration and continued building tables migration component ([#295](https://github.com/databricks/ucx/pull/295)).
* Fixed debug notebook code with refactored package structure ([#250](https://github.com/databricks/ucx/pull/250)) ([#265](https://github.com/databricks/ucx/pull/265)).
* Fixed replacement of custom configured database to replicate in the report for external locations ([#296](https://github.com/databricks/ucx/pull/296)).
* Removed redundant `notebooks` top-level folder ([#263](https://github.com/databricks/ucx/pull/263)).
* Split checking for test failures and linting errors into independent GitHub Actions checks ([#287](https://github.com/databricks/ucx/pull/287)).
* Verify query metadata for assessment dashboards during unit tests ([#294](https://github.com/databricks/ucx/pull/294)).

## 0.1.1

* Added batched iteration for `INSERT INTO` queries in `StatementExecutionBackend` with default `max_records_per_batch=1000` ([#237](https://github.com/databricks/ucx/pull/237)).
* Added crawler for mount points ([#209](https://github.com/databricks/ucx/pull/209)).
* Added crawlers for compatibility of jobs and clusters, along with basic recommendations for external locations ([#244](https://github.com/databricks/ucx/pull/244)).
* Added safe return on grants ([#246](https://github.com/databricks/ucx/pull/246)).
* Added ability to specify empty group filter in the installer script ([#216](https://github.com/databricks/ucx/pull/216)) ([#217](https://github.com/databricks/ucx/pull/217)).
* Added ability to install application by multiple different users on the same workspace ([#235](https://github.com/databricks/ucx/pull/235)).
* Added dashboard creation on installation and a requirement for `warehouse_id` in config, so that the assessment dashboards are refreshed automatically after job runs ([#214](https://github.com/databricks/ucx/pull/214)).
* Added reliance on rate limiting from Databricks SDK for listing workspace ([#258](https://github.com/databricks/ucx/pull/258)).
* Fixed errors in corner cases where Azure Service Principal Credentials were not available in Spark context ([#254](https://github.com/databricks/ucx/pull/254)).
* Fixed `DESCRIBE TABLE` throwing errors when listing Legacy Table ACLs ([#238](https://github.com/databricks/ucx/pull/238)).
* Fixed `file already exists` error in the installer script ([#219](https://github.com/databricks/ucx/pull/219)) ([#222](https://github.com/databricks/ucx/pull/222)).
* Fixed `guess_external_locations` failure with `AttributeError: as_dict` and added an integration test ([#259](https://github.com/databricks/ucx/pull/259)).
* Fixed error handling edge cases in `crawl_tables` task ([#243](https://github.com/databricks/ucx/pull/243)) ([#251](https://github.com/databricks/ucx/pull/251)).
* Fixed `crawl_permissions` task failure on folder names containing a forward slash ([#234](https://github.com/databricks/ucx/pull/234)).
* Improved `README` notebook documentation ([#260](https://github.com/databricks/ucx/pull/260), [#228](https://github.com/databricks/ucx/pull/228), [#252](https://github.com/databricks/ucx/pull/252), [#223](https://github.com/databricks/ucx/pull/223), [#225](https://github.com/databricks/ucx/pull/225)).
* Removed redundant `.python-version` file ([#221](https://github.com/databricks/ucx/pull/221)).
* Removed discovery of account groups from `crawl_permissions` task ([#240](https://github.com/databricks/ucx/pull/240)).
* Updated databricks-sdk requirement from ~=0.8.0 to ~=0.9.0 ([#245](https://github.com/databricks/ucx/pull/245)).

## 0.1.0

Features

* Added interactive installation wizard ([#184](https://github.com/databricks/ucx/pull/184), [#117](https://github.com/databricks/ucx/pull/117)).
* Added schedule of jobs as part of `install.sh` flow and created some documentation ([#187](https://github.com/databricks/ucx/pull/187)).
* Added debug notebook companion to troubleshoot the installation ([#191](https://github.com/databricks/ucx/pull/191)).
* Added support for Hive Metastore Table ACLs inventory from all databases ([#78](https://github.com/databricks/ucx/pull/78), [#122](https://github.com/databricks/ucx/pull/122), [#151](https://github.com/databricks/ucx/pull/151)).
* Created `$inventory.tables` from Scala notebook ([#207](https://github.com/databricks/ucx/pull/207)).
* Added local group migration support for ML-related objects ([#56](https://github.com/databricks/ucx/pull/56)).
* Added local group migration support for SQL warehouses ([#57](https://github.com/databricks/ucx/pull/57)).
* Added local group migration support for all compute-related resources ([#53](https://github.com/databricks/ucx/pull/53)).
* Added local group migration support for security-related objects ([#58](https://github.com/databricks/ucx/pull/58)).
* Added local group migration support for workflows ([#54](https://github.com/databricks/ucx/pull/54)).
* Added local group migration support for workspace-level objects ([#59](https://github.com/databricks/ucx/pull/59)).
* Added local group migration support for dashboards, queries, and alerts ([#144](https://github.com/databricks/ucx/pull/144)).

Stability

* Added `codecov.io` publishing ([#204](https://github.com/databricks/ucx/pull/204)).
* Added more tests to group.py ([#148](https://github.com/databricks/ucx/pull/148)).
* Added tests for group state ([#133](https://github.com/databricks/ucx/pull/133)).
* Added tests for inventorizer and typed ([#125](https://github.com/databricks/ucx/pull/125)).
* Added tests WorkspaceListing ([#110](https://github.com/databricks/ucx/pull/110)).
* Added `make_*_permissions` fixtures ([#159](https://github.com/databricks/ucx/pull/159)).
* Added reusable fixtures module ([#119](https://github.com/databricks/ucx/pull/119)).
* Added testing for permissions ([#126](https://github.com/databricks/ucx/pull/126)).
* Added inventory table manager tests ([#153](https://github.com/databricks/ucx/pull/153)).
* Added `product_info` to track as SDK integration ([#76](https://github.com/databricks/ucx/pull/76)).
* Added failsafe permission get operations ([#65](https://github.com/databricks/ucx/pull/65)).
* Always install the latest `pip` version in `./install.sh` ([#201](https://github.com/databricks/ucx/pull/201)).
* Always store inventory in `hive_metastore` and make only `inventory_database` configurable ([#178](https://github.com/databricks/ucx/pull/178)).
* Changed default logging level from `TRACE` to `DEBUG` log level ([#124](https://github.com/databricks/ucx/pull/124)).
* Consistently use `WorkspaceClient` from `databricks.sdk` ([#120](https://github.com/databricks/ucx/pull/120)).
* Convert pipeline code to use fixtures. ([#166](https://github.com/databricks/ucx/pull/166)).
* Exclude mixins from coverage ([#130](https://github.com/databricks/ucx/pull/130)).
* Fixed codecov.io reporting ([#212](https://github.com/databricks/ucx/pull/212)).
* Fixed configuration path in job task install code ([#210](https://github.com/databricks/ucx/pull/210)).
* Fixed a bug with dependency definitions ([#70](https://github.com/databricks/ucx/pull/70)).
* Fixed failing `test_jobs` ([#140](https://github.com/databricks/ucx/pull/140)).
* Fixed the issues with experiment listing ([#64](https://github.com/databricks/ucx/pull/64)).
* Fixed integration testing configuration ([#77](https://github.com/databricks/ucx/pull/77)).
* Make project runnable on nightly testing infrastructure ([#75](https://github.com/databricks/ucx/pull/75)).
* Migrated cluster policies to new fixtures ([#174](https://github.com/databricks/ucx/pull/174)).
* Migrated clusters to the new fixture framework ([#162](https://github.com/databricks/ucx/pull/162)).
* Migrated instance pool to the new fixture framework ([#161](https://github.com/databricks/ucx/pull/161)).
* Migrated to `databricks.labs.ucx` package ([#90](https://github.com/databricks/ucx/pull/90)).
* Migrated token authorization to new fixtures ([#175](https://github.com/databricks/ucx/pull/175)).
* Migrated experiment fixture to standard one ([#168](https://github.com/databricks/ucx/pull/168)).
* Migrated jobs test to fixture based one. ([#167](https://github.com/databricks/ucx/pull/167)).
* Migrated model fixture to the standard fixtures ([#169](https://github.com/databricks/ucx/pull/169)).
* Migrated warehouse fixture to standard one ([#170](https://github.com/databricks/ucx/pull/170)).
* Organise modules by domain ([#197](https://github.com/databricks/ucx/pull/197)).
* Prefetch all account-level and workspace-level groups ([#192](https://github.com/databricks/ucx/pull/192)).
* Programmatically create a dashboard ([#121](https://github.com/databricks/ucx/pull/121)).
* Properly integrate Python `logging` facility ([#118](https://github.com/databricks/ucx/pull/118)).
* Refactored code to use Databricks SDK for Python ([#27](https://github.com/databricks/ucx/pull/27)).
* Refactored configuration and remove global provider state ([#71](https://github.com/databricks/ucx/pull/71)).
* Removed `pydantic` dependency ([#138](https://github.com/databricks/ucx/pull/138)).
* Removed redundant `pyspark`, `databricks-connect`, `delta-spark`, and `pandas` dependencies ([#193](https://github.com/databricks/ucx/pull/193)).
* Removed redundant `typer[all]` dependency and its usages ([#194](https://github.com/databricks/ucx/pull/194)).
* Renamed `MigrationGroupsProvider` to `GroupMigrationState` ([#81](https://github.com/databricks/ucx/pull/81)).
* Replaced `ratelimit` and `tenacity` dependencies with simpler implementations ([#195](https://github.com/databricks/ucx/pull/195)).
* Reorganised integration tests to align more with unit tests ([#206](https://github.com/databricks/ucx/pull/206)).
* Run `build` workflow also on `main` branch ([#211](https://github.com/databricks/ucx/pull/211)).
* Run integration test with a single group ([#152](https://github.com/databricks/ucx/pull/152)).
* Simplify `SqlBackend` and table creation logic ([#203](https://github.com/databricks/ucx/pull/203)).
* Updated `migration_config.yml` ([#179](https://github.com/databricks/ucx/pull/179)).
* Updated legal information ([#196](https://github.com/databricks/ucx/pull/196)).
* Use `make_secret_scope` fixture ([#163](https://github.com/databricks/ucx/pull/163)).
* Use fixture factory for `make_table`, `make_schema`, and `make_catalog` ([#189](https://github.com/databricks/ucx/pull/189)).
* Use new fixtures for notebooks and folders ([#176](https://github.com/databricks/ucx/pull/176)).
* Validate toolkit notebook test ([#183](https://github.com/databricks/ucx/pull/183)).

Contributing

* Added a note on external dependencies ([#139](https://github.com/databricks/ucx/pull/139)).
* Added ability to run SQL queries on Spark when in Databricks Runtime ([#108](https://github.com/databricks/ucx/pull/108)).
* Added some ground rules for contributing ([#82](https://github.com/databricks/ucx/pull/82)).
* Added contributing instructions link from main readme ([#109](https://github.com/databricks/ucx/pull/109)).
* Added info about environment refreshes ([#155](https://github.com/databricks/ucx/pull/155)).
* Clarified documentation ([#137](https://github.com/databricks/ucx/pull/137)).
* Enabled merge queue ([#146](https://github.com/databricks/ucx/pull/146)).
* Improved `CONTRIBUTING.md` guide ([#135](https://github.com/databricks/ucx/pull/135), [#145](https://github.com/databricks/ucx/pull/145)).