# Version changelog

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