# Version changelog

## 0.7.0

* Added `databricks labs ucx create-table-mapping` and `databricks labs ucx manual-workspace-info` commands for CLI ([#682](https://github.com/databrickslabs/ucx/pull/682)).
* Added `databricks labs ucx ensure-assessment-run` to CLI commands ([#708](https://github.com/databrickslabs/ucx/pull/708)).
* Added `databricks labs ucx installations` command ([#679](https://github.com/databrickslabs/ucx/pull/679)).
* Added `databricks labs ucx skip --schema ... --table ...` command to mark table/schema for skipping in the table migration process ([#680](https://github.com/databrickslabs/ucx/pull/680)).
* Added `databricks labs ucx validate-external-locations` command for cli ([#715](https://github.com/databrickslabs/ucx/pull/715)).
* Added capturing `ANY FILE` and `ANONYMOUS FUNCTION` grants ([#653](https://github.com/databrickslabs/ucx/pull/653)).
* Added cluster override and handle case of write protected DBFS ([#610](https://github.com/databrickslabs/ucx/pull/610)).
* Added cluster policy selector in the installer ([#655](https://github.com/databrickslabs/ucx/pull/655)).
* Added detailed UCX pre-requisite information to README.md ([#689](https://github.com/databrickslabs/ucx/pull/689)).
* Added interactive wizard for `databricks labs uninstall ucx` command ([#657](https://github.com/databrickslabs/ucx/pull/657)).
* Added more granular error retry logic ([#704](https://github.com/databrickslabs/ucx/pull/704)).
* Added parallel fetching of registered model identifiers to speed-up assessment workflow ([#691](https://github.com/databrickslabs/ucx/pull/691)).
* Added retry on workspace listing ([#659](https://github.com/databrickslabs/ucx/pull/659)).
* Added support for mapping workspace group to account group by prefix/suffix/regex/external id ([#650](https://github.com/databrickslabs/ucx/pull/650)).
* Changed cluster security mode from NONE to LEGACY_SINGLE_USER, as `crawl_tables` was failing when run on non-UC Workspace in No Isolation mode with unable to access the config file ([#661](https://github.com/databrickslabs/ucx/pull/661)).
* Changed the fields of the table "Tables" to lower case ([#684](https://github.com/databrickslabs/ucx/pull/684)).
* Enabled integration tests for `EXTERNAL` table migrations ([#677](https://github.com/databrickslabs/ucx/pull/677)).
* Enforced `mypy` validation ([#713](https://github.com/databrickslabs/ucx/pull/713)).
* Filtering out inventory database from loading into tables and filtering out the same from grant detail view ([#705](https://github.com/databrickslabs/ucx/pull/705)).
* Fixed documentation for `reflect_account_groups_on_workspace` task and updated `CONTRIBUTING.md` guide ([#654](https://github.com/databrickslabs/ucx/pull/654)).
* Fixed secret scope apply task to raise ValueError ([#683](https://github.com/databrickslabs/ucx/pull/683)).
* Fixed legacy table ACL ownership migration and other integration testing issues ([#722](https://github.com/databrickslabs/ucx/pull/722)).
* Fixed some flaky integration tests ([#700](https://github.com/databrickslabs/ucx/pull/700)).
* New CLI command for workspace mapping ([#678](https://github.com/databrickslabs/ucx/pull/678)).
* Reduce server load for getting workspace groups and their members ([#666](https://github.com/databrickslabs/ucx/pull/666)).
* Throwing ManyError on migrate-groups tasks ([#710](https://github.com/databrickslabs/ucx/pull/710)).
* Updated installation documentation to use Databricks CLI ([#686](https://github.com/databrickslabs/ucx/pull/686)).

Dependency updates:

 * Updated databricks-sdk requirement from ~=0.13.0 to ~=0.14.0 ([#651](https://github.com/databrickslabs/ucx/pull/651)).
 * Updated databricks-sdk requirement from ~=0.14.0 to ~=0.15.0 ([#687](https://github.com/databrickslabs/ucx/pull/687)).
 * Updated databricks-sdk requirement from ~=0.15.0 to ~=0.16.0 ([#712](https://github.com/databrickslabs/ucx/pull/712)).

## 0.6.2

 * Added current version of UCX to task logs ([#566](https://github.com/databrickslabs/ucx/pull/566)).
 * Fixed `'str' object has no attribute 'value'` failure on apply backup group permissions task ([#574](https://github.com/databrickslabs/ucx/pull/574)).
 * Fixed `crawl_cluster` failure over custom runtimes ([#602](https://github.com/databrickslabs/ucx/pull/602)).
 * Fixed `databricks labs ucx workflows` command ([#608](https://github.com/databrickslabs/ucx/pull/608)).
 * Fixed problematic integration test fixture `make_ucx_group` ([#613](https://github.com/databrickslabs/ucx/pull/613)).
 * Fixed internal API request retry logic by relying on concrete exception types ([#637](https://github.com/databrickslabs/ucx/pull/637)).
 * Fixed `tables.scala` notebook to read inventory database from `~/.ucx/config.yml` file. ([#614](https://github.com/databrickslabs/ucx/pull/614)).
 * Introduced `StaticTablesCrawler` for integration tests ([#632](https://github.com/databrickslabs/ucx/pull/632)).
 * Reduced runtime of `test_set_owner_permission` from 15 minutes to 44 seconds ([#636](https://github.com/databrickslabs/ucx/pull/636)).
 * Updated `LICENSE` ([#643](https://github.com/databrickslabs/ucx/pull/643)).
 * Updated documentation ([#611](https://github.com/databrickslabs/ucx/pull/611), [#646](https://github.com/databrickslabs/ucx/pull/646)).

**Breaking changes** (existing installations need to remove `ucx` database, reinstall UCX and re-run assessment jobs)
 * Fixed external locations widget to return hostname for `jdbc:`-sourced tables ([#621](https://github.com/databrickslabs/ucx/pull/621)).

## 0.6.1

 * Added a logo for UCX ([#605](https://github.com/databrickslabs/ucx/pull/605)).
 * Check if the `hatch` is already installed, and install only if it isn't installed yet ([#603](https://github.com/databrickslabs/ucx/pull/603)).
 * Fixed installation check for git pre-release versions ([#600](https://github.com/databrickslabs/ucx/pull/600)).
 * Temporarily remove SQL warehouse requirement from `labs.yml` ([#604](https://github.com/databrickslabs/ucx/pull/604)).

## 0.6.0

**Breaking changes** (existing installations need to reinstall UCX and re-run assessment jobs)

 * Switched local group migration component to rename groups instead of creating backup groups ([#450](https://github.com/databrickslabs/ucx/pull/450)).
 * Mitigate permissions loss in Table ACLs by folding grants belonging to the same principal, object id and object type together ([#512](https://github.com/databrickslabs/ucx/pull/512)).

**New features**

 * Added support for the experimental Databricks CLI launcher ([#517](https://github.com/databrickslabs/ucx/pull/517)).
 * Added support for external Hive Metastores including AWS Glue ([#400](https://github.com/databrickslabs/ucx/pull/400)).
 * Added more views to assessment dashboard ([#474](https://github.com/databrickslabs/ucx/pull/474)).
 * Added rate limit for creating backup group to increase stability ([#500](https://github.com/databrickslabs/ucx/pull/500)).
 * Added deduplication for mount point list ([#569](https://github.com/databrickslabs/ucx/pull/569)).
 * Added documentation to describe interaction with external Hive Metastores ([#473](https://github.com/databrickslabs/ucx/pull/473)).
 * Added failure injection for job failure message propagation  ([#591](https://github.com/databrickslabs/ucx/pull/591)).
 * Added uniqueness in the new warehouse name to avoid conflicts on installation ([#542](https://github.com/databrickslabs/ucx/pull/542)).
 * Added a global init script to collect Hive Metastore lineage ([#513](https://github.com/databrickslabs/ucx/pull/513)).
 * Added retry set/update permissions when possible and assess the changes in the workspace ([#519](https://github.com/databrickslabs/ucx/pull/519)).
 * Use `~/.ucx/state.json` to store the state of both dashboards and jobs ([#561](https://github.com/databrickslabs/ucx/pull/561)).

**Bug fixes**

 * Fixed handling for `OWN` table permissions ([#571](https://github.com/databrickslabs/ucx/pull/571)).
 * Fixed handling of keys with and without values. ([#514](https://github.com/databrickslabs/ucx/pull/514)).
 * Fixed integration test failures related to concurrent group delete ([#584](https://github.com/databrickslabs/ucx/pull/584)).
 * Fixed issue with workspace listing process on None type `object_type` ([#481](https://github.com/databrickslabs/ucx/pull/481)).
 * Fixed missing group entitlement migration bug ([#583](https://github.com/databrickslabs/ucx/pull/583)).
 * Fixed entitlement application for account-level groups ([#529](https://github.com/databrickslabs/ucx/pull/529)).
 * Fixed assessment throwing an error when the owner of an object is empty ([#485](https://github.com/databrickslabs/ucx/pull/485)).
 * Fixed installer to migrate between different configuration file versions ([#596](https://github.com/databrickslabs/ucx/pull/596)).
 * Fixed cluster policy crawler to be aware of deleted policies ([#486](https://github.com/databrickslabs/ucx/pull/486)).
 * Improved error message for not null constraints violated ([#532](https://github.com/databrickslabs/ucx/pull/532)).
 * Improved integration test resiliency ([#597](https://github.com/databrickslabs/ucx/pull/597), [#594](https://github.com/databrickslabs/ucx/pull/594), [#586](https://github.com/databrickslabs/ucx/pull/586)).
 * Introduced Safer access to workspace objects' properties. ([#530](https://github.com/databrickslabs/ucx/pull/530)).
 * Mitigated permissions loss in Table ACLs by running appliers with single thread ([#518](https://github.com/databrickslabs/ucx/pull/518)).
 * Running apply permission task before assessment should display message ([#487](https://github.com/databrickslabs/ucx/pull/487)).
 * Split integration tests from blocking the merge queue ([#496](https://github.com/databrickslabs/ucx/pull/496)).
 * Support more than one dashboard per step ([#472](https://github.com/databrickslabs/ucx/pull/472)).
 * Update databricks-sdk requirement from ~=0.11.0 to ~=0.12.0 ([#505](https://github.com/databrickslabs/ucx/pull/505)).
 * Update databricks-sdk requirement from ~=0.12.0 to ~=0.13.0 ([#575](https://github.com/databrickslabs/ucx/pull/575)).

## 0.5.0

* Added `make install-dev` and a stronger `make clean` for easier dev on-boarding and release upgrades ([#458](https://github.com/databrickslabs/ucx/pull/458)).
* Added failure summary in the assessment dashboard ([#455](https://github.com/databrickslabs/ucx/pull/455)).
* Added test for checking grants in default schema ([#470](https://github.com/databrickslabs/ucx/pull/470)).
* Added unit tests for generic permissions ([#457](https://github.com/databrickslabs/ucx/pull/457)).
* Enabled integration tests via OIDC for every pull request ([#378](https://github.com/databrickslabs/ucx/pull/378)).
* Added check if permissions are up to date ([#421](https://github.com/databrickslabs/ucx/pull/421)).
* Fixed casing in `all_tables.sql` query. ([#464](https://github.com/databrickslabs/ucx/pull/464)).
* Fixed missed scans for empty databases and views in `crawl_grants` ([#469](https://github.com/databrickslabs/ucx/pull/469)).
* Improved logging colors for dark terminal backgrounds ([#468](https://github.com/databrickslabs/ucx/pull/468)).
* Improved local group migration state handling and made log files flush every 10 minutes ([#449](https://github.com/databrickslabs/ucx/pull/449)).
* Moved workspace listing as a separate task for an assessment workflow ([#437](https://github.com/databrickslabs/ucx/pull/437)).
* Removed rate limit for get or create backup group to speed up the prepare environment ([#453](https://github.com/databrickslabs/ucx/pull/453)).
* Updated databricks-sdk requirement from ~=0.10.0 to ~=0.11.0 ([#448](https://github.com/databrickslabs/ucx/pull/448)).

## 0.4.0

* Added exception handling for secret scope not found. ([#418](https://github.com/databrickslabs/ucx/pull/418)).
* Added a crawler for creating an inventory of Azure Service Principals ([#326](https://github.com/databrickslabs/ucx/pull/326)).
* Added check if account group already exists during failure recovery ([#446](https://github.com/databrickslabs/ucx/pull/446)).
* Added checking for index out of range. ([#429](https://github.com/databrickslabs/ucx/pull/429)).
* Added hyperlink to UCX releases in the main readme ([#408](https://github.com/databrickslabs/ucx/pull/408)).
* Added integration test to check backup groups get deleted ([#387](https://github.com/databrickslabs/ucx/pull/387)).
* Added logging of errors during threadpool operations. ([#376](https://github.com/databrickslabs/ucx/pull/376)).
* Added recovery mode for workspace-local groups from temporary groups ([#435](https://github.com/databrickslabs/ucx/pull/435)).
* Added support for migrating Legacy Table ACLs from workspace-local to account-level groups ([#412](https://github.com/databrickslabs/ucx/pull/412)).
* Added detection for installations of unreleased versions ([#399](https://github.com/databrickslabs/ucx/pull/399)).
* Decoupled `PermissionsManager` from `GroupMigrationToolkit` ([#407](https://github.com/databrickslabs/ucx/pull/407)).
* Enabled debug logging for every job task run through a file, which is accessible from both workspace UI and Databricks CLI ([#426](https://github.com/databrickslabs/ucx/pull/426)).
* Ensured that table exists, even when crawlers produce zero records ([#373](https://github.com/databrickslabs/ucx/pull/373)).
* Extended test suite for HMS->HMS TACL migration ([#439](https://github.com/databrickslabs/ucx/pull/439)).
* Fixed handling of secret scope responses ([#431](https://github.com/databrickslabs/ucx/pull/431)).
* Fixed `crawl_permissions` task to respect 'workspace_start_path' config ([#444](https://github.com/databrickslabs/ucx/pull/444)).
* Fixed broken logic in `parallel` module and applied hardened error handling design for parallel code ([#405](https://github.com/databrickslabs/ucx/pull/405)).
* Fixed codecov.io reporting ([#403](https://github.com/databrickslabs/ucx/pull/403)).
* Fixed integration tests for crawlers ([#379](https://github.com/databrickslabs/ucx/pull/379)).
* Improved README.py and logging messages ([#433](https://github.com/databrickslabs/ucx/pull/433)).
* Improved cleanup for workspace backup groups by adding more retries on errors ([#375](https://github.com/databrickslabs/ucx/pull/375)).
* Improved dashboard queries to show unsupported storage types. ([#398](https://github.com/databrickslabs/ucx/pull/398)).
* Improved documentation for readme notebook ([#257](https://github.com/databrickslabs/ucx/pull/257)).
* Improved test coverage for installer ([#371](https://github.com/databrickslabs/ucx/pull/371)).
* Introduced deterministic `env_or_skip` fixture for integration tests ([#396](https://github.com/databrickslabs/ucx/pull/396)).
* Made HMS & UC fixtures return `CatalogInfo`, `SchemaInfo`, and `TableInfo` ([#409](https://github.com/databrickslabs/ucx/pull/409)).
* Merge `workspace_access.Crawler` and `workspace_access.Applier` interfaces to `workspace_access.AclSupport` ([#436](https://github.com/databrickslabs/ucx/pull/436)).
* Moved examples to docs ([#404](https://github.com/databrickslabs/ucx/pull/404)).
* Properly isolated integration testing for workflows on an existing shared cluster ([#414](https://github.com/databrickslabs/ucx/pull/414)).
* Removed thread pool for any IAM Group removals and additions ([#394](https://github.com/databrickslabs/ucx/pull/394)).
* Replace plus char with minus in version tag for GCP dev installation of UCX ([#420](https://github.com/databrickslabs/ucx/pull/420)).
* Run integration tests on shared clusters for a faster devloop ([#397](https://github.com/databrickslabs/ucx/pull/397)).
* Show difference between serverless and PRO warehouses during installation ([#385](https://github.com/databrickslabs/ucx/pull/385)).
* Split `migrate-groups` workflow into three different stages for reliability ([#442](https://github.com/databrickslabs/ucx/pull/442)).
* Use groups instead of usernames in code owners file ([#389](https://github.com/databrickslabs/ucx/pull/389)).

## 0.3.0

* Added `inventory_database` name check during installation ([#275](https://github.com/databrickslabs/ucx/pull/275)).
* Added a column to `$inventory.tables` to specify if a table might have been synchronised to Unity Catalog already or not ([#306](https://github.com/databrickslabs/ucx/pull/306)).
* Added a migration state to skip already migrated tables ([#325](https://github.com/databrickslabs/ucx/pull/325)).
* Fixed appending to tables by adding filtering of `None` rows ([#356](https://github.com/databrickslabs/ucx/pull/356)).
* Fixed handling of missing but linked cluster policies. ([#361](https://github.com/databrickslabs/ucx/pull/361)).
* Ignore errors for Redash widgets and queries redeployment during installation ([#367](https://github.com/databrickslabs/ucx/pull/367)).
* Remove exception and added proper logging for groups in the list that… ([#357](https://github.com/databrickslabs/ucx/pull/357)).
* Skip group migration when no groups are available after preparation step. ([#363](https://github.com/databrickslabs/ucx/pull/363)).
* Update databricks-sdk requirement from ~=0.9.0 to ~=0.10.0 ([#362](https://github.com/databrickslabs/ucx/pull/362)).

## 0.2.0

* Added retrieving for all account-level groups with matching names to workspace-level groups in case no explicit configuration ([#277](https://github.com/databrickslabs/ucx/pull/277)).
* Added crawler for Azure Service principals used for direct storage access ([#305](https://github.com/databrickslabs/ucx/pull/305)).
* Added more SQL queries to the assessment step dashboard ([#269](https://github.com/databrickslabs/ucx/pull/269)).
* Added filtering out for job clusters in the clusters crawler ([#298](https://github.com/databrickslabs/ucx/pull/298)).
* Added recording errors from `crawl_tables` step in `$inventory.table_failures` table and display counter on the dashboard ([#300](https://github.com/databrickslabs/ucx/pull/300)).
* Added comprehensive introduction user manual ([#273](https://github.com/databrickslabs/ucx/pull/273)).
* Added interactive tutorial for local group migration readme ([#291](https://github.com/databrickslabs/ucx/pull/291)).
* Added tutorial links to the landing page of documentation ([#290](https://github.com/databrickslabs/ucx/pull/290)).
* Added (internal) support for account-level configuration and multi-cloud workspace list ([#264](https://github.com/databrickslabs/ucx/pull/264)).
* Improved order of tasks in the README notebook ([#286](https://github.com/databrickslabs/ucx/pull/286)).
* Improved installation script to run in a Windows Git Bash terminal ([#282](https://github.com/databrickslabs/ucx/pull/282)).
* Improved installation script by setting log level to uppercase by default ([#271](https://github.com/databrickslabs/ucx/pull/271)).
* Improved installation finish messages within installer script ([#267](https://github.com/databrickslabs/ucx/pull/267)).
* Improved automation for `MANAGED` table migration and continued building tables migration component ([#295](https://github.com/databrickslabs/ucx/pull/295)).
* Fixed debug notebook code with refactored package structure ([#250](https://github.com/databrickslabs/ucx/pull/250)) ([#265](https://github.com/databrickslabs/ucx/pull/265)).
* Fixed replacement of custom configured database to replicate in the report for external locations ([#296](https://github.com/databrickslabs/ucx/pull/296)).
* Removed redundant `notebooks` top-level folder ([#263](https://github.com/databrickslabs/ucx/pull/263)).
* Split checking for test failures and linting errors into independent GitHub Actions checks ([#287](https://github.com/databrickslabs/ucx/pull/287)).
* Verify query metadata for assessment dashboards during unit tests ([#294](https://github.com/databrickslabs/ucx/pull/294)).

## 0.1.1

* Added batched iteration for `INSERT INTO` queries in `StatementExecutionBackend` with default `max_records_per_batch=1000` ([#237](https://github.com/databrickslabs/ucx/pull/237)).
* Added crawler for mount points ([#209](https://github.com/databrickslabs/ucx/pull/209)).
* Added crawlers for compatibility of jobs and clusters, along with basic recommendations for external locations ([#244](https://github.com/databrickslabs/ucx/pull/244)).
* Added safe return on grants ([#246](https://github.com/databrickslabs/ucx/pull/246)).
* Added ability to specify empty group filter in the installer script ([#216](https://github.com/databrickslabs/ucx/pull/216)) ([#217](https://github.com/databrickslabs/ucx/pull/217)).
* Added ability to install application by multiple different users on the same workspace ([#235](https://github.com/databrickslabs/ucx/pull/235)).
* Added dashboard creation on installation and a requirement for `warehouse_id` in config, so that the assessment dashboards are refreshed automatically after job runs ([#214](https://github.com/databrickslabs/ucx/pull/214)).
* Added reliance on rate limiting from Databricks SDK for listing workspace ([#258](https://github.com/databrickslabs/ucx/pull/258)).
* Fixed errors in corner cases where Azure Service Principal Credentials were not available in Spark context ([#254](https://github.com/databrickslabs/ucx/pull/254)).
* Fixed `DESCRIBE TABLE` throwing errors when listing Legacy Table ACLs ([#238](https://github.com/databrickslabs/ucx/pull/238)).
* Fixed `file already exists` error in the installer script ([#219](https://github.com/databrickslabs/ucx/pull/219)) ([#222](https://github.com/databrickslabs/ucx/pull/222)).
* Fixed `guess_external_locations` failure with `AttributeError: as_dict` and added an integration test ([#259](https://github.com/databrickslabs/ucx/pull/259)).
* Fixed error handling edge cases in `crawl_tables` task ([#243](https://github.com/databrickslabs/ucx/pull/243)) ([#251](https://github.com/databrickslabs/ucx/pull/251)).
* Fixed `crawl_permissions` task failure on folder names containing a forward slash ([#234](https://github.com/databrickslabs/ucx/pull/234)).
* Improved `README` notebook documentation ([#260](https://github.com/databrickslabs/ucx/pull/260), [#228](https://github.com/databrickslabs/ucx/pull/228), [#252](https://github.com/databrickslabs/ucx/pull/252), [#223](https://github.com/databrickslabs/ucx/pull/223), [#225](https://github.com/databrickslabs/ucx/pull/225)).
* Removed redundant `.python-version` file ([#221](https://github.com/databrickslabs/ucx/pull/221)).
* Removed discovery of account groups from `crawl_permissions` task ([#240](https://github.com/databrickslabs/ucx/pull/240)).
* Updated databricks-sdk requirement from ~=0.8.0 to ~=0.9.0 ([#245](https://github.com/databrickslabs/ucx/pull/245)).

## 0.1.0

Features

* Added interactive installation wizard ([#184](https://github.com/databrickslabs/ucx/pull/184), [#117](https://github.com/databrickslabs/ucx/pull/117)).
* Added schedule of jobs as part of `install.sh` flow and created some documentation ([#187](https://github.com/databrickslabs/ucx/pull/187)).
* Added debug notebook companion to troubleshoot the installation ([#191](https://github.com/databrickslabs/ucx/pull/191)).
* Added support for Hive Metastore Table ACLs inventory from all databases ([#78](https://github.com/databrickslabs/ucx/pull/78), [#122](https://github.com/databrickslabs/ucx/pull/122), [#151](https://github.com/databrickslabs/ucx/pull/151)).
* Created `$inventory.tables` from Scala notebook ([#207](https://github.com/databrickslabs/ucx/pull/207)).
* Added local group migration support for ML-related objects ([#56](https://github.com/databrickslabs/ucx/pull/56)).
* Added local group migration support for SQL warehouses ([#57](https://github.com/databrickslabs/ucx/pull/57)).
* Added local group migration support for all compute-related resources ([#53](https://github.com/databrickslabs/ucx/pull/53)).
* Added local group migration support for security-related objects ([#58](https://github.com/databrickslabs/ucx/pull/58)).
* Added local group migration support for workflows ([#54](https://github.com/databrickslabs/ucx/pull/54)).
* Added local group migration support for workspace-level objects ([#59](https://github.com/databrickslabs/ucx/pull/59)).
* Added local group migration support for dashboards, queries, and alerts ([#144](https://github.com/databrickslabs/ucx/pull/144)).

Stability

* Added `codecov.io` publishing ([#204](https://github.com/databrickslabs/ucx/pull/204)).
* Added more tests to group.py ([#148](https://github.com/databrickslabs/ucx/pull/148)).
* Added tests for group state ([#133](https://github.com/databrickslabs/ucx/pull/133)).
* Added tests for inventorizer and typed ([#125](https://github.com/databrickslabs/ucx/pull/125)).
* Added tests WorkspaceListing ([#110](https://github.com/databrickslabs/ucx/pull/110)).
* Added `make_*_permissions` fixtures ([#159](https://github.com/databrickslabs/ucx/pull/159)).
* Added reusable fixtures module ([#119](https://github.com/databrickslabs/ucx/pull/119)).
* Added testing for permissions ([#126](https://github.com/databrickslabs/ucx/pull/126)).
* Added inventory table manager tests ([#153](https://github.com/databrickslabs/ucx/pull/153)).
* Added `product_info` to track as SDK integration ([#76](https://github.com/databrickslabs/ucx/pull/76)).
* Added failsafe permission get operations ([#65](https://github.com/databrickslabs/ucx/pull/65)).
* Always install the latest `pip` version in `./install.sh` ([#201](https://github.com/databrickslabs/ucx/pull/201)).
* Always store inventory in `hive_metastore` and make only `inventory_database` configurable ([#178](https://github.com/databrickslabs/ucx/pull/178)).
* Changed default logging level from `TRACE` to `DEBUG` log level ([#124](https://github.com/databrickslabs/ucx/pull/124)).
* Consistently use `WorkspaceClient` from `databricks.sdk` ([#120](https://github.com/databrickslabs/ucx/pull/120)).
* Convert pipeline code to use fixtures. ([#166](https://github.com/databrickslabs/ucx/pull/166)).
* Exclude mixins from coverage ([#130](https://github.com/databrickslabs/ucx/pull/130)).
* Fixed codecov.io reporting ([#212](https://github.com/databrickslabs/ucx/pull/212)).
* Fixed configuration path in job task install code ([#210](https://github.com/databrickslabs/ucx/pull/210)).
* Fixed a bug with dependency definitions ([#70](https://github.com/databrickslabs/ucx/pull/70)).
* Fixed failing `test_jobs` ([#140](https://github.com/databrickslabs/ucx/pull/140)).
* Fixed the issues with experiment listing ([#64](https://github.com/databrickslabs/ucx/pull/64)).
* Fixed integration testing configuration ([#77](https://github.com/databrickslabs/ucx/pull/77)).
* Make project runnable on nightly testing infrastructure ([#75](https://github.com/databrickslabs/ucx/pull/75)).
* Migrated cluster policies to new fixtures ([#174](https://github.com/databrickslabs/ucx/pull/174)).
* Migrated clusters to the new fixture framework ([#162](https://github.com/databrickslabs/ucx/pull/162)).
* Migrated instance pool to the new fixture framework ([#161](https://github.com/databrickslabs/ucx/pull/161)).
* Migrated to `databricks.labs.ucx` package ([#90](https://github.com/databrickslabs/ucx/pull/90)).
* Migrated token authorization to new fixtures ([#175](https://github.com/databrickslabs/ucx/pull/175)).
* Migrated experiment fixture to standard one ([#168](https://github.com/databrickslabs/ucx/pull/168)).
* Migrated jobs test to fixture based one. ([#167](https://github.com/databrickslabs/ucx/pull/167)).
* Migrated model fixture to the standard fixtures ([#169](https://github.com/databrickslabs/ucx/pull/169)).
* Migrated warehouse fixture to standard one ([#170](https://github.com/databrickslabs/ucx/pull/170)).
* Organise modules by domain ([#197](https://github.com/databrickslabs/ucx/pull/197)).
* Prefetch all account-level and workspace-level groups ([#192](https://github.com/databrickslabs/ucx/pull/192)).
* Programmatically create a dashboard ([#121](https://github.com/databrickslabs/ucx/pull/121)).
* Properly integrate Python `logging` facility ([#118](https://github.com/databrickslabs/ucx/pull/118)).
* Refactored code to use Databricks SDK for Python ([#27](https://github.com/databrickslabs/ucx/pull/27)).
* Refactored configuration and remove global provider state ([#71](https://github.com/databrickslabs/ucx/pull/71)).
* Removed `pydantic` dependency ([#138](https://github.com/databrickslabs/ucx/pull/138)).
* Removed redundant `pyspark`, `databricks-connect`, `delta-spark`, and `pandas` dependencies ([#193](https://github.com/databrickslabs/ucx/pull/193)).
* Removed redundant `typer[all]` dependency and its usages ([#194](https://github.com/databrickslabs/ucx/pull/194)).
* Renamed `MigrationGroupsProvider` to `GroupMigrationState` ([#81](https://github.com/databrickslabs/ucx/pull/81)).
* Replaced `ratelimit` and `tenacity` dependencies with simpler implementations ([#195](https://github.com/databrickslabs/ucx/pull/195)).
* Reorganised integration tests to align more with unit tests ([#206](https://github.com/databrickslabs/ucx/pull/206)).
* Run `build` workflow also on `main` branch ([#211](https://github.com/databrickslabs/ucx/pull/211)).
* Run integration test with a single group ([#152](https://github.com/databrickslabs/ucx/pull/152)).
* Simplify `SqlBackend` and table creation logic ([#203](https://github.com/databrickslabs/ucx/pull/203)).
* Updated `migration_config.yml` ([#179](https://github.com/databrickslabs/ucx/pull/179)).
* Updated legal information ([#196](https://github.com/databrickslabs/ucx/pull/196)).
* Use `make_secret_scope` fixture ([#163](https://github.com/databrickslabs/ucx/pull/163)).
* Use fixture factory for `make_table`, `make_schema`, and `make_catalog` ([#189](https://github.com/databrickslabs/ucx/pull/189)).
* Use new fixtures for notebooks and folders ([#176](https://github.com/databrickslabs/ucx/pull/176)).
* Validate toolkit notebook test ([#183](https://github.com/databrickslabs/ucx/pull/183)).

Contributing

* Added a note on external dependencies ([#139](https://github.com/databrickslabs/ucx/pull/139)).
* Added ability to run SQL queries on Spark when in Databricks Runtime ([#108](https://github.com/databrickslabs/ucx/pull/108)).
* Added some ground rules for contributing ([#82](https://github.com/databrickslabs/ucx/pull/82)).
* Added contributing instructions link from main readme ([#109](https://github.com/databrickslabs/ucx/pull/109)).
* Added info about environment refreshes ([#155](https://github.com/databrickslabs/ucx/pull/155)).
* Clarified documentation ([#137](https://github.com/databrickslabs/ucx/pull/137)).
* Enabled merge queue ([#146](https://github.com/databrickslabs/ucx/pull/146)).
* Improved `CONTRIBUTING.md` guide ([#135](https://github.com/databrickslabs/ucx/pull/135), [#145](https://github.com/databrickslabs/ucx/pull/145)).