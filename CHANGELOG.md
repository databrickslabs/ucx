# Version changelog

## 0.15.0

* Added AWS S3 support for `migrate-locations` command ([#1009](https://github.com/databrickslabs/ucx/issues/1009)). In this release, the open-source library has been enhanced with AWS S3 support for the `migrate-locations` command, enabling efficient and secure management of S3 data. The new functionality includes the identification of missing S3 prefixes and the creation of corresponding roles and policies through the addition of methods `_identify_missing_paths`, `_get_existing_credentials_dict`, and `create_external_locations`. The library now also includes new classes `AwsIamRole`, `ExternalLocationInfo`, and `StorageCredentialInfo` for better handling of AWS-related functionality. Additionally, two new tests, `test_create_external_locations` and `test_create_external_locations_skip_existing`, have been added to ensure the correct behavior of the new AWS-related functionality. The new test function `test_migrate_locations_aws` checks the AWS-specific implementation of the `migrate-locations` command, while `test_missing_aws_cli` verifies the correct error message is displayed when the AWS CLI is not found in the system path. These changes enhance the library's capabilities, improving data security, privacy, and overall performance for users working with AWS S3.
* Added `databricks labs ucx create-uber-principal` command to create Azure Service Principal for migration ([#976](https://github.com/databrickslabs/ucx/issues/976)). The new CLI command, `databricks labs ucx create-uber-principal`, has been introduced to create an Azure Service Principal (SPN) and grant it STORAGE BLOB READER access on all the storage accounts used by the tables in the workspace. The SPN information is then stored in the UCX cluster policy. A new class, AzureApiClient, has been added to isolate Azure API calls, and unit and integration tests have been included to verify the functionality. This development enhances migration capabilities for Azure workspaces, providing a more streamlined and automated way to create and manage Service Principals, and improves the functionality and usability of the UCX tool. The changes are well-documented and follow the project's coding standards.
* Added `migrate-locations` command ([#1016](https://github.com/databrickslabs/ucx/issues/1016)). In this release, we've added a new CLI command, `migrate_locations`, to create Unity Catalog (UC) external locations. This command extracts candidates for location creation from the `guess_external_locations` assessment task and checks if corresponding UC Storage Credentials exist before creating the locations. Currently, the command only supports Azure, with plans to add support for AWS and GCP in the future. The `migrate_locations` function is marked with the `ucx.command` decorator and is available as a command-line interface (CLI) command. The pull request also includes unit tests for this new command, which check the environment (Azure, AWS, or GCP) before executing the migration and log a message if the environment is AWS or GCP, indicating that the migration is not yet supported on those platforms. No changes have been made to existing workflows, commands, or tables.
* Added handling for widget delete on upgrade platform bug ([#1011](https://github.com/databrickslabs/ucx/issues/1011)). In this release, the `_install_dashboard` method in `dashboards.py` has been updated to handle a platform bug that occurred during the deletion of dashboard widgets during an upgrade process (issue [#1011](https://github.com/databrickslabs/ucx/issues/1011)). Previously, the method attempted to delete each widget using the `self._ws.dashboard_widgets.delete(widget.id)` command, which resulted in a `TypeError` when attempting to delete a widget. The updated method now includes a try/except block that catches this `TypeError` and logs a warning message, while also tracking the issue under bug ES-1061370. The rest of the method remains unchanged, creating a dashboard with the given name, role, and parent folder ID if no widgets are present. This enhancement improves the robustness of the `_install_dashboard` method by adding error handling for the SDK API response when deleting dashboard widgets, ensuring a smoother upgrade process.
* Create UC external locations in Azure based on migrated storage credentials ([#992](https://github.com/databrickslabs/ucx/issues/992)). The `locations.py` file in the `databricks.labs.ucx.azure` package has been updated to include a new class `ExternalLocationsMigration`, which creates UC external locations in Azure based on migrated storage credentials. This class takes various arguments, including `WorkspaceClient`, `HiveMetastoreLocations`, `AzureResourcePermissions`, and `AzureResources`. It has a `run()` method that lists any missing external locations in UC, extracts their location URLs, and attempts to create a UC external location with a mapped storage credential name if the missing external location is in the mapping. The class also includes helper methods for generating credential name mappings. Additionally, the `resources.py` file in the same package has been modified to include a new method `managed_identity_client_id`, which retrieves the client ID of a managed identity associated with a given access connector. Test functions for the `ExternalLocationsMigration` class and Azure external locations functionality have been added in the new file `test_locations.py`. The `test_resources.py` file has been updated to include tests for the `managed_identity_client_id` method. A new `mappings.json` file has also been added for tests related to Azure external location mappings based on migrated storage credentials.
* Deprecate legacy installer ([#1014](https://github.com/databrickslabs/ucx/issues/1014)). In this release, we have deprecated the legacy installer for the UCX project, which was previously implemented as a bash script. A warning message has been added to inform users about the deprecation and direct them to the UCX installation instructions. The functionality of the script remains unchanged, and it still performs tasks such as installing Python dependencies and building Python bindings. The script will eventually be replaced with the `databricks labs install ucx` command. This change is part of issue [#1014](https://github.com/databrickslabs/ucx/issues/1014) and is intended to streamline the installation process and improve the overall user experience. We recommend that users update their installation process to the new recommended method as soon as possible to avoid any issues with the legacy installer in the future.
* Prompt user if Terraform utilised for deploying infrastructure ([#1004](https://github.com/databrickslabs/ucx/issues/1004)). In this update, the `config.py` file has been modified to include a new attribute, `is_terraform_used`, in the `WorkspaceConfig` class. This boolean flag indicates whether Terraform has been used for deploying certain entities in the workspace. Issue [#393](https://github.com/databrickslabs/ucx/issues/393) has been addressed with this change. The `WorkspaceInstaller` configuration has also been updated to take advantage of this new attribute, allowing developers to determine if Terraform was used for infrastructure deployment, thereby increasing visibility into the deployment process. Additionally, a new prompt has been added to the `warehouse_type` function to ascertain if Terraform is being utilized for infrastructure deployment, setting the `is_terraform_used` variable to True if it is. This improvement is intended for software engineers adopting this open-source library.
* Updated CONTRIBUTING.md ([#1005](https://github.com/databrickslabs/ucx/issues/1005)). In this contribution to the open-source library, the CONTRIBUTING.md file has been significantly updated with clearer instructions on how to effectively contibute to the project. The previous command to print the Python path has been removed, as the IDE is now advised to be configured to use the Python interpreter from the virtual environment. A new step has been added, recommending the use of a consistent styleguide and formatting of the code before every commit. Moreover, it is now encouraged to run tests before committing to minimize potential issues during the review process. The steps on how to make a Fork from the ucx repo and create a PR have been updated with links to official documentation. Lastly, the commit now includes information on handling dependency errors that may occur after `git pull`.
* Updated databricks-labs-blueprint requirement from ~=0.2.4 to ~=0.3.0 ([#1001](https://github.com/databrickslabs/ucx/issues/1001)). In this pull request update, the requirements file, pyproject.toml, has been modified to upgrade the databricks-labs-blueprint package from version ~0.2.4 to ~0.3.0. This update integrates the latest features and bug fixes of the package, including an automated upgrade framework, a brute-forcing approach for handling SerdeError, and enhancements for running nightly integration tests with service principals. These improvements increase the testability and functionality of the software, ensuring its stable operation with service principals during nightly integration tests. Furthermore, the reliability of the test for detecting existing installations has been reinforced by adding a new test function that checks for the correct detection of existing installations and retries the test for up to 15 seconds if they are not.

Dependency updates:

 * Updated databricks-labs-blueprint requirement from ~=0.2.4 to ~=0.3.0 ([#1001](https://github.com/databrickslabs/ucx/pull/1001)).

## 0.14.0

* Added `upgraded_from_workspace_id` property to migrated tables to indicated the source workspace ([#987](https://github.com/databrickslabs/ucx/issues/987)). In this release, updates have been made to the `_migrate_external_table`, `_migrate_dbfs_root_table`, and `_migrate_view` methods in the `table_migrate.py` file to include a new parameter `upgraded_from_ws` in the SQL commands used to alter tables, views, or managed tables. This parameter is used to store the source workspace ID in the migrated tables, indicating the migration origin. A new utility method `sql_alter_from` has been added to the `Table` class in `tables.py` to generate the SQL command with the new parameter. Additionally, a new class-level attribute `UPGRADED_FROM_WS_PARAM` has been added to the `Table` class in `tables.py` to indicate the source workspace. A new property `upgraded_from_workspace_id` has been added to migrated tables to store the source workspace ID. These changes resolve issue [#899](https://github.com/databrickslabs/ucx/issues/899) and are tested through manual testing, unit tests, and integration tests. No new CLI commands, workflows, or tables have been added or modified, and there are no changes to user documentation.
* Added a command to create account level groups if they do not exist ([#763](https://github.com/databrickslabs/ucx/issues/763)). This commit introduces a new feature that enables the creation of account-level groups if they do not already exist in the account. A new command, `create-account-groups`, has been added to the `databricks labs ucx` tool, which crawls all workspaces in the account and creates account-level groups if a corresponding workspace-local group is not found. The feature supports various scenarios, including creating account-level groups that exist in some workspaces but not in others, and creating multiple account-level groups with the same name but different members. Several new methods have been added to the `account.py` file to support the new feature, and the `test_account.py` file has been updated with new tests to ensure the correct behavior of the `create_account_level_groups` method. Additionally, the `cli.py` file has been updated to include the new `create-account-groups` command. With these changes, users can easily manage account-level groups and ensure that they are consistent across all workspaces in the account, improving the overall user experience.
* Added assessment for the incompatible `RunSubmit` API usages ([#849](https://github.com/databrickslabs/ucx/issues/849)). In this release, the assessment functionality for incompatible `RunSubmit` API usages has been significantly enhanced through various changes. The 'clusters.py' file has seen improvements in clarity and consistency with the renaming of private methods `check_spark_conf` to `_check_spark_conf` and `check_cluster_failures` to `_check_cluster_failures`. The `_assess_clusters` method has been updated to call the renamed `_check_cluster_failures` method for thorough checks of cluster configurations, resulting in better assessment functionality. A new `SubmitRunsCrawler` class has been added to the `databricks.labs.ucx.assessment.jobs` module, implementing `CrawlerBase`, `JobsMixin`, and `CheckClusterMixin` classes. This class crawls and assesses job runs based on their submitted runs, ensuring compatibility and identifying failure issues. Additionally, a new configuration attribute, `num_days_submit_runs_history`, has been introduced in the `WorkspaceConfig` class of the `config.py` module, controlling the number of days for which submission history of `RunSubmit` API calls is retained. Lastly, various new JSON files have been added for unit testing, assessing the `RunSubmit` API usages related to different scenarios like dbt task runs, Git source-based job runs, JAR file runs, and more. These tests will aid in identifying and addressing potential compatibility issues with the `RunSubmit` API.
* Added group members difference to the output of `validate-groups-membership` cli command ([#995](https://github.com/databrickslabs/ucx/issues/995)). The `validate-groups-membership` command has been updated to include a comparison of group memberships at both the account and workspace levels. This enhancement is implemented through the `validate_group_membership` function, which has been updated to calculate the difference in members between the two levels and display it in a new `group_members_difference` column. This allows for a more detailed analysis of group memberships and easily identifies any discrepancies between the account and workspace levels. The corresponding unit test file, "test_groups.py," has been updated to include a new test case that verifies the calculation of the `group_members_difference` value. The functionality of the other commands remains unchanged. The new `group_members_difference` value is calculated as the difference in the number of members in the workspace group and the account group, with a positive value indicating more members in the workspace group and a negative value indicating more members in the account group. The table template in the labs.yml file has also been updated to include the new column for the group membership difference.
* Added handling for empty `directory_id` if managed identity encountered during the crawling of StoragePermissionMapping ([#986](https://github.com/databrickslabs/ucx/issues/986)). This PR adds a `type` field to the `StoragePermissionMapping` and `Principal` dataclasses to differentiate between service principals and managed identities, allowing `None` for the `directory_id` field if the principal is not a service principal. During the migration to UC storage credentials, managed identities are currently ignored. These changes improve handling of managed identities during the crawling of `StoragePermissionMapping`, prevent errors when creating storage credentials with managed identities, and address issue [#339](https://github.com/databrickslabs/ucx/issues/339). The changes are tested through unit tests, manual testing, and integration tests, and only affect the `StoragePermissionMapping` class and related methods, without introducing new commands, workflows, or tables.
* Added migration for Azure Service Principals with secrets stored in Databricks Secret to UC Storage Credentials ([#874](https://github.com/databrickslabs/ucx/issues/874)). In this release, we have made significant updates to migrate Azure Service Principals with their secrets stored in Databricks Secret to UC Storage Credentials, enhancing security and management of storage access. The changes include: Addition of a new `migrate_credentials` command in the `labs.yml` file to migrate credentials for storage access to UC storage credential. Modification of `secrets.py` to handle the case where a secret has been removed from the backend and to log warning messages for secrets with invalid Base64 bytes. Introduction of the `StorageCredentialManager` and `ServicePrincipalMigration` classes in `credentials.py` to manage Azure Service Principals and their associated client secrets, and to migrate them to UC Storage Credentials. Addition of a new `directory_id` attribute in the `Principal` class and its associated dataclass in `resources.py` to store the directory ID for creating UC storage credentials using a service principal. Creation of a new pytest fixture, `make_storage_credential_spn`, in `fixtures.py` to simplify writing tests requiring Databricks Storage Credentials with Azure Service Principal auth. Addition of a new test file for the Azure integration of the project, including new classes, methods, and test cases for testing the migration of Azure Service Principals to UC Storage Credentials. These improvements will ensure better security and management of storage access using Azure Service Principals, while providing more efficient and robust testing capabilities.
* Added permission migration support for feature tables and the root permissions for models and feature tables ([#997](https://github.com/databrickslabs/ucx/issues/997)). This commit introduces support for migration of permissions related to feature tables and sets root permissions for models and feature tables. New functions such as `feature_store_listing`, `feature_tables_root_page`, `models_root_page`, and `tokens_and_passwords` have been added to facilitate population of a workspace access page with necessary permissions information. The `factory` function in `manager.py` has been updated to include new listings for models' root page, feature tables' root page, and the feature store for enhanced management and access control of models and feature tables. New classes and methods have been implemented to handle permissions for these resources, utilizing `GenericPermissionsSupport`, `AccessControlRequest`, and `MigratedGroup` classes. Additionally, new test methods have been included to verify feature tables listing functionality and root page listing functionality for feature tables and registered models. The test manager method has been updated to include `feature-tables` in the list of items to be checked for permissions, ensuring comprehensive testing of permission functionality related to these new feature tables.
* Added support for serving endpoints ([#990](https://github.com/databrickslabs/ucx/issues/990)). In this release, we have made significant enhancements to support serving endpoints in our open-source library. The `fixtures.py` file in the `databricks.labs.ucx.mixins` module has been updated with new classes and functions to create and manage serving endpoints, accompanied by integration tests to verify their functionality. We have added a new listing for serving endpoints in the assessment's permissions crawling, using the `ws.serving_endpoints.list` function and the `serving-endpoints` category. A new integration test, "test_endpoints," has been added to verify that assessments now crawl permissions for serving endpoints. This test demonstrates the ability to migrate permissions from one group to another. The test suite has been updated to ensure the proper functioning of the new feature and improve the assessment of permissions for serving endpoints, ensuring compatibility with the updated `test_manager.py` file.
* Expanded end-user documentation with detailed descriptions for workflows and commands ([#999](https://github.com/databrickslabs/ucx/issues/999)). The Databricks Labs UCX project has been updated with several new features to assist in upgrading to Unity Catalog, including an assessment workflow that generates a detailed compatibility report for workspace entities, a group migration workflow for upgrading all Databricks workspace assets, and utility commands for managing cross-workspace installations. The Assessment Report now includes a more detailed summary of the assessment findings, table counts, database summaries, and external locations. Additional improvements include expanded workspace group migration to handle potential conflicts with locally scoped group names, enhanced documentation for external Hive Metastore integration, a new debugging notebook, and detailed descriptions of table upgrade considerations, data access permissions, external storage, and table crawler.
* Fixed `config.yml` upgrade from very old versions ([#984](https://github.com/databrickslabs/ucx/issues/984)). In this release, we've introduced enhancements to the configuration upgrading process for `config.yml` in our open-source library. We've replaced the previous `v1_migrate` class method with a new implementation that specifically handles migration from version 1. The new method retrieves the `groups` field, extracts the `selected` value, and assigns it to the `include_group_names` key in the configuration. The `backup_group_prefix` value from the `groups` field is assigned to the `renamed_group_prefix` key, and the `groups` field is removed, with the version number updated to 2. These changes simplify the code and improve readability, enabling users to upgrade smoothly from version 1 of the configuration. Furthermore, we've added new unit tests to the `test_config.py` file to ensure backward compatibility. Two new tests, `test_v1_migrate_zeroconf` and `test_v1_migrate_some_conf`, have been added, utilizing the `MockInstallation` class and loading the configuration using `WorkspaceConfig`. These tests enhance the robustness and reliability of the migration process for `config.yml`.
* Renamed columns in assessment SQL queries to use actual names, not aliases ([#983](https://github.com/databrickslabs/ucx/issues/983)). In this update, we have resolved an issue where aliases used for column references in SQL queries caused errors in certain setups by renaming them to use actual names. Specifically, for assessment SQL queries, we have modified the definition of the `is_delta` column to use the actual `table_format` name instead of the alias `format`. This change improves compatibility and enhances the reliability of query execution. As a software engineer, you will appreciate that this modification ensures consistent interpretation of column references across various setups, thereby avoiding potential errors caused by aliases. This change does not introduce any new methods, but instead modifies existing functionality to use actual column names, ensuring a more reliable and consistent SQL query for the `05_0_all_tables` assessment.
* Updated groups permissions validation to use Table ACL cluster ([#979](https://github.com/databrickslabs/ucx/issues/979)). In this update, the `validate_groups_permissions` task has been modified to utilize the Table ACL cluster, as indicated by the inclusion of `job_cluster="tacl"`. This task is responsible for ensuring that all crawled permissions are accurately applied to the destination groups by calling the `permission_manager.apply_group_permissions` method during the migration state. This modification enhances the validation of group permissions by performing it on the Table ACL cluster, potentially improving performance or functionality. If you are implementing this project, it is crucial to comprehend the consequences of this change on your permissions validation process and adjust your workflows appropriately.


## 0.13.2

* Fixed `AnalysisException` in `crawl_tables` task by ignoring the database that is not found ([#970](https://github.com/databrickslabs/ucx/pull/970)).
* Fixed `Unknown: org.apache.hadoop.hive.ql.metadata.HiveException: NoSuchObjectException` in `crawl_grants` task by ignoring the database that is not found ([#967](https://github.com/databrickslabs/ucx/pull/967)).
* Fixed ruff config for ruff==2.0 ([#969](https://github.com/databrickslabs/ucx/pull/969)).
* Made groups integration tests less flaky ([#965](https://github.com/databrickslabs/ucx/pull/965)).


## 0.13.1

* Added secret detection logic to Azure service principal crawler ([#950](https://github.com/databrickslabs/ucx/pull/950)).
* Create storage credentials based on instance profiles and existing roles ([#869](https://github.com/databrickslabs/ucx/pull/869)).
* Enforced `protected-access` pylint rule ([#956](https://github.com/databrickslabs/ucx/pull/956)).
* Enforced `pylint` on unit and integration test code ([#953](https://github.com/databrickslabs/ucx/pull/953)).
* Enforcing `invalid-name` pylint rule ([#957](https://github.com/databrickslabs/ucx/pull/957)).
* Fixed AzureResourcePermissions.load to call Installation.load ([#962](https://github.com/databrickslabs/ucx/pull/962)).
* Fixed installer script to reuse an existing UCX Cluster policy if present ([#964](https://github.com/databrickslabs/ucx/pull/964)).
* More `pylint` tuning ([#958](https://github.com/databrickslabs/ucx/pull/958)).
* Refactor `workspace_client_mock` to have combine fixtures stored in separate JSON files ([#955](https://github.com/databrickslabs/ucx/pull/955)).

Dependency updates:

 * Updated databricks-sdk requirement from ~=0.19.0 to ~=0.20.0 ([#961](https://github.com/databrickslabs/ucx/pull/961)).

## 0.13.0

* Added CLI Command `databricks labs ucx principal-prefix-access` ([#949](https://github.com/databrickslabs/ucx/pull/949)).
* Added a widget with all jobs to track migration progress ([#940](https://github.com/databrickslabs/ucx/pull/940)).
* Added legacy cluster types to the assessment result ([#932](https://github.com/databrickslabs/ucx/pull/932)).
* Cleanup of install documentation ([#951](https://github.com/databrickslabs/ucx/pull/951), [#947](https://github.com/databrickslabs/ucx/pull/947)).
* Fixed `WorkspaceConfig` initialization for `DEBUG` notebook ([#934](https://github.com/databrickslabs/ucx/pull/934)).
* Fixed installer not opening config file during the installation ([#945](https://github.com/databrickslabs/ucx/pull/945)).
* Fixed groups in config file not considered for group migration job ([#943](https://github.com/databrickslabs/ucx/pull/943)).
* Fixed bug where `tenant_id` inside secret scope is not detected ([#942](https://github.com/databrickslabs/ucx/pull/942)).


## 0.12.0

* Added CLI Command `databricks labs ucx save-uc-compatible-roles` ([#863](https://github.com/databrickslabs/ucx/pull/863)).
* Added dashboard widget with table count by storage and format ([#852](https://github.com/databrickslabs/ucx/pull/852)).
* Added verification of group permissions ([#841](https://github.com/databrickslabs/ucx/pull/841)).
* Checking pipeline cluster config and cluster policy in 'crawl_pipelines' task ([#864](https://github.com/databrickslabs/ucx/pull/864)).
* Created cluster policy (ucx-policy) to be used by all UCX compute. This may require customers to reinstall UCX. ([#853](https://github.com/databrickslabs/ucx/pull/853)).
* Skip scanning objects that were removed on platform side since the last scan time, so that integration tests are less flaky ([#922](https://github.com/databrickslabs/ucx/pull/922)).
* Updated assessment documentation ([#873](https://github.com/databrickslabs/ucx/pull/873)).

Dependency updates:

 * Updated databricks-sdk requirement from ~=0.18.0 to ~=0.19.0 ([#930](https://github.com/databrickslabs/ucx/pull/930)).

## 0.11.1

* Added "what" property for migration to scope down table migrations ([#856](https://github.com/databrickslabs/ucx/pull/856)).
* Added job count in the assessment dashboard ([#858](https://github.com/databrickslabs/ucx/pull/858)).
* Adopted `installation` package from `databricks-labs-blueprint` ([#860](https://github.com/databrickslabs/ucx/pull/860)).
* Debug logs to print only the first 96 bytes of SQL query by default, tunable by `debug_truncate_bytes` SDK configuration property ([#859](https://github.com/databrickslabs/ucx/pull/859)).
* Extract command codes and unify the checks for spark_conf, cluster_policy, init_scripts ([#855](https://github.com/databrickslabs/ucx/pull/855)).
* Improved installation failure with actionable message ([#840](https://github.com/databrickslabs/ucx/pull/840)).
* Improved validating groups membership cli command ([#816](https://github.com/databrickslabs/ucx/pull/816)).

Dependency updates:

 * Updated databricks-labs-blueprint requirement from ~=0.1.0 to ~=0.2.4 ([#867](https://github.com/databrickslabs/ucx/pull/867)).

## 0.11.0

* Added `databricks labs ucx alias` command to create a view of tables from one schema/catalog in another schema/catalog ([#837](https://github.com/databrickslabs/ucx/pull/837)).
* Added `databricks labs ucx save-aws-iam-profiles` command to scan instance profiles identify AWS S3 access and save a CSV with permissions ([#817](https://github.com/databrickslabs/ucx/pull/817)).
* Added total view counts in the assessment dashboard ([#834](https://github.com/databrickslabs/ucx/pull/834)).
* Cleaned up `assess_jobs` and `assess_clusters` tasks in the `assessment` workflow to improve testing and reduce redundancy.([#825](https://github.com/databrickslabs/ucx/pull/825)).
* Added documentation for the assessment report ([#806](https://github.com/databrickslabs/ucx/pull/806)).
* Fixed escaping for SQL object names ([#836](https://github.com/databrickslabs/ucx/pull/836)).

Dependency updates:

 * Updated databricks-sdk requirement from ~=0.17.0 to ~=0.18.0 ([#832](https://github.com/databrickslabs/ucx/pull/832)).

## 0.10.0

* Added `databricks labs ucx validate-groups-membership` command to validate groups to see if they have same membership across acount and workspace level ([#772](https://github.com/databrickslabs/ucx/pull/772)).
* Added baseline for getting Azure Resource Role Assignments ([#764](https://github.com/databrickslabs/ucx/pull/764)).
* Added issue and pull request templates ([#791](https://github.com/databrickslabs/ucx/pull/791)).
* Added linked issues to PR template ([#793](https://github.com/databrickslabs/ucx/pull/793)).
* Added optional `debug_truncate_bytes` parameter to the config and extend the default log truncation limit ([#782](https://github.com/databrickslabs/ucx/pull/782)).
* Added support for crawling grants and applying Hive Metastore UDF ACLs ([#812](https://github.com/databrickslabs/ucx/pull/812)).
* Changed Python requirement from 3.10.6 to 3.10 ([#805](https://github.com/databrickslabs/ucx/pull/805)).
* Extend error handling of delta issues in crawlers and hive metastore ([#795](https://github.com/databrickslabs/ucx/pull/795)).
* Fixed `databricks labs ucx repair-run` command to execute correctly ([#801](https://github.com/databrickslabs/ucx/pull/801)).
* Fixed handling of `DELTASHARING` table format ([#802](https://github.com/databrickslabs/ucx/pull/802)).
* Fixed listing of workflows via CLI ([#811](https://github.com/databrickslabs/ucx/pull/811)).
* Fixed logger import path for DEBUG notebook ([#792](https://github.com/databrickslabs/ucx/pull/792)).
* Fixed move table command to delete table/view regardless if permissions are present, skipping corrupted tables when crawling table size and making existing tests more stable ([#777](https://github.com/databrickslabs/ucx/pull/777)).
* Fixed the issue of `databricks labs ucx installations` and `databricks labs ucx manual-workspace-info` ([#814](https://github.com/databrickslabs/ucx/pull/814)).
* Increase the unit test coverage for cli.py ([#800](https://github.com/databrickslabs/ucx/pull/800)).
* Mount Point crawler lists /Volume with four variations which is confusing ([#779](https://github.com/databrickslabs/ucx/pull/779)).
* Updated README.md to remove mention of deprecated install.sh ([#781](https://github.com/databrickslabs/ucx/pull/781)).
* Updated `bug` issue template ([#797](https://github.com/databrickslabs/ucx/pull/797)).
* Fixed writing log readme in multiprocess safe way ([#794](https://github.com/databrickslabs/ucx/pull/794)).


## 0.9.0

* Added assessment step to estimate the size of DBFS root tables ([#741](https://github.com/databrickslabs/ucx/pull/741)).
* Added `TableMapping` functionality to table migrate ([#752](https://github.com/databrickslabs/ucx/pull/752)).
* Added `databricks labs ucx move` command to move tables and schemas between catalogs ([#756](https://github.com/databrickslabs/ucx/pull/756)).
* Added functionality to determine migration method based on DBFS Root ([#759](https://github.com/databrickslabs/ucx/pull/759)).
* Added `get_tables_to_migrate` functionality in the mapping module ([#755](https://github.com/databrickslabs/ucx/pull/755)).
* Added retry and rate limit to rename workspace group operation and corrected rate limit for reflecting account groups to workspace ([#751](https://github.com/databrickslabs/ucx/pull/751)).
* Adopted `databricks-labs-blueprint` library for common utilities to be reused in the other projects ([#758](https://github.com/databrickslabs/ucx/pull/758)).
* Converted `RuntimeBackend` query executions exceptions to SDK exceptions ([#769](https://github.com/databrickslabs/ucx/pull/769)).
* Fixed issue with missing users and temp groups after workspace-local groups migration and skip table when crawling table size if it does not exist anymore ([#770](https://github.com/databrickslabs/ucx/pull/770)).
* Improved error handling by not failing group rename step if a group was removed from account before reflecting it to workspace ([#762](https://github.com/databrickslabs/ucx/pull/762)).
* Improved error message inference from failed workflow runs ([#753](https://github.com/databrickslabs/ucx/pull/753)).
* Moved `TablesMigrate` to a separate module ([#747](https://github.com/databrickslabs/ucx/pull/747)).
* Reorganized assessment dashboard to increase readability ([#738](https://github.com/databrickslabs/ucx/pull/738)).
* Updated databricks-sdk requirement from ~=0.16.0 to ~=0.17.0 ([#773](https://github.com/databrickslabs/ucx/pull/773)).
* Verify metastore exists in current workspace ([#735](https://github.com/databrickslabs/ucx/pull/735)).


## 0.8.0

* Added `databricks labs ucx repair-run --step ...` CLI command for repair run of any failed workflows, like `assessment`, `migrate-groups` etc. ([#724](https://github.com/databrickslabs/ucx/pull/724)).
* Added `databricks labs ucx revert-migrated-table` command ([#729](https://github.com/databrickslabs/ucx/pull/729)).
* Allow specifying a group list when group match options are used ([#725](https://github.com/databrickslabs/ucx/pull/725)).
* Fixed installation issue when upgrading from an older version of the tool and improve logs ([#740](https://github.com/databrickslabs/ucx/pull/740)).
* Renamed summary panel from Failure Summary to Assessment Summary ([#733](https://github.com/databrickslabs/ucx/pull/733)).
* Retry internal error when getting permissions and update legacy table ACL documentation ([#728](https://github.com/databrickslabs/ucx/pull/728)).
* Speedup installer execution ([#727](https://github.com/databrickslabs/ucx/pull/727)).


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