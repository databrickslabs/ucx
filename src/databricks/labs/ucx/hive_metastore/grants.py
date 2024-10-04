import logging
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from functools import partial, cached_property

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist, NotFound, DatabricksError
from databricks.sdk.service.catalog import (
    ExternalLocationInfo,
    SchemaInfo,
    TableInfo,
    Privilege,
    PermissionsChange,
    SecurableType,
)
from databricks.sdk.service.compute import ClusterSource, DataSecurityMode

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.assessment.aws import AWSRoleAction
from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
)
from databricks.labs.ucx.aws.access import AWSResourcePermissions
from databricks.labs.ucx.azure.access import (
    AzureResourcePermissions,
    StoragePermissionMapping,
)
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocations,
    Mount,
    Mounts,
)
from databricks.labs.ucx.hive_metastore.mapping import TableToMigrate, Rule
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatusRefresher
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler
from databricks.labs.ucx.workspace_access.groups import GroupManager

logger = logging.getLogger(__name__)


@dataclass
class ComputeLocations:
    compute_id: str
    locations: dict
    compute_type: str


@dataclass(frozen=True)
class Grant:
    principal: str
    action_type: str
    catalog: str | None = None
    database: str | None = None
    table: str | None = None
    view: str | None = None
    udf: str | None = None
    any_file: bool = False
    anonymous_function: bool = False

    @staticmethod
    def type_and_key(
        *,
        catalog: str | None = None,
        database: str | None = None,
        table: str | None = None,
        view: str | None = None,
        udf: str | None = None,
        any_file: bool = False,
        anonymous_function: bool = False,
    ) -> tuple[str, str]:
        if table is not None:
            catalog = "hive_metastore" if catalog is None else catalog
            database = "default" if database is None else database
            return "TABLE", f"{catalog}.{database}.{table}"
        if view is not None:
            catalog = "hive_metastore" if catalog is None else catalog
            database = "default" if database is None else database
            return "VIEW", f"{catalog}.{database}.{view}"
        if udf is not None:
            catalog = "hive_metastore" if catalog is None else catalog
            database = "default" if database is None else database
            return "FUNCTION", f"{catalog}.{database}.{udf}"
        if database is not None:
            catalog = "hive_metastore" if catalog is None else catalog
            return "DATABASE", f"{catalog}.{database}"
        if any_file:
            return "ANY FILE", ""
        if anonymous_function:
            return "ANONYMOUS FUNCTION", ""
        # Must come last, as it has the lowest priority here but is a required parameter
        if catalog is not None:
            return "CATALOG", catalog
        msg = (
            f"invalid grant keys: catalog={catalog}, database={database}, view={view}, udf={udf}"
            f"any_file={any_file}, anonymous_function={anonymous_function}"
        )
        raise ValueError(msg)

    @property
    def object_key(self) -> str:
        _, key = self.this_type_and_key()
        return key.lower()

    def this_type_and_key(self):
        return self.type_and_key(
            catalog=self.catalog,
            database=self.database,
            table=self.table,
            view=self.view,
            udf=self.udf,
            any_file=self.any_file,
            anonymous_function=self.anonymous_function,
        )

    def hive_grant_sql(self) -> list[str]:
        object_type, object_key = self.this_type_and_key()
        # See https://docs.databricks.com/en/sql/language-manual/security-grant.html
        statements = []
        actions = self.action_type.split(", ")
        deny_actions = [action for action in actions if "DENIED" in action]
        grant_actions = [action for action in actions if "DENIED" not in action]
        if "OWN" in grant_actions:
            grant_actions.remove("OWN")
            statements.append(self._set_owner_sql(object_type, object_key))
        if grant_actions:
            statements.append(self._apply_grant_sql(", ".join(grant_actions), object_type, object_key))
        if deny_actions:
            statements.append(self._apply_grant_sql(", ".join(deny_actions), object_type, object_key))
        return statements

    def hive_revoke_sql(self) -> str:
        object_type, object_key = self.this_type_and_key()
        return f"REVOKE {self.action_type} ON {object_type} {escape_sql_identifier(object_key)} FROM `{self.principal}`"

    def _set_owner_sql(self, object_type, object_key):
        return f"ALTER {object_type} {escape_sql_identifier(object_key)} OWNER TO `{self.principal}`"

    def _apply_grant_sql(self, action_type, object_type, object_key):
        if "DENIED" in action_type:
            action_type = action_type.replace("DENIED_", "")
            # need to wrap the action type in backticks to avoid syntax errors with DENY SELECT
            return f"DENY `{action_type}` ON {object_type} {escape_sql_identifier(object_key)} TO `{self.principal}`"
        return f"GRANT {action_type} ON {object_type} {escape_sql_identifier(object_key)} TO `{self.principal}`"

    def _uc_action(self, action_type):
        def inner(object_type, object_key):
            return self._apply_grant_sql(action_type, object_type, object_key)

        return inner

    def uc_grant_sql(self, object_type: str | None = None, object_key: str | None = None) -> str | None:
        """Get SQL translated SQL statement for granting similar permissions in UC.

        If there's no UC equivalent, returns None. This can also be the case for missing mapping.
        """

        # TODO: verify and complete the mapping
        # See: https://docs.databricks.com/sql/language-manual/sql-ref-privileges-hms.html
        # See: https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/ownership.html
        # See: https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html
        if object_type is None:
            object_type, object_key = self.this_type_and_key()
        hive_to_uc = {
            ("FUNCTION", "SELECT"): self._uc_action("EXECUTE"),
            ("TABLE", "SELECT"): self._uc_action("SELECT"),
            ("TABLE", "MODIFY"): self._uc_action("MODIFY"),
            ("TABLE", "ALL PRIVILEGES"): self._uc_action("ALL PRIVILEGES"),
            ("TABLE", "OWN"): self._set_owner_sql,
            ("VIEW", "SELECT"): self._uc_action("SELECT"),
            ("VIEW", "OWN"): self._set_owner_sql,
            ("DATABASE", "USAGE"): self._uc_action("USE SCHEMA"),
            ("DATABASE", "CREATE"): self._uc_action("CREATE TABLE"),
            ("DATABASE", "CREATE_NAMED_FUNCTION"): self._uc_action("CREATE FUNCTION"),
            ("DATABASE", "SELECT"): self._uc_action("SELECT"),
            ("DATABASE", "MODIFY"): self._uc_action("MODIFY"),
            ("DATABASE", "OWN"): self._set_owner_sql,
            ("CATALOG", "OWN"): self._set_owner_sql,
            ("CATALOG", "USAGE"): self._uc_action("USE CATALOG"),
        }
        make_query = hive_to_uc.get((object_type, self.action_type), None)
        if make_query is None:
            # unknown mapping or ignore
            return None
        return make_query(object_type, object_key)


CLUSTER_WITHOUT_ACL_FRAGMENT = "Table Access Control is not enabled on this cluster"


class GrantsCrawler(CrawlerBase[Grant]):
    """Crawler that captures access controls that relate to data and other securable objects."""

    def __init__(self, tc: TablesCrawler, udf: UdfsCrawler, include_databases: list[str] | None = None):
        assert tc._backend == udf._backend
        assert tc._catalog == udf._catalog
        assert tc._schema == udf._schema
        super().__init__(tc._backend, tc._catalog, tc._schema, "grants", Grant)
        self._tc = tc
        self._udf = udf
        self._include_databases = include_databases

    def snapshot(self, *, force_refresh: bool = False) -> Iterable[Grant]:
        try:
            return super().snapshot(force_refresh=force_refresh)
        except Exception as e:  # pylint: disable=broad-exception-caught
            log_fn = logger.warning if CLUSTER_WITHOUT_ACL_FRAGMENT in repr(e) else logger.error
            log_fn(f"Couldn't fetch grants snapshot: {e}")
            return []

    def _try_fetch(self):
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Grant(*row)

    def _crawl(self) -> Iterable[Grant]:
        """
        Crawls and lists grants for all databases, tables,  views, udfs, any file
        and anonymous function within hive_metastore.

        Returns:
            list[Grant]: A list of Grant objects representing the listed grants.

        Behavior:
        - Constructs a list of tasks to fetch grants using the `_grants` method, including both database-wide and
          table/view-specific grants.
        - Iterates through tables in the specified database using the `_tc.snapshot` method.
        - For each table, adds tasks to fetch grants for the table or its view, depending on the kind of the table.
        - Iterates through udfs in the specified database using the `_udf.snapshot` method.
        - For each udf, adds tasks to fetch grants for the udf.
        - Executes the tasks concurrently using Threads.gather.
        - Flattens the list of retrieved grant lists into a single list of Grant objects.

        Note:
        - The method assumes that the `_grants` method fetches grants based on the provided parameters (catalog,
          database, table, view, udfs, any file, anonymous function).

        Returns:
        list[Grant]: A list of Grant objects representing the grants found in hive_metastore.
        """
        catalog = "hive_metastore"
        tasks = [
            partial(self.grants, catalog=catalog),
            # Scanning ANY FILE and ANONYMOUS FUNCTION grants
            partial(self.grants, catalog=catalog, any_file=True),
            partial(self.grants, catalog=catalog, anonymous_function=True),
        ]
        if not self._include_databases:
            # scan all databases, even empty ones
            for row in self._fetch(f"SHOW DATABASES FROM {escape_sql_identifier(catalog)}"):
                tasks.append(partial(self.grants, catalog=catalog, database=row.databaseName))
        else:
            for database in self._include_databases:
                tasks.append(partial(self.grants, catalog=catalog, database=database))
        for table in self._tc.snapshot():
            fn = partial(self.grants, catalog=catalog, database=table.database)
            # Views are treated as a type of table and enumerated by the table crawler.
            if table.view_text is None:
                task = partial(fn, table=table.name)
            else:
                task = partial(fn, view=table.name)
            tasks.append(task)
        for udf in self._udf.snapshot():
            fn = partial(self.grants, catalog=catalog, database=udf.database)
            tasks.append(partial(fn, udf=udf.name))
        catalog_grants, errors = Threads.gather(f"listing grants for {catalog}", tasks)
        if len(errors) > 0:
            raise ManyError(errors)
        return [grant for grants in catalog_grants for grant in grants]

    def for_table_info(self, table: TableInfo):
        # TODO: it does not work yet for views
        principal_permissions = defaultdict(set)
        for grant in self.grants(catalog=table.catalog_name, database=table.schema_name, table=table.name):
            principal_permissions[grant.principal].add(grant.action_type)
        return principal_permissions

    def for_schema_info(self, schema: SchemaInfo):
        principal_permissions = defaultdict(set)
        for grant in self.grants(catalog=schema.catalog_name, database=schema.name):
            principal_permissions[grant.principal].add(grant.action_type)
        return principal_permissions

    # The reported ObjectType for grants doesn't necessary match the type of grants we asked for. This maps
    # those that aren't themselves.
    _grants_reported_as = {
        # SHOW type: RESULT type
        "SCHEMA": "DATABASE",
        "CATALOG": "CATALOG$",
        "VIEW": "TABLE",
        "ANY FILE": "ANY_FILE",
        "ANONYMOUS FUNCTION": "ANONYMOUS_FUNCTION",
    }

    def grants(
        self,
        *,
        catalog: str | None = None,
        database: str | None = None,
        table: str | None = None,
        view: str | None = None,
        udf: str | None = None,
        any_file: bool = False,
        anonymous_function: bool = False,
    ) -> list[Grant]:
        """
        Fetches and yields grant information for the specified database objects.

        Keyword Args:
            catalog (str): The catalog name (optional).
            database (str | None): The database name (optional).
            table (str | None): The table name (optional).
            view (str | None): The view name (optional).
            udf (str | None): The udf name (optional).
            any_file (bool): Whether to include any file grants (optional).
            anonymous_function (bool): Whether to include anonymous function grants (optional).

        Behavior:
        - Normalizes the provided parameters and constructs an object type and key using
          the `Grant.type_and_key` method.
        - Iterates through rows fetched using the `_fetch` method by executing a SQL query
          to retrieve grant information.
        - For each fetched row, extracts the principal, action type, and object type.
        - Normalizes the object type and filters grants based on the provided object type
          and the fetched object type.
        - Yields a Grant object representing the fetched grant information.

        Note:
        - The method fetches and yields grants based on the provided parameters and
          the available grant information in the database.

        Returns:
        Iterator[Grant]: An iterator of Grant objects representing the fetched grant information.
        """
        on_type, key = Grant.type_and_key(
            catalog=self._try_valid(catalog),
            database=self._try_valid(database),
            table=self._try_valid(table),
            view=self._try_valid(view),
            udf=self._try_valid(udf),
            any_file=any_file,
            anonymous_function=anonymous_function,
        )
        expected_grant_object_type = self._grants_reported_as.get(on_type, on_type)
        grants = []
        try:
            for row in self._fetch(f"SHOW GRANTS ON {on_type} {escape_sql_identifier(key)}"):
                (principal, action_type, object_type, _) = row
                # Sometimes we get grants for other objects we didn't ask for. For example, listing DATABASE grants
                # may also enumerate grants on the associated CATALOG.
                if object_type != expected_grant_object_type:
                    continue
                # we have to return concrete list, as with yield we're executing
                # everything on the main thread.
                grant = Grant(
                    principal=principal,
                    action_type=action_type,
                    table=table,
                    view=view,
                    udf=udf,
                    database=database,
                    catalog=catalog,
                    any_file=any_file,
                    anonymous_function=anonymous_function,
                )
                grants.append(grant)
            return grants
        except NotFound:
            # This makes the integration test more robust as many test schemas are being created and deleted quickly.
            logger.warning(f"Schema {catalog}.{database} no longer existed")
            return []
        except Exception as e:  # pylint: disable=broad-exception-caught
            log_fn = logger.warning if CLUSTER_WITHOUT_ACL_FRAGMENT in repr(e) else logger.error
            log_fn(f"Couldn't fetch grants for object {on_type} {key}: {e}")
            return []


class AwsACL:
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        installation: Installation,
    ):
        self._backend = backend
        self._ws = ws
        self._installation = installation

    def _get_cluster_to_instance_profile_mapping(self) -> dict[str, str]:
        # this function gives a mapping between an interactive cluster and the instance profile used by it
        # either directly or through a cluster policy.
        cluster_instance_profiles = {}

        for cluster in self._ws.clusters.list():
            if (
                cluster.cluster_id is None
                or cluster.cluster_source == ClusterSource.JOB
                or (cluster.data_security_mode not in [DataSecurityMode.LEGACY_SINGLE_USER, DataSecurityMode.NONE])
            ):
                continue
            if cluster.aws_attributes is None:
                continue
            if cluster.aws_attributes.instance_profile_arn is not None:
                role_name = cluster.aws_attributes.instance_profile_arn
                assert role_name is not None
                cluster_instance_profiles[cluster.cluster_id] = role_name
                continue

        return cluster_instance_profiles

    def _update_warehouse_to_instance_profile_mapping(
        self,
    ) -> dict[str, str]:
        warehouse_instance_profiles = {}
        sql_config = self._ws.warehouses.get_workspace_warehouse_config()
        if sql_config.instance_profile_arn is not None:
            role_name = sql_config.instance_profile_arn
            for warehouse in self._ws.warehouses.list():
                if warehouse.id is not None:
                    warehouse_instance_profiles[warehouse.id] = role_name
        return warehouse_instance_profiles

    def get_eligible_locations_principals(self) -> list[ComputeLocations]:
        cluster_instance_profiles = self._get_cluster_to_instance_profile_mapping()
        warehouse_instance_profiles = self._update_warehouse_to_instance_profile_mapping()
        compute_locations = []
        if len(cluster_instance_profiles) == 0 and len(warehouse_instance_profiles) == 0:
            # if there are no interactive clusters or warehouse with instance profile , then return empty grants
            logger.info("No interactive cluster or sql warehouse found with instance profiles configured")
            return []
        external_locations = list(self._ws.external_locations.list())
        if len(external_locations) == 0:
            # if there are no external locations, then throw an error to run migrate_locations cli command
            msg = (
                "No external location found, If hive metastore tables are created in external storage, "
                "ensure migrate-locations cli cmd is run to create the required locations."
            )
            logger.error(msg)
            raise ResourceDoesNotExist(msg) from None

        permission_mappings = self._installation.load(
            list[AWSRoleAction],
            filename=AWSResourcePermissions.INSTANCE_PROFILES_FILE_NAME,
        )
        if len(permission_mappings) == 0:
            # if permission mapping is empty, raise an error to run principal_prefix cmd
            msg = (
                "No instance profile permission file found. Please ensure principal-prefix-access cli "
                "cmd is run to create the instance profile permission file."
            )
            logger.error(msg)
            raise ResourceDoesNotExist(msg) from None

        for cluster_id, role_compute in cluster_instance_profiles.items():
            eligible_locations = self._get_external_locations(role_compute, external_locations, permission_mappings)
            if len(eligible_locations) == 0:
                continue
            compute_locations.append(ComputeLocations(cluster_id, eligible_locations, "clusters"))
        for warehouse_id, role_compute in warehouse_instance_profiles.items():
            eligible_locations = self._get_external_locations(role_compute, external_locations, permission_mappings)
            if len(eligible_locations) == 0:
                continue
            compute_locations.append(ComputeLocations(warehouse_id, eligible_locations, "warehouses"))
        return compute_locations

    @staticmethod
    def _get_external_locations(
        role_name: str,
        external_locations: list[ExternalLocationInfo],
        permission_mappings: list[AWSRoleAction],
    ) -> dict[str, str]:
        matching_location = {}
        for location in external_locations:
            if location.url is None:
                continue
            for permission_mapping in permission_mappings:
                # check if resource_path contains "*"
                if permission_mapping.resource_path.find('*') > 1:
                    resource_url = permission_mapping.resource_path[: permission_mapping.resource_path.index('*') - 1]
                else:
                    resource_url = permission_mapping.resource_path
                if location.url.startswith(resource_url) and permission_mapping.role_arn == role_name:
                    matching_location[location.url] = permission_mapping.privilege
        return matching_location


class AzureACL:
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        spn_crawler: AzureServicePrincipalCrawler,
        installation: Installation,
    ):
        self._backend = backend
        self._ws = ws
        self._spn_crawler = spn_crawler
        self._installation = installation

    def get_eligible_locations_principals(self) -> list[ComputeLocations]:
        compute_locations = []
        spn_cluster_mapping = self._spn_crawler.get_cluster_to_storage_mapping()
        spn_warehouse_mapping = self._spn_crawler.get_warehouse_to_storage_mapping()
        if len(spn_cluster_mapping) == 0 and len(spn_warehouse_mapping) == 0:
            # if there are no interactive clusters , then return empty grants
            logger.info("No interactive cluster found with spn configured")
            return []
        external_locations = list(self._ws.external_locations.list())
        if len(external_locations) == 0:
            # if there are no external locations, then throw an error to run migrate_locations cli command
            msg = (
                "No external location found, If hive metastore tables are created in external storage, "
                "ensure migrate-locations cli cmd is run to create the required locations."
            )
            logger.error(msg)
            raise ResourceDoesNotExist(msg) from None

        permission_mappings = self._installation.load(
            list[StoragePermissionMapping],
            filename=AzureResourcePermissions.FILENAME,
        )
        if len(permission_mappings) == 0:
            # if permission mapping is empty, raise an error to run principal_prefix cmd
            msg = (
                "No storage permission file found. Please ensure principal-prefix-access cli "
                "cmd is run to create the access permission file."
            )
            logger.error(msg)
            raise ResourceDoesNotExist(msg) from None

        for cluster_spn in spn_cluster_mapping:
            eligible_locations = {}
            for spn in cluster_spn.spn_info:
                eligible_locations.update(self._get_external_locations(spn, external_locations, permission_mappings))
            compute_locations.append(ComputeLocations(cluster_spn.cluster_id, eligible_locations, "clusters"))

        for warehouse_spn in spn_warehouse_mapping:
            eligible_locations = {}
            for spn in warehouse_spn.spn_info:
                eligible_locations.update(self._get_external_locations(spn, external_locations, permission_mappings))
            compute_locations.append(ComputeLocations(warehouse_spn.cluster_id, eligible_locations, "warehouses"))

        return compute_locations

    def _get_external_locations(
        self,
        spn: AzureServicePrincipalInfo,
        external_locations: list[ExternalLocationInfo],
        permission_mappings: list[StoragePermissionMapping],
    ) -> dict[str, str]:
        matching_location = {}
        for location in external_locations:
            if location.url is None:
                continue
            for permission_mapping in permission_mappings:
                prefix = permission_mapping.prefix
                if (
                    location.url.startswith(permission_mapping.prefix)
                    and permission_mapping.client_id == spn.application_id
                    and spn.storage_account is not None
                    # check for storage account name starting after @ in the prefix url
                    and prefix[prefix.index('@') + 1 :].startswith(spn.storage_account)
                ):
                    matching_location[location.url] = permission_mapping.privilege
        return matching_location


class PrincipalACL:
    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        installation: Installation,
        tables_crawler: TablesCrawler,
        mounts_crawler: Mounts,
        cluster_locations: Callable[[], list[ComputeLocations]],
    ):
        self._backend = backend
        self._ws = ws
        self._installation = installation
        self._tables_crawler = tables_crawler
        self._mounts_crawler = mounts_crawler
        self._compute_locations = cluster_locations

    def get_interactive_cluster_grants(self) -> list[Grant]:
        tables = list(self._tables_crawler.snapshot())
        mounts = list(self._mounts_crawler.snapshot())
        grants: set[Grant] = set()

        for compute_location in self._compute_locations():
            principals = self._get_cluster_principal_mapping(compute_location.compute_id, compute_location.compute_type)
            if len(principals) == 0:
                continue
            cluster_usage = self._get_grants(compute_location.locations, principals, tables, mounts)
            grants.update(cluster_usage)
        return list(grants)

    def _get_privilege(self, table: Table, locations: dict[str, str], mounts: list[Mount]) -> str | None:
        if table.view_text is not None:
            # return nothing for view so that it goes to the separate view logic
            return None
        if table.location is None:
            return None
        if table.location.startswith('dbfs:/mnt') or table.location.startswith('/dbfs/mnt'):
            mount_location = ExternalLocations.resolve_mount(table.location, mounts)
            for loc, privilege in locations.items():
                if loc is not None and mount_location.startswith(loc):
                    return privilege
            return None
        if table.location.startswith('dbfs:/') or table.location.startswith('/dbfs/'):
            return "WRITE_FILES"

        for loc, privilege in locations.items():
            if loc is not None and table.location.startswith(loc):
                return privilege
        return None

    def _get_database_grants(self, tables: list[Table], principals: list[str]) -> list[Grant]:
        databases = {table.database for table in tables}
        grants = []
        for database in databases:
            for principal in principals:
                grant = Grant(principal, "USAGE", "hive_metastore", database)
                grants.append(grant)
        return grants

    def _get_grants(
        self, locations: dict[str, str], principals: list[str], tables: list[Table], mounts: list[Mount]
    ) -> list[Grant]:
        grants = []
        filtered_tables = []
        for table in tables:
            privilege = self._get_privilege(table, locations, mounts)
            if privilege == "READ_FILES":
                for principal in principals:
                    grants.append(Grant(principal, "SELECT", table.catalog, table.database, table.name))
                filtered_tables.append(table)
                continue
            if privilege == "WRITE_FILES":
                for principal in principals:
                    grants.append(Grant(principal, "ALL PRIVILEGES", table.catalog, table.database, table.name))
                filtered_tables.append(table)
                continue
            if table.view_text is not None:
                for principal in principals:
                    grants.append(Grant(principal, "ALL PRIVILEGES", table.catalog, table.database, view=table.name))
                filtered_tables.append(table)
        database_grants = self._get_database_grants(filtered_tables, principals)
        grants.extend(database_grants)
        return grants

    def _get_cluster_principal_mapping(self, cluster_id: str, object_type: str) -> list[str]:
        # gets all the users,groups,spn which have access to the clusters and returns a dataclass of that mapping
        principal_list = []
        try:
            cluster_permission = self._ws.permissions.get(object_type, cluster_id)
        except ResourceDoesNotExist:
            return []
        if cluster_permission.access_control_list is None:
            return []
        for acl in cluster_permission.access_control_list:
            if acl.user_name is not None:
                principal_list.append(acl.user_name)
            if acl.group_name is not None:
                if acl.group_name == "admins":
                    continue
                principal_list.append(acl.group_name)
            if acl.service_principal_name is not None:
                principal_list.append(acl.service_principal_name)
        return principal_list

    def apply_location_acl(self):
        """
        Check the interactive cluster and the principals mapped to it
        identifies the spn or instance profile configured for the interactive cluster
        identifies any location the spn/instance profile have access to (read or write)
        applies create_external_table, create_external_volume and read_files permission for all location
        to the principal
        """
        logger.info(
            "Applying permission for external location (CREATE EXTERNAL TABLE, "
            "CREATE EXTERNAL VOLUME and READ_FILES for existing eligible interactive cluster users"
        )
        # get the eligible location mapped for each interactive cluster
        for compute_location in self._compute_locations():
            # get interactive cluster users
            principals = self._get_cluster_principal_mapping(compute_location.compute_id, compute_location.compute_type)
            if len(principals) == 0:
                continue
            for location_url in compute_location.locations.keys():
                # get the location name for the given url
                location_name = self._get_location_name(location_url)
                if location_name is None:
                    continue
                self._update_location_permissions(location_name, principals)
        logger.info("Applied all the permission on external location")

    def _update_location_permissions(self, location_name: str, principals: list[str]):
        permissions = [Privilege.CREATE_EXTERNAL_TABLE, Privilege.CREATE_EXTERNAL_VOLUME, Privilege.READ_FILES]
        changes = [PermissionsChange(add=permissions, principal=principal) for principal in principals]
        self._ws.grants.update(
            SecurableType.EXTERNAL_LOCATION,
            location_name,
            changes=changes,
        )

    def _get_location_name(self, location_url: str):
        for location in self._ws.external_locations.list():
            if location.url == location_url:
                return location.name
        return None


class MigrateGrants:
    def __init__(
        self,
        sql_backend: SqlBackend,
        group_manager: GroupManager,
        grant_loaders: list[Callable[[], Iterable[Grant]]],
    ):
        self._sql_backend = sql_backend
        self._group_manager = group_manager
        self._grant_loaders = grant_loaders

    def apply(self, src: Table, uc_table_key: str) -> bool:
        for grant in self._match_grants(src):
            acl_migrate_sql = grant.uc_grant_sql(src.kind, uc_table_key)
            if acl_migrate_sql is None:
                logger.warning(
                    f"failed-to-migrate: Hive metastore grant '{grant.action_type}' cannot be mapped to UC grant for "
                    f"{src.kind} '{uc_table_key}'. Skipping."
                )
                continue
            logger.debug(f"Migrating acls on {uc_table_key} using SQL query: {acl_migrate_sql}")
            try:
                self._sql_backend.execute(acl_migrate_sql)
            except DatabricksError as e:
                logger.warning(f"failed-to-migrate: Failed to migrate ACL for {src.key} to {uc_table_key}: {e}")
        return True

    @cached_property
    def _workspace_to_account_group_names(self) -> dict[str, str]:
        return {g.name_in_workspace: g.name_in_account for g in self._group_manager.snapshot()}

    @cached_property
    def _grants(self) -> list[Grant]:
        grants = []
        for loader in self._grant_loaders:
            for grant in loader():
                grants.append(grant)
        return grants

    def _match_grants(self, table: Table) -> list[Grant]:
        matched_grants = []
        for grant in self._grants:
            if grant.database != table.database:
                continue
            if table.name not in (grant.table, grant.view):
                continue
            grant = self._replace_account_group(grant)
            matched_grants.append(grant)
        return matched_grants

    def _replace_account_group(self, grant: Grant) -> Grant:
        target_principal = self._workspace_to_account_group_names.get(grant.principal)
        if not target_principal:
            return grant
        return replace(grant, principal=target_principal)


class ACLMigrator:
    def __init__(
        self,
        tables_crawler: TablesCrawler,
        workspace_info: WorkspaceInfo,
        migration_status_refresher: TableMigrationStatusRefresher,
        migrate_grants: MigrateGrants,
    ):
        self._table_crawler = tables_crawler
        self._workspace_info = workspace_info
        self._migration_status_refresher = migration_status_refresher
        self._migrate_grants = migrate_grants

    def migrate_acls(self, *, target_catalog: str | None = None, hms_fed: bool = False) -> None:
        workspace_name = self._workspace_info.current()
        tables = list(self._table_crawler.snapshot())
        if not tables:
            logger.info("No tables found to acl")
            return
        if hms_fed:
            tables_to_migrate = self._get_hms_fed_tables(tables, target_catalog if target_catalog else workspace_name)
        else:
            tables_to_migrate = self._get_migrated_tables(tables)
        self._migrate_acls(tables_to_migrate)

    def _get_migrated_tables(self, tables: list[Table]) -> list[TableToMigrate]:
        # gets all the migrated table to apply ACLs to
        tables_to_migrate = []
        seen_tables = self._migration_status_refresher.get_seen_tables()
        reverse_seen_tables = {v: k for k, v in seen_tables.items()}
        for table in tables:
            if table.key not in reverse_seen_tables:
                logger.warning(f"Table {table.key} not found in migration status. Skipping.")
                continue
            dst_table_parts = reverse_seen_tables[table.key].split(".")
            if len(dst_table_parts) != 3:
                logger.warning(
                    f"Invalid table name {reverse_seen_tables[table.key]} found in migration status. Skipping."
                )
                continue
            rule = Rule(
                self._workspace_info.current(),
                dst_table_parts[0],
                table.database,
                dst_table_parts[1],
                table.name,
                dst_table_parts[2],
            )
            table_to_migrate = TableToMigrate(table, rule)
            tables_to_migrate.append(table_to_migrate)
        return tables_to_migrate

    def _get_hms_fed_tables(self, tables: list[Table], target_catalog) -> list[TableToMigrate]:
        # if it is hms_fed acl migration, migrate all the acls for tables in the provided catalog
        tables_to_migrate = []
        for table in tables:
            rule = Rule(
                self._workspace_info.current(),
                target_catalog,
                table.database,
                table.database,
                table.name,
                table.name,
            )
            table_to_migrate = TableToMigrate(table, rule)
            tables_to_migrate.append(table_to_migrate)
        return tables_to_migrate

    def _migrate_acls(self, tables_in_scope: list[TableToMigrate]) -> None:
        tasks = []
        for table in tables_in_scope:
            tasks.append(partial(self._migrate_grants.apply, table.src, table.rule.as_uc_table_key))
        Threads.strict("migrate grants", tasks)

    def _is_migrated(self, schema: str, table: str) -> bool:
        index = self._migration_status_refresher.index()
        return index.is_migrated(schema, table)
