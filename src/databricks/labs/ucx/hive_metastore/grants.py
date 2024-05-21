import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist, NotFound
from databricks.sdk.service.catalog import (
    ExternalLocationInfo,
    SchemaInfo,
    TableInfo,
    Privilege,
    PermissionsChange,
    SecurableType,
)
from databricks.sdk.service.compute import ClusterSource, DataSecurityMode

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
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler

logger = logging.getLogger(__name__)


@dataclass
class LocationACL:
    location_name: str
    principal: str


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
        if "OWN" in actions:
            actions.remove("OWN")
            statements.append(self._set_owner_sql(object_type, object_key))
        if actions:
            statements.append(self._apply_grant_sql(", ".join(actions), object_type, object_key))
        return statements

    def hive_revoke_sql(self) -> str:
        object_type, object_key = self.this_type_and_key()
        return f"REVOKE {self.action_type} ON {object_type} {escape_sql_identifier(object_key)} FROM `{self.principal}`"

    def _set_owner_sql(self, object_type, object_key):
        return f"ALTER {object_type} {escape_sql_identifier(object_key)} OWNER TO `{self.principal}`"

    def _apply_grant_sql(self, action_type, object_type, object_key):
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
            ("TABLE", "READ_METADATA"): self._uc_action("BROWSE"),
            ("TABLE", "ALL PRIVILEGES"): self._uc_action("ALL PRIVILEGES"),
            ("TABLE", "OWN"): self._set_owner_sql,
            ("VIEW", "SELECT"): self._uc_action("SELECT"),
            ("VIEW", "READ_METADATA"): self._uc_action("BROWSE"),
            ("VIEW", "OWN"): self._set_owner_sql,
            ("DATABASE", "USAGE"): self._uc_action("USE SCHEMA"),
            ("DATABASE", "CREATE"): self._uc_action("CREATE TABLE"),
            ("DATABASE", "CREATE_NAMED_FUNCTION"): self._uc_action("CREATE FUNCTION"),
            ("DATABASE", "SELECT"): self._uc_action("SELECT"),
            ("DATABASE", "MODIFY"): self._uc_action("MODIFY"),
            ("DATABASE", "OWN"): self._set_owner_sql,
            ("DATABASE", "READ_METADATA"): self._uc_action("BROWSE"),
            ("CATALOG", "OWN"): self._set_owner_sql,
            ("CATALOG", "USAGE"): self._uc_action("USE CATALOG"),
        }
        make_query = hive_to_uc.get((object_type, self.action_type), None)
        if make_query is None:
            # unknown mapping or ignore
            return None
        return make_query(object_type, object_key)


class GrantsCrawler(CrawlerBase[Grant]):
    def __init__(self, tc: TablesCrawler, udf: UdfsCrawler, include_databases: list[str] | None = None):
        assert tc._backend == udf._backend
        assert tc._catalog == udf._catalog
        assert tc._schema == udf._schema
        super().__init__(tc._backend, tc._catalog, tc._schema, "grants", Grant)
        self._tc = tc
        self._udf = udf
        self._include_databases = include_databases

    def snapshot(self) -> Iterable[Grant]:
        return self._snapshot(partial(self._try_load), partial(self._crawl))

    def _try_load(self):
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
        tasks = [partial(self.grants, catalog=catalog)]
        # Scanning ANY FILE and ANONYMOUS FUNCTION grants
        tasks.append(partial(self.grants, catalog=catalog, any_file=True))
        tasks.append(partial(self.grants, catalog=catalog, anonymous_function=True))
        if not self._include_databases:
            # scan all databases, even empty ones
            for row in self._fetch(f"SHOW DATABASES FROM {escape_sql_identifier(catalog)}"):
                tasks.append(partial(self.grants, catalog=catalog, database=row.databaseName))
        else:
            for database in self._include_databases:
                tasks.append(partial(self.grants, catalog=catalog, database=database))
        for table in self._tc.snapshot():
            fn = partial(self.grants, catalog=catalog, database=table.database)
            # views are recognized as tables
            tasks.append(partial(fn, table=table.name))
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
        try:  # pylint: disable=too-many-try-statements
            grants = []
            object_type_normalization = {
                "SCHEMA": "DATABASE",
                "CATALOG$": "CATALOG",
                "ANY_FILE": "ANY FILE",
                "ANONYMOUS_FUNCTION": "ANONYMOUS FUNCTION",
            }
            for row in self._fetch(f"SHOW GRANTS ON {on_type} {escape_sql_identifier(key)}"):
                (principal, action_type, object_type, _) = row
                object_type = object_type_normalization.get(object_type, object_type)
                if on_type != object_type:
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
            # This make the integration test more robust as many test schemas are being created and deleted quickly.
            logger.warning(f"Schema {catalog}.{database} no longer existed")
            return []
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"Couldn't fetch grants for object {on_type} {key}: {e}")
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

    def get_eligible_locations_principals(self) -> dict[str, dict]:
        cluster_locations = {}
        eligible_locations = {}
        cluster_instance_profiles = self._get_cluster_to_instance_profile_mapping()
        if len(cluster_instance_profiles) == 0:
            # if there are no interactive clusters , then return empty grants
            logger.info("No interactive cluster found with instance profiles configured")
            return {}
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

        for cluster_id, role_name in cluster_instance_profiles.items():
            eligible_locations.update(self._get_external_locations(role_name, external_locations, permission_mappings))
            if len(eligible_locations) == 0:
                continue
            cluster_locations[cluster_id] = eligible_locations
        return cluster_locations

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

    def get_eligible_locations_principals(self) -> dict[str, dict]:
        cluster_locations = {}
        eligible_locations = {}
        spn_cluster_mapping = self._spn_crawler.get_cluster_to_storage_mapping()
        if len(spn_cluster_mapping) == 0:
            # if there are no interactive clusters , then return empty grants
            logger.info("No interactive cluster found with spn configured")
            return {}
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
            for spn in cluster_spn.spn_info:
                eligible_locations.update(self._get_external_locations(spn, external_locations, permission_mappings))
            cluster_locations[cluster_spn.cluster_id] = eligible_locations
        return cluster_locations

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
        cluster_locations: dict[str, dict],
    ):
        self._backend = backend
        self._ws = ws
        self._installation = installation
        self._tables_crawler = tables_crawler
        self._mounts_crawler = mounts_crawler
        self._cluster_locations = cluster_locations

    def get_interactive_cluster_grants(self) -> list[Grant]:
        tables = self._tables_crawler.snapshot()
        mounts = list(self._mounts_crawler.snapshot())
        grants: set[Grant] = set()

        for cluster_id, locations in self._cluster_locations.items():
            principals = self._get_cluster_principal_mapping(cluster_id)
            if len(principals) == 0:
                continue
            cluster_usage = self._get_grants(locations, principals, tables, mounts)
            grants.update(cluster_usage)
        return list(grants)

    def _get_privilege(self, table: Table, locations: dict[str, str], mounts: list[Mount]):
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
        return [
            Grant(principal, "USAGE", "hive_metastore", database) for database in databases for principal in principals
        ]

    def _get_grants(
        self, locations: dict[str, str], principals: list[str], tables: list[Table], mounts: list[Mount]
    ) -> list[Grant]:
        grants = []
        filtered_tables = []
        for table in tables:
            privilege = self._get_privilege(table, locations, mounts)
            if privilege == "READ_FILES":
                grants.extend(
                    [Grant(principal, "SELECT", table.catalog, table.database, table.name) for principal in principals]
                )
                filtered_tables.append(table)
                continue
            if privilege == "WRITE_FILES":
                grants.extend(
                    [
                        Grant(principal, "ALL PRIVILEGES", table.catalog, table.database, table.name)
                        for principal in principals
                    ]
                )
                filtered_tables.append(table)
                continue
            if table.view_text is not None:
                grants.extend(
                    [
                        Grant(principal, "ALL PRIVILEGES", table.catalog, table.database, view=table.name)
                        for principal in principals
                    ]
                )
                filtered_tables.append(table)

        database_grants = self._get_database_grants(filtered_tables, principals)

        grants.extend(database_grants)

        return grants

    def _get_cluster_principal_mapping(self, cluster_id: str) -> list[str]:
        # gets all the users,groups,spn which have access to the clusters and returns a dataclass of that mapping
        principal_list = []
        try:
            cluster_permission = self._ws.permissions.get("clusters", cluster_id)
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
        for cluster_id, locations in self._cluster_locations.items():
            # get interactive cluster users
            principals = self._get_cluster_principal_mapping(cluster_id)
            if len(principals) == 0:
                continue
            for location_url in locations.keys():
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
