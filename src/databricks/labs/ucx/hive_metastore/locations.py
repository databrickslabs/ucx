import dataclasses
import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar, Optional
from urllib.parse import urlparse, ParseResult

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import ExternalLocationInfo
from databricks.sdk.dbutils import FileInfo
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler

logger = logging.getLogger(__name__)

_EXTERNAL_FILE_LOCATION_SCHEMES = ("s3", "s3a", "s3n", "gcs", "abfss", "wasbs")


@dataclass
class ExternalLocation:
    location: str
    table_count: int


# Union with string is invalid syntax: "LocationTrie" | None
OptionalLocationTrie = Optional["LocationTrie"]  # pylint: disable=consider-alternative-union-syntax


@dataclass
class LocationTrie:
    """A trie datastructure to search locations.

    Used to find overlapping locations.
    """

    key: str = ""
    parent: OptionalLocationTrie = dataclasses.field(repr=False, default=None)
    children: dict[str, "LocationTrie"] = dataclasses.field(default_factory=dict)
    tables: list[Table] = dataclasses.field(repr=False, default_factory=list)

    @cached_property
    def _path(self) -> list[str]:
        """The path to traverse to get to the current node."""
        parts = []
        current: LocationTrie | None = self
        while current:
            parts.append(current.key)
            current = current.parent
        return list(reversed(parts))[1:]

    @property
    def location(self) -> str | None:
        if not self.is_valid():
            return None
        try:
            scheme, netloc, *path = self._path
            return f"{scheme}://{netloc}/{'/'.join(path)}".rstrip("/")
        except ValueError:
            return None

    @classmethod
    def _parse_location(cls, location: str | None) -> list[str]:
        if not location:
            return []
        location = ExternalLocations.clean_location(location)
        parse_result = cls._parse_url(location)
        if not parse_result:
            return []
        parts = [parse_result.scheme, parse_result.netloc]
        for part in parse_result.path.split("/"):
            if not part:
                continue  # remove empty strings
            parts.append(part)
        return parts

    @staticmethod
    def _parse_url(location: str) -> ParseResult | None:
        parse_result = urlparse(location)
        if parse_result.scheme == 'jdbc':
            jdbc_path = parse_result.path.split('://')
            if len(jdbc_path) != 2:
                return None
            netloc, path = jdbc_path[1].split('/', 1)
            parse_result = ParseResult(
                scheme=f'{parse_result.scheme}:{jdbc_path[0]}',
                netloc=netloc,
                path=path,
                params='',
                query='',
                fragment='',
            )
        return parse_result

    def insert(self, table: Table) -> None:
        current = self
        for part in self._parse_location(table.location):
            if part not in current.children:
                parent = current
                current = LocationTrie(part, parent)
                parent.children[part] = current
                continue
            current = current.children[part]
        current.tables.append(table)

    def find(self, table: Table) -> OptionalLocationTrie:
        current = self
        for part in self._parse_location(table.location):
            if part not in current.children:
                return None
            current = current.children[part]
        return current

    def is_valid(self) -> bool:
        """A valid location has a scheme and netloc; the path is optional."""
        if len(self._path) < 2:
            return False
        scheme, netloc, *_ = self._path
        if scheme.startswith('jdbc:') and len(netloc) > 0:
            return True
        return scheme in _EXTERNAL_FILE_LOCATION_SCHEMES and len(netloc) > 0

    def is_jdbc(self) -> bool:
        if not self.is_valid():
            return False
        return self._path[0].startswith('jdbc:')

    def all_tables(self) -> Iterable[Table]:
        for node in self:
            yield from node.tables

    def has_children(self):
        return len(self.children) > 0

    def __iter__(self):
        if self.is_valid():
            yield self
        for child in self.children.values():
            yield from child


class ExternalLocations(CrawlerBase[ExternalLocation]):
    _prefix_size: ClassVar[list[int]] = [1, 12]

    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        schema: str,
        tables_crawler: TablesCrawler,
        mounts_crawler: 'MountsCrawler',
        *,
        enable_hms_federation: bool = False,
    ):
        super().__init__(sql_backend, "hive_metastore", schema, "external_locations", ExternalLocation)
        self._ws = ws
        self._tables_crawler = tables_crawler
        self._mounts_crawler = mounts_crawler
        self._enable_hms_federation = enable_hms_federation

    @cached_property
    def _mounts_snapshot(self) -> list['Mount']:
        """Returns all mounts, sorted by longest prefixes first."""
        return sorted(self._mounts_crawler.snapshot(), key=lambda _: (len(_.name), _.name), reverse=True)

    @staticmethod
    def clean_location(location: str) -> str:
        # remove the s3a scheme and replace it with s3 as these can be considered the same and will be treated as such
        # Having s3a and s3 as separate locations will cause issues when trying to find overlapping locations
        return re.sub(r"^s3a:/", r"s3:/", location).rstrip("/")

    def external_locations_with_root(self) -> Iterable[ExternalLocation]:
        """
        Produces a list of external locations with the DBFS root location appended to the list.
        Utilizes the snapshot method.
        Used for HMS Federation.

        Yields:
                Iterable[Result]: Combination of all the external locations and the DBFS root location
        """

        yield from self.snapshot()
        dbfs_root = self._get_dbfs_root()
        if dbfs_root:
            yield dbfs_root

    def _get_dbfs_root(self) -> ExternalLocation | None:
        """
        Get the root location of the DBFS only if HMS Fed is enabled.
        Utilizes an undocumented Databricks API call

        Returns:
            Cloud storage root location for dbfs

        """
        if not self._enable_hms_federation:
            return None
        logger.debug("Retrieving DBFS root location")
        try:
            response = self._ws.api_client.do("GET", "/api/2.0/dbfs/resolve-path", query={"path": "dbfs:/"})
            if isinstance(response, dict):
                resolved_path = response.get("resolved_path")
                if resolved_path:
                    path = f"{self.clean_location(resolved_path)}/user/hive/warehouse"
                    return ExternalLocation(path, 0)
        except NotFound:
            # Couldn't retrieve the DBFS root location
            logger.warning("DBFS root location not found")
            return None
        return None

    @staticmethod
    def wasbs_to_abfss(location: str | None) -> str | None:
        """
        Converts a wasbs:// location to abfss://
        """
        if not location:
            return None
        try:
            parsed = urlparse(location)
        except ValueError as e:
            logger.warning(f"Invalid URL format for location {location}: {e}")
            return location
        if parsed.scheme == "wasbs":
            return f"abfss://{parsed.netloc.replace('.blob.core.windows.net','.dfs.core.windows.net')}{parsed.path}"
        return location

    def _external_locations(self) -> Iterable[ExternalLocation]:
        trie = LocationTrie()
        for table in self._tables_crawler.snapshot():
            table = self._resolve_location(table)
            if not table.location:
                continue
            trie.insert(table)
        queue = list(trie.children.values())
        external_locations = []
        while queue:
            curr = queue.pop()
            num_children = len(curr.children)  # 0 - take parent
            if curr.location and (num_children > 1 or num_children == 0):
                # Checking if the parent location is a valid location for external location.
                # If the table location is a leaf location (a foldermore than 2 levels from the root)
                # the parent folder will be considered as external location.
                # If the table location is a root location (a folder at the root level) it will be considered as
                # external location.

                if (
                    curr.parent and curr.parent.is_valid() and num_children == 0 and not curr.is_jdbc()
                ):  # one table having the prefix
                    curr = curr.parent
                if not curr.location:
                    continue
                external_location = ExternalLocation(curr.location, len(list(curr.all_tables())))
                external_locations.append(external_location)
                continue
            queue.extend(curr.children.values())
        return sorted(external_locations, key=lambda _: _.location)

    def _resolve_location(self, table: Table) -> Table:
        location = table.location
        if not location:
            return table
        location = self._resolve_jdbc(table)
        location = self.resolve_mount(location)
        location = self.wasbs_to_abfss(location)
        return dataclasses.replace(table, location=location)

    def resolve_mount(self, location: str | None) -> str | None:
        if not location:
            return None
        if location.startswith('/dbfs'):
            location = 'dbfs:' + location[5:]  # convert FUSE path to DBFS path
        if not location.startswith('dbfs:'):
            return location  # not a mount, save some cycles
        for mount in self._mounts_snapshot:
            prefix = mount.as_scheme_prefix()
            if not location.startswith(prefix):
                continue
            logger.debug(f"Replacing location {prefix} with {mount.source} in {location}")
            location = location.replace(prefix, mount.source)
            return location
        logger.debug(f"Mount not found for location {location}. Skipping replacement.")
        return location

    def _resolve_jdbc(self, table: Table) -> str | None:
        location = table.location
        if not location or not table.storage_properties or not location.startswith('jdbc:'):
            return location
        pattern = r"(\w+)=(.*?)(?=\s*,|\s*\])"
        # Find all matches in the input string
        # Storage properties is of the format
        # "personalAccessToken=*********(redacted),
        #  httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be, host=dbc-test1-aa11.cloud.databricks.com,
        #  dbtable=samples.nyctaxi.trips"
        matches = re.findall(pattern, table.storage_properties)
        # Create a dictionary from the matches
        result_dict = dict(matches)
        # Fetch the value of host from the newly created dict
        host = result_dict.get("host", "")
        port = result_dict.get("port", "")
        database = result_dict.get("database", "")
        httppath = result_dict.get("httpPath", "")
        provider = result_dict.get("provider", "")
        # currently supporting databricks and mysql external tables
        # add other jdbc types
        if "databricks" in location.lower():
            return f"jdbc:databricks://{host};httpPath={httppath}"
        if "mysql" in location.lower():
            return f"jdbc:mysql://{host}:{port}/{database}"
        if not provider == "":
            return f"jdbc:{provider.lower()}://{host}:{port}/{database}"
        return f"{location.lower()}/{host}:{port}/{database}"

    def _crawl(self) -> Iterable[ExternalLocation]:
        return self._external_locations()

    def _try_fetch(self) -> Iterable[ExternalLocation]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield ExternalLocation(*row)

    @staticmethod
    def _get_ext_location_definitions(missing_locations: list[ExternalLocation]) -> list:
        tf_script = []
        cnt = 1
        res_name = ""
        for loc in missing_locations:
            for scheme in _EXTERNAL_FILE_LOCATION_SCHEMES:
                prefix = f"{scheme}://"
                prefix_len = len(prefix)
                if not loc.location.startswith(prefix):
                    continue
                if prefix == "abfss://":
                    container_sep_loc = loc.location.index("@")
                    container_name = loc.location[prefix_len:container_sep_loc]
                    res_name = (
                        loc.location[container_sep_loc + 1 :]
                        .replace(".dfs.core.windows.net", "")
                        .rstrip("/")
                        .replace("/", "_")
                    )
                    res_name = f"{container_name}_{res_name}"
                else:
                    res_name = loc.location[prefix_len:].rstrip("/").replace("/", "_")
            if res_name == "":
                # if the cloud storage url doesn't match the above condition or incorrect
                # dont generate tf script and ignore
                logger.warning(f"unsupported storage format {loc.location}")
                continue
            script = f'resource "databricks_external_location" "{res_name}" {{ \n'
            script += f'    name = "{res_name}"\n'
            script += f'    url  = "{loc.location.rstrip("/")}"\n'
            script += "    credential_name = databricks_storage_credential.<storage_credential_reference>.id\n"
            script += "}\n"
            tf_script.append(script)
            cnt += 1
        return tf_script

    def match_table_external_locations(self) -> tuple[dict[str, int], list[ExternalLocation]]:
        existing_locations = list(self._ws.external_locations.list())
        table_locations = self.snapshot()
        matching_locations: dict[str, int] = {}
        missing_locations = []
        for table_loc in table_locations:
            # external_location.list returns url without trailing "/" but ExternalLocation.snapshot
            # does so removing the trailing slash before comparing
            if not self._match_existing(table_loc, matching_locations, existing_locations):
                missing_locations.append(table_loc)
        return matching_locations, missing_locations

    @staticmethod
    def _match_existing(table_loc, matching_locations: dict[str, int], existing_locations: list[ExternalLocationInfo]):
        for uc_loc in existing_locations:
            if not uc_loc.url:
                continue
            if not uc_loc.name:
                continue
            uc_loc_path = uc_loc.url.lower()
            if uc_loc_path in table_loc.location.rstrip("/").lower():
                if uc_loc.name not in matching_locations:
                    matching_locations[uc_loc.name] = table_loc.table_count
                else:
                    matching_locations[uc_loc.name] = matching_locations[uc_loc.name] + table_loc.table_count
                return True
        return False

    def save_as_terraform_definitions_on_workspace(self, installation: Installation):
        matching_locations, missing_locations = self.match_table_external_locations()
        if len(matching_locations) > 0:
            logger.info("following external locations are already configured.")
            logger.info("sharing details of # tables that can be migrated for each location")
            for location, table_count in matching_locations.items():
                logger.info(f"{table_count} tables can be migrated using UC external location: {location}.")
        if len(missing_locations) > 0:
            logger.info("following external location need to be created.")
            for _ in missing_locations:
                logger.info(f"{_.table_count} tables can be migrated using external location {_.location}.")
            buffer = []
            for script in self._get_ext_location_definitions(missing_locations):
                buffer.append(script)
            return installation.upload('external_locations.tf', ("\n".join(buffer)).encode('utf8'))
        logger.info("no additional external location to be created.")
        return None


@dataclass(unsafe_hash=True)
class Mount:
    name: str
    source: str

    def as_scheme_prefix(self) -> str:
        return f'dbfs:{self.name}'  # dbfs:/mnt/mount-name


class MountsCrawler(CrawlerBase[Mount]):
    def __init__(
        self,
        sql_backend: SqlBackend,
        ws: WorkspaceClient,
        inventory_database: str,
    ):
        super().__init__(sql_backend, "hive_metastore", inventory_database, "mounts", Mount)
        self._dbutils = ws.dbutils

    @staticmethod
    def _deduplicate_mounts(mounts: list) -> list:
        seen = set()
        deduplicated_mounts = []
        for obj in mounts:
            if "dbfsreserved" in obj.source.lower():
                obj_tuple = Mount("/Volume", obj.source)
            else:
                obj_tuple = Mount(obj.name, obj.source)
            if obj_tuple not in seen:
                seen.add(obj_tuple)
                deduplicated_mounts.append(obj)
        return deduplicated_mounts

    def _crawl(self) -> Iterable[Mount]:
        mounts = []
        try:
            for mount_point, source, _ in self._dbutils.fs.mounts():
                mounts.append(Mount(mount_point, source))
        except Exception as error:  # pylint: disable=broad-except
            if "com.databricks.backend.daemon.dbutils.DBUtilsCore.mounts() is not whitelisted" in str(error):
                logger.warning(
                    "dbutils.fs.mounts() is not whitelisted. Skipping mount point discovery."
                    "Please make sure you run the assessment workflow."
                )
        return self._deduplicate_mounts(mounts)

    def _try_fetch(self) -> Iterable[Mount]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Mount(*row)


@dataclass
class TableInMount:
    format: str
    is_partitioned: bool

    def __post_init__(self) -> None:
        if isinstance(self.format, str):  # Should not happen according to type hint, still safer
            self.format = self.format.upper()


class TablesInMounts(CrawlerBase[Table]):
    """Experimental scanner for tables that can be found on mounts.

    This crawler was developed with a specific use-case in mind and isn't currently in use. It does not conform to the
    design of other crawlers. In particular:
     - It depends on the `tables` inventory, but without verifying the tables crawler has run.
     - Rather than have its own table it will blindly overwrite the existing content of the tables inventory.
    """

    TABLE_IN_MOUNT_DB = "mounted_"

    def __init__(
        self,
        sql_backend: SqlBackend,
        ws: WorkspaceClient,
        inventory_database: str,
        mounts_crawler: MountsCrawler,
        include_mounts: list[str] | None = None,
        exclude_paths_in_mount: list[str] | None = None,
        include_paths_in_mount: list[str] | None = None,
    ):
        super().__init__(sql_backend, "hive_metastore", inventory_database, "tables", Table)
        self._dbutils = ws.dbutils
        self._mounts_crawler = mounts_crawler
        self._include_mounts = include_mounts
        self._ws = ws
        self._include_paths_in_mount = include_paths_in_mount

        irrelevant_patterns = {'_SUCCESS', '_committed_', '_started_'}
        if exclude_paths_in_mount:
            irrelevant_patterns.update(exclude_paths_in_mount)
        self._fiter_paths = irrelevant_patterns

    def snapshot(self, *, force_refresh: bool = False) -> list[Table]:
        if not force_refresh:
            msg = "This crawler only supports forced refresh; refer to source implementation for details."
            raise NotImplementedError(msg)
        return list(super().snapshot(force_refresh=force_refresh))

    def _crawl(self) -> list[Table]:
        logger.debug(f"[{self.full_name}] fetching {self._table} inventory")
        try:
            cached_results = list(self._try_fetch())
        except NotFound:
            # This happens when the table crawler hasn't run yet, and is arguably incorrect:
            # rather than pretending there are no tables it should instead trigger a crawl.
            cached_results = []
        table_paths = self._get_tables_paths_from_assessment(cached_results)
        logger.debug(f"[{self.full_name}] crawling new batch for {self._table}")
        loaded_records = list(self._crawl_tables(table_paths))
        if cached_results:
            loaded_records = [*loaded_records, *cached_results]
        return loaded_records

    def _try_fetch(self) -> Iterable[Table]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(
            f"SELECT * FROM {escape_sql_identifier(self.full_name)} WHERE NOT STARTSWITH(database, '{self.TABLE_IN_MOUNT_DB}')"
        ):
            yield Table(*row)

    @staticmethod
    def _get_tables_paths_from_assessment(loaded_records: Iterable[Table]) -> dict[str, str]:
        seen = {}
        for rec in loaded_records:
            if not rec.location:
                continue
            seen[rec.location] = rec.key
        return seen

    def _crawl_tables(self, table_paths_from_assessment: dict[str, str]) -> list[Table]:
        all_mounts = self._mounts_crawler.snapshot()
        all_tables = []
        for mount in all_mounts:
            if self._include_mounts and mount.name not in self._include_mounts:
                logger.info(f"Filtering mount {mount.name}")
                continue
            if self._include_paths_in_mount:
                table_paths = {}
                for path in self._include_paths_in_mount:
                    table_paths.update(self._find_delta_log_folders(path))
            else:
                table_paths = self._find_delta_log_folders(mount.name)

            for path, entry in table_paths.items():
                guess_table = os.path.basename(path)
                table_location = self._get_table_location(mount, path)

                # A table in mount may have already been identified by the assessment job because they're on the current workspace HMS
                # We filter those tables as we give better support to migrate those tables to UC
                if table_location in table_paths_from_assessment:
                    logger.info(
                        f"Path {path} is identified as a table in mount, but is present in current workspace as a registered table {table_paths_from_assessment[table_location]}"
                    )
                    continue
                if path in table_paths_from_assessment:
                    logger.info(
                        f"Path {path} is identified as a table in mount, but is present in current workspace as a registered table {table_paths_from_assessment[path]}"
                    )
                    continue
                table = Table(
                    catalog="hive_metastore",
                    database=f"{self.TABLE_IN_MOUNT_DB}{mount.name.replace('/mnt/', '').replace('/', '_')}",
                    name=guess_table,
                    object_type="EXTERNAL",
                    table_format=entry.format,
                    location=table_location,
                    is_partitioned=entry.is_partitioned,
                )
                all_tables.append(table)
        logger.info(f"Found a total of {len(all_tables)} tables in mount points")
        return all_tables

    @staticmethod
    def _get_table_location(mount: Mount, path: str) -> str:
        """
        There can be different cases for mounts:
            - Mount(name='/mnt/things/a', source='abfss://things@labsazurethings.dfs.core.windows.net/a')
            - Mount(name='/mnt/mount' source='abfss://container@dsss.net/')
            Both must return the complete source with a forward slash in the end
        """
        if mount.source.endswith("/"):
            return path.replace(f"dbfs:{mount.name}/", mount.source)
        return path.replace(f"dbfs:{mount.name}", mount.source)

    def _find_delta_log_folders(
        self,
        root_dir: str,
        delta_log_folders: dict[str, TableInMount] | None = None,
    ) -> dict[str, TableInMount]:
        if delta_log_folders is None:
            delta_log_folders = {}
        logger.info(f"Listing {root_dir}")
        file_infos = self._dbutils.fs.ls(root_dir) or []
        for file_info in file_infos:
            if self._is_irrelevant(file_info.name) or file_info.path == root_dir:
                logger.debug(f"Path {file_info.path} is irrelevant")
                continue

            root_path = os.path.dirname(root_dir)
            parent_entry = delta_log_folders.get(root_path)
            table_in_mount = self._assess_path(file_info)

            if parent_entry:
                # if the root_path was already found to be partitioned, we can break to reduce redundant ops
                if delta_log_folders[root_path] == TableInMount(format="DELTA", is_partitioned=True):
                    break

                if (
                    # Happens when first folder was _delta_log and next folders are partitioned folder
                    parent_entry.format == "DELTA"
                    and self._is_partitioned(file_info.name)
                ) or (
                    # Happens when previous entries where partitioned folders and the current one is delta_log
                    parent_entry.is_partitioned
                    and table_in_mount
                    and table_in_mount.format == "DELTA"
                ):
                    delta_log_folders[root_path] = TableInMount(format="DELTA", is_partitioned=True)
                    logger.debug(f"Added DELTA table for {root_path} (partitioned delta)")

                self._recurse_dir_if_needed(file_info, delta_log_folders)

            elif self._is_partitioned(file_info.name):
                self._handle_partitioned_file(file_info.path, root_path, delta_log_folders)

            elif not table_in_mount:
                self._recurse_dir_if_needed(file_info, delta_log_folders)

            elif table_in_mount.format == "DELTA" and file_info.name == "_delta_log/":
                delta_log_folders[root_path] = table_in_mount
                logger.debug(f"Added {table_in_mount.format} table for {root_path} (normal delta)")

            else:
                delta_log_folders[root_path] = table_in_mount
                logger.debug(f"Added {table_in_mount.format} table for {root_path} (general)")
                self._recurse_dir_if_needed(file_info, delta_log_folders)

        return delta_log_folders

    def _recurse_dir_if_needed(self, file_info: FileInfo, delta_log_folders: dict[str, TableInMount]) -> None:
        if self._is_recursible_dir(file_info):
            self._find_delta_log_folders(file_info.path, delta_log_folders)

    def _handle_partitioned_file(self, file_path, root_path, delta_log_folders):
        partition_format = self._find_partition_file_format(file_path)
        if partition_format:
            delta_log_folders[root_path] = partition_format
            logger.debug(f"Added {partition_format.format} table for {root_path} (partitioned)")

    def _find_partition_file_format(self, root_dir: str) -> TableInMount | None:
        logger.info(f"Listing partitioned file {root_dir}")
        file_infos = self._dbutils.fs.ls(root_dir)
        for file_info in file_infos:
            path_extension = self._assess_path(file_info)
            if path_extension:
                return TableInMount(format=path_extension.format, is_partitioned=True)
            if self._is_partitioned(file_info.name) and file_info.path != root_dir:
                return self._find_partition_file_format(file_info.path)
        return None

    def _assess_path(self, file_info: FileInfo) -> TableInMount | None:
        if file_info.name == "_delta_log/":
            logger.debug(f"Found delta table {file_info.path}")
            return TableInMount(format="DELTA", is_partitioned=False)
        if self._is_csv(file_info.name):
            logger.debug(f"Found csv {file_info.path}")
            return TableInMount(format="CSV", is_partitioned=False)
        if self._is_json(file_info.name):
            logger.debug(f"Found json {file_info.path}")
            return TableInMount(format="JSON", is_partitioned=False)
        if self._is_parquet(file_info.name):
            logger.debug(f"Found parquet {file_info.path}")
            return TableInMount(format="PARQUET", is_partitioned=False)
        return None

    def _is_recursible_dir(self, file_info: FileInfo) -> bool:
        # Rules for recursing into a folder
        # - should be size 0 (usually a directory)
        # - should not be the _delta_log directory
        # - file should not be partitioned
        # - file name should not start with 'part-'
        # - no brackets, brackets are not allowed in dbutils.fs.ls
        # - is not a streaming checkpoint dir
        return (
            file_info.size == 0
            and file_info.name != "_delta_log/"
            and not self._is_partitioned(file_info.name)
            and not file_info.name.startswith("part-")
            and not any(char in file_info.name for char in "[]")
            and not self._is_streaming_checkpoint_dir(file_info)
        )

    @staticmethod
    def _is_partitioned(file_name: str) -> bool:
        return '=' in file_name

    @staticmethod
    def _is_parquet(file_name: str) -> bool:
        parquet_patterns = {'.parquet'}
        return any(pattern in file_name for pattern in parquet_patterns)

    @staticmethod
    def _is_csv(file_name: str) -> bool:
        csv_patterns = {'.csv'}
        return any(pattern in file_name for pattern in csv_patterns)

    @staticmethod
    def _is_json(file_name: str) -> bool:
        json_patterns = {'.json'}
        return any(pattern in file_name for pattern in json_patterns)

    def _is_irrelevant(self, file_name: str) -> bool:
        return any(pattern in file_name for pattern in self._fiter_paths)

    def _is_streaming_checkpoint_dir(self, file_info: FileInfo) -> bool:
        path_dirs = [file.name for file in self._dbutils.fs.ls(file_info.path) if file.size == 0]
        return 'commits/' in path_dirs and 'offsets/' in path_dirs
