import dataclasses
import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar, Optional
from urllib.parse import urlparse

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import ExternalLocationInfo
from databricks.sdk.dbutils import FileInfo
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table

logger = logging.getLogger(__name__)


_EXTERNAL_FILE_LOCATION_SCHEMES = ("s3", "s3a", "s3n", "gcs", "abfss")


@dataclass
class ExternalLocation:
    location: str
    table_count: int


@dataclass(unsafe_hash=True)
class Mount:
    name: str
    source: str


# Union with string is invalid syntax: "LocationTrie" | None
OptionalLocationTrie = Optional["LocationTrie"]  # pylint: disable=consider-alternative-union-syntax


@dataclass
class LocationTrie:
    """A trie datastructure to search locations.

    Used to find overlapping locations.
    """

    key: str = ""
    parent: OptionalLocationTrie = None
    children: dict[str, "LocationTrie"] = dataclasses.field(default_factory=dict)
    tables: list[Table] = dataclasses.field(default_factory=list)

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
    def location(self):
        scheme, netloc, *path = self._path
        return f"{scheme}://{netloc}/{'/'.join(path)}"

    @staticmethod
    def _parse_location(location: str | None) -> list[str]:
        if not location:
            return []
        parse_result = urlparse(location)
        parts = [parse_result.scheme, parse_result.netloc]
        parts.extend(parse_result.path.strip("/").split("/"))
        return parts

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
        if len(self._path) < 3:
            return False
        scheme, netloc, *_ = self._path
        return scheme in _EXTERNAL_FILE_LOCATION_SCHEMES and len(netloc) > 0

    def has_children(self):
        return len(self.children) > 0

    def __iter__(self):
        if self.is_valid():
            yield self
        for child in self.children.values():
            yield from child


class ExternalLocations(CrawlerBase[ExternalLocation]):
    _prefix_size: ClassVar[list[int]] = [1, 12]

    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema: str):
        super().__init__(sbe, "hive_metastore", schema, "external_locations", ExternalLocation)
        self._ws = ws

    def _external_locations(self, tables: list[Row], mounts) -> Iterable[ExternalLocation]:
        min_slash = 2
        external_locations: list[ExternalLocation] = []
        for table in tables:
            location = table.location
            if not location:
                continue
            if location.startswith("dbfs:/mnt"):
                location = self.resolve_mount(location, mounts)
            if (
                not location.startswith("dbfs")
                and (self._prefix_size[0] < location.find(":/") < self._prefix_size[1])
                and not location.startswith("jdbc")
            ):
                self._dbfs_locations(external_locations, location, min_slash)
            if location.startswith("jdbc"):
                self._add_jdbc_location(external_locations, location, table)
        return external_locations

    @staticmethod
    def resolve_mount(location, mounts):
        for mount in mounts:
            if location[5:].startswith(mount.name.lower()):
                location = location[5:].replace(mount.name, mount.source)
                break
        return location

    @staticmethod
    def _dbfs_locations(external_locations, location, min_slash):
        dupe = False
        loc = 0
        while loc < len(external_locations) and not dupe:
            common = (
                os.path.commonpath([external_locations[loc].location, os.path.dirname(location) + "/"]).replace(
                    ":/", "://"
                )
                + "/"
            )
            if common.count("/") > min_slash:
                table_count = external_locations[loc].table_count
                external_locations[loc] = ExternalLocation(common, table_count + 1)
                dupe = True
            loc += 1
        if not dupe:
            external_locations.append(ExternalLocation(os.path.dirname(location) + "/", 1))

    def _add_jdbc_location(self, external_locations, location, table):
        dupe = False
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
            jdbc_location = f"jdbc:databricks://{host};httpPath={httppath}"
        elif "mysql" in location.lower():
            jdbc_location = f"jdbc:mysql://{host}:{port}/{database}"
        elif not provider == "":
            jdbc_location = f"jdbc:{provider.lower()}://{host}:{port}/{database}"
        else:
            jdbc_location = f"{location.lower()}/{host}:{port}/{database}"
        for ext_loc in external_locations:
            if ext_loc.location == jdbc_location:
                ext_loc.table_count += 1
                dupe = True
                break
        if not dupe:
            external_locations.append(ExternalLocation(jdbc_location, 1))

    def _crawl(self) -> Iterable[ExternalLocation]:
        tables_identifier = escape_sql_identifier(f"{self._catalog}.{self._schema}.tables")
        tables = list(
            self._backend.fetch(
                f"SELECT location, storage_properties FROM {tables_identifier} WHERE location IS NOT NULL"
            )
        )
        mounts = Mounts(self._backend, self._ws, self._schema).snapshot()
        return self._external_locations(list(tables), list(mounts))

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
                # if the cloud storage url doesn't match the above condition or incorrect (example wasb://)
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


class Mounts(CrawlerBase[Mount]):
    def __init__(self, backend: SqlBackend, ws: WorkspaceClient, inventory_database: str):
        super().__init__(backend, "hive_metastore", inventory_database, "mounts", Mount)
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
        for mount_point, source, _ in self._dbutils.fs.mounts():
            mounts.append(Mount(mount_point, source))
        return self._deduplicate_mounts(mounts)

    def _try_fetch(self) -> Iterable[Mount]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Mount(*row)


@dataclass
class TableInMount:
    format: str
    is_partitioned: bool


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
        backend: SqlBackend,
        ws: WorkspaceClient,
        inventory_database: str,
        mc: Mounts,
        include_mounts: list[str] | None = None,
        exclude_paths_in_mount: list[str] | None = None,
        include_paths_in_mount: list[str] | None = None,
    ):
        super().__init__(backend, "hive_metastore", inventory_database, "tables", Table)
        self._dbutils = ws.dbutils
        self._mounts_crawler = mc
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
        file_infos = self._dbutils.fs.ls(root_dir)
        for file_info in file_infos:
            if self._is_irrelevant(file_info.name) or file_info.path == root_dir:
                logger.debug(f"Path {file_info.path} is irrelevant")
                continue

            root_path = os.path.dirname(root_dir)
            previous_entry = delta_log_folders.get(root_path)
            table_in_mount = self._assess_path(file_info)

            if previous_entry:
                # Happens when first folder was _delta_log and next folders are partitioned folder
                if previous_entry.format == "DELTA" and self._is_partitioned(file_info.name):
                    delta_log_folders[root_path] = TableInMount(format=previous_entry.format, is_partitioned=True)
                # Happens when previous entries where partitioned folders and the current one is delta_log
                if previous_entry.is_partitioned and table_in_mount and table_in_mount.format == "DELTA":
                    delta_log_folders[root_path] = TableInMount(format=table_in_mount.format, is_partitioned=True)
                continue

            if self._is_partitioned(file_info.name):
                partition_format = self._find_partition_file_format(file_info.path)
                if partition_format:
                    delta_log_folders[root_path] = partition_format
                continue

            if not table_in_mount:
                self._find_delta_log_folders(file_info.path, delta_log_folders)
                continue

            delta_log_folders[root_path] = table_in_mount
        return delta_log_folders

    def _find_partition_file_format(self, root_dir: str) -> TableInMount | None:
        logger.info(f"Listing {root_dir}")
        file_infos = self._dbutils.fs.ls(root_dir)
        for file_info in file_infos:
            path_extension = self._assess_path(file_info)
            if path_extension:
                return TableInMount(format=path_extension.format, is_partitioned=True)
            if self._is_partitioned(file_info.name):
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
