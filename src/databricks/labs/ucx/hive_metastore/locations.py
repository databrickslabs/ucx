import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import ClassVar

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ExternalLocationInfo

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier

logger = logging.getLogger(__name__)


@dataclass
class ExternalLocation:
    location: str
    table_count: int


@dataclass(unsafe_hash=True)
class Mount:
    name: str
    source: str


class ExternalLocations(CrawlerBase[ExternalLocation]):
    _prefix_size: ClassVar[list[int]] = [1, 12]

    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
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
                location = self._resolve_mount(location, mounts)
            if (
                not location.startswith("dbfs")
                and (self._prefix_size[0] < location.find(":/") < self._prefix_size[1])
                and not location.startswith("jdbc")
            ):
                self._dbfs_locations(external_locations, location, min_slash)
            if location.startswith("jdbc"):
                self._add_jdbc_location(external_locations, location, table)
        return external_locations

    def _resolve_mount(self, location, mounts):
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
        # "[personalAccessToken=*********(redacted), \
        #  httpPath=/sql/1.0/warehouses/65b52fb5bd86a7be, host=dbc-test1-aa11.cloud.databricks.com, \
        #  dbtable=samples.nyctaxi.trips]"
        matches = re.findall(pattern, table.storage_properties)
        # Create a dictionary from the matches
        result_dict = dict(matches)
        # Fetch the value of host from the newly created dict
        host = result_dict.get("host", "")
        port = result_dict.get("port", "")
        database = result_dict.get("database", "")
        httppath = result_dict.get("httpPath", "")
        provider = result_dict.get("provider", "")
        # dbtable = result_dict.get("dbtable", "")
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

    def _external_location_list(self) -> Iterable[ExternalLocation]:
        tables = list(
            self._backend.fetch(
                f"SELECT location, storage_properties FROM {escape_sql_identifier(self._schema)}.tables WHERE location IS NOT NULL"
            )
        )
        mounts = Mounts(self._backend, self._ws, self._schema).snapshot()
        return self._external_locations(list(tables), list(mounts))

    def snapshot(self) -> Iterable[ExternalLocation]:
        return self._snapshot(self._try_fetch, self._external_location_list)

    def _try_fetch(self) -> Iterable[ExternalLocation]:
        for row in self._fetch(
            f"SELECT * FROM {escape_sql_identifier(self._schema)}.{escape_sql_identifier(self._table)}"
        ):
            yield ExternalLocation(*row)

    def _get_ext_location_definitions(self, missing_locations: list[ExternalLocation]) -> list:
        tf_script = []
        cnt = 1
        for loc in missing_locations:
            if loc.location.startswith("s3://"):
                res_name = loc.location[5:].rstrip("/").replace("/", "_")
            elif loc.location.startswith("gcs://"):
                res_name = loc.location[6:].rstrip("/").replace("/", "_")
            elif loc.location.startswith("abfss://"):
                container_name = loc.location[8 : loc.location.index("@")]
                res_name = (
                    loc.location[loc.location.index("@") + 1 :]
                    .replace(".dfs.core.windows.net", "")
                    .rstrip("/")
                    .replace("/", "_")
                )
                res_name = f"{container_name}_{res_name}"
            else:
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

    def _deduplicate_mounts(self, mounts: list) -> list:
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

    def inventorize_mounts(self):
        self._append_records(self._list_mounts())

    def _list_mounts(self) -> Iterable[Mount]:
        mounts = []
        for mount_point, source, _ in self._dbutils.fs.mounts():
            mounts.append(Mount(mount_point, source))
        return self._deduplicate_mounts(mounts)

    def snapshot(self) -> Iterable[Mount]:
        return self._snapshot(self._try_fetch, self._list_mounts)

    def _try_fetch(self) -> Iterable[Mount]:
        for row in self._fetch(
            f"SELECT * FROM {escape_sql_identifier(self._schema)}.{escape_sql_identifier(self._table)}"
        ):
            yield Mount(*row)
