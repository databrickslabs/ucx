import io
import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import ClassVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.mixins.sql import Row

logger = logging.getLogger(__name__)


@dataclass
class ExternalLocation:
    location: str
    table_count: int


@dataclass
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
            if location is not None and len(location) > 0:
                if location.startswith("dbfs:/mnt"):
                    for mount in mounts:
                        if location[5:].startswith(mount.name.lower()):
                            location = location[5:].replace(mount.name, mount.source)
                            break
                if (
                    not location.startswith("dbfs")
                    and (self._prefix_size[0] < location.find(":/") < self._prefix_size[1])
                    and not location.startswith("jdbc")
                ):
                    dupe = False
                    loc = 0
                    while loc < len(external_locations) and not dupe:
                        common = (
                            os.path.commonpath(
                                [external_locations[loc].location, os.path.dirname(location) + "/"]
                            ).replace(":/", "://")
                            + "/"
                        )
                        if common.count("/") > min_slash:
                            table_count = external_locations[loc].table_count
                            external_locations[loc] = ExternalLocation(common, table_count + 1)
                            dupe = True
                        loc += 1
                    if not dupe:
                        external_locations.append(ExternalLocation(os.path.dirname(location) + "/", 1))
                if location.startswith("jdbc"):
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

        return external_locations

    def _external_location_list(self) -> Iterable[ExternalLocation]:
        tables = list(
            self._backend.fetch(
                f"SELECT location, storage_properties FROM {self._schema}.tables WHERE location IS NOT NULL"
            )
        )
        mounts = Mounts(self._backend, self._ws, self._schema).snapshot()
        return self._external_locations(list(tables), list(mounts))

    def snapshot(self) -> Iterable[ExternalLocation]:
        return self._snapshot(self._try_fetch, self._external_location_list)

    def _try_fetch(self) -> Iterable[ExternalLocation]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
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

    def _match_table_external_locations(self) -> tuple[list[list], list[ExternalLocation]]:
        external_locations = list(self._ws.external_locations.list())
        location_path = [_.url.lower() for _ in external_locations]
        table_locations = self.snapshot()
        matching_locations = []
        missing_locations = []
        for loc in table_locations:
            # external_location.list returns url without trailing "/" but ExternalLocation.snapshot
            # does so removing the trailing slash before comparing
            if loc.location.rstrip("/").lower() in location_path:
                # identify the index of the matching external_locations
                iloc = location_path.index(loc.location.rstrip("/"))
                matching_locations.append([external_locations[iloc].name, loc.table_count])
                continue
            missing_locations.append(loc)
        return matching_locations, missing_locations

    def save_as_terraform_definitions_on_workspace(self, folder: str) -> str | None:
        matching_locations, missing_locations = self._match_table_external_locations()
        if len(matching_locations) > 0:
            logger.info("following external locations are already configured.")
            logger.info("sharing details of # tables that can be migrated for each location")
            for _ in matching_locations:
                logger.info(f"{_[1]} tables can be migrated using external location {_[0]}.")
        if len(missing_locations) > 0:
            logger.info("following external location need to be created.")
            for _ in missing_locations:
                logger.info(f"{_.table_count} tables can be migrated using external location {_.location}.")
            buffer = io.StringIO()
            for script in self._get_ext_location_definitions(missing_locations):
                buffer.write(script)
            buffer.seek(0)
            return self._overwrite_mapping(folder, buffer)
        logger.info("no additional external location to be created.")
        return None

    def _overwrite_mapping(self, folder, buffer) -> str:
        path = f"{folder}/external_locations.tf"
        self._ws.workspace.upload(path, buffer, overwrite=True, format=ImportFormat.AUTO)
        return path


class Mounts(CrawlerBase[Mount]):
    def __init__(self, backend: SqlBackend, ws: WorkspaceClient, inventory_database: str):
        super().__init__(backend, "hive_metastore", inventory_database, "mounts", Mount)
        self._dbutils = ws.dbutils

    def _deduplicate_mounts(self, mounts: list) -> list:
        seen = set()
        deduplicated_mounts = []

        for obj in mounts:
            obj_tuple = (obj.name, obj.source)
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
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield Mount(*row)
