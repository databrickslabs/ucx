import os
import typing
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.hive_metastore.list_mounts import Mounts


@dataclass
class ExtLoc:
    location: str


class ExternalLocationCrawler(CrawlerBase):
    _prefix_size: typing.ClassVar[list[int]] = [1, 12]

    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "external_locations")
        self._ws = ws

    def _external_locations(self, tables, mounts):
        ext_locations = []
        for table in tables:
            location = table.as_dict()["location"]
            if location is not None and len(location) > 0:
                if location.startswith("dbfs:/mnt"):
                    for mount in mounts:
                        if location[5:].startswith(mount.name):
                            location = location[5:].replace(mount.name, mount.source)
                            break
                if not location.startswith("dbfs") and (
                    self._prefix_size[0] < location.find(":/") < self._prefix_size[1]
                ):
                    dupe = False
                    loc = 0
                    while loc < len(ext_locations) and not dupe:
                        common = (
                            os.path.commonpath([ext_locations[loc].location, os.path.dirname(location) + "/"]).replace(
                                ":/", "://"
                            )
                            + "/"
                        )
                        if common.count("/") > 2:
                            ext_locations[loc] = ExtLoc(common)
                            dupe = True
                        loc += 1
                    if not dupe:
                        ext_locations.append(ExtLoc(os.path.dirname(location) + "/"))
        return ext_locations

    def _ext_loc_list(self):
        tables = self._backend.fetch(f"SELECT location FROM {self._schema}.tables WHERE location IS NOT NULL")
        mounts = Mounts(self._backend, self._ws, self._schema).snapshot()
        return self._external_locations(list(tables), list(mounts))

    def snapshot(self) -> list[ExtLoc]:
        return self._snapshot(self._try_fetch, self._ext_loc_list)

    def _try_fetch(self) -> list[ExtLoc]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield ExtLoc(*row)
