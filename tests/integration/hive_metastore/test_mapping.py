import logging

from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
)
from databricks.labs.ucx.hive_metastore.mapping import ExternalLocationMapping

logger = logging.getLogger(__name__)


def test_save_external_location_mapping_missing_location(ws, sql_backend, inventory_schema):
    logger.info("setting up fixtures")
    locations = [
        ExternalLocation("abfss://cont1@storage123/test_location", 2),
        ExternalLocation("abfss://cont1@storage456/test_location2", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    ext_location_mapping = ExternalLocationMapping(ws)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)
    path = ext_location_mapping.save(location_crawler)
    assert ws.workspace.get_status(path)
