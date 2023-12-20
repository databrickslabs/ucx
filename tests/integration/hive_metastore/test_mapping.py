import logging

from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
)
from databricks.labs.ucx.hive_metastore.mapping import ExternalLocationMapping

logger = logging.getLogger(__name__)


def test_save_external_location_mapping_missing_location(ws, sql_backend, inventory_schema, make_external_location):
    logger.info("setting up fixtures")
    locations = [
        ExternalLocation("abfss://cont1@storage123/test_location", 2),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    #make_external_location()
    ext_location_mapping = ExternalLocationMapping(ws)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)
    path = ext_location_mapping.save(location_crawler)
    with ws.workspace.download(path) as f:
        tf_script = f.read()

    assert (
        'resource "databricks_external_location" "name_1" { \n
        'name = "name_1"\n' \
        'url  = "abfss://cont1@storage123/test_location"\n' \
        'credential_name = <storage_credential_reference>\n' \
        '}'
    ) == tf_script
