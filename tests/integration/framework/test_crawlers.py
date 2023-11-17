from databricks.labs.ucx.framework.crawlers import SchemaDeployer
from databricks.labs.ucx.hive_metastore.grants import Grant


def test_deploys_database(sql_backend, inventory_schema):
    from databricks.labs import ucx

    deployer = SchemaDeployer(sql_backend, inventory_schema, ucx)
    deployer.deploy_schema()
    deployer.deploy_table("grants", Grant)
    deployer.deploy_view("grant_detail", "queries/views/grant_detail.sql")
