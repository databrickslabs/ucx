import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound

from databricks.labs.ucx.workspace_access.generic import WorkspaceListing

from ..retries import retried

_TEST_STORAGE_ACCOUNT = "storage_acct_1"

_TEST_TENANT_ID = "directory_12345"

logger = logging.getLogger(__name__)

_PIPELINE_CONF = {
    f"spark.hadoop.fs.azure.account.oauth2.client.id.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": ""
    "pipeline_dummy_application_id",
    f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": ""
    "https://login"
    f".microsoftonline.com/{_TEST_TENANT_ID}/oauth2/token",
}

_PIPELINE_CONF_WITH_SECRET = {
    "fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/reallyasecret123/sp_app_client_id}}",
    "fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com"
    "/dummy_application/token",
}

_SPARK_CONF = {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]",
    f"fs.azure.account.auth.type.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "org.apache.hadoop.fs"
    ".azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "dummy_application_id",
    f"fs.azure.account.oauth2.client.secret.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "dummy",
    f"fs.azure.account.oauth2.client.endpoint.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "https://login"
    f".microsoftonline.com/{_TEST_TENANT_ID}/oauth2/token",
}


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_workspace_object_crawler(ws, make_notebook, inventory_schema, sql_backend):
    notebook = make_notebook()
    workspace_listing = WorkspaceListing(ws, sql_backend, inventory_schema)
    workspace_objects = {_.path: _ for _ in workspace_listing.snapshot()}

    assert notebook in workspace_objects
    assert workspace_objects[notebook].object_type == "NOTEBOOK"
