def get_azure_spark_conf():
    return {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]",
        "fs.azure.account.auth.type.labsazurethings.dfs.core.windows.net": "OAuth",
        "fs.azure.account.oauth.provider.type.labsazurethings.dfs.core.windows.net": "org.apache.hadoop.fs"
        ".azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id.labsazurethings.dfs.core.windows.net": "dummy_application_id",
        "fs.azure.account.oauth2.client.secret.labsazurethings.dfs.core.windows.net": "dummy",
        "fs.azure.account.oauth2.client.endpoint.labsazurethings.dfs.core.windows.net": "https://login"
        ".microsoftonline.com/directory_12345/oauth2/token",
    }
