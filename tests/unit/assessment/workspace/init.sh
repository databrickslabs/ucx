#!/bin/bash
# Set a custom Spark configuration
echo "spark.executor.memory 4g" >> /databricks/spark/conf/spark-defaults.conf
echo "spark.driver.memory 2g" >> /databricks/spark/conf/spark-defaults.conf
echo "spark.hadoop.fs.azure.account.auth.type.abcde.dfs.core.windows.net OAuth" >> /databricks/spark/conf/spark-defaults.conf
echo "spark.hadoop.fs.azure.account.oauth.provider.type.abcde.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider" >> /databricks/spark/conf/spark-defaults.conf
echo "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net dummy_application_id" >> /databricks/spark/conf/spark-defaults.conf
echo "spark.hadoop.fs.azure.account.oauth2.client.secret.abcde.dfs.core.windows.net ddddddddddddddddddd" >> /databricks/spark/conf/spark-defaults.conf
echo "spark.hadoop.fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net https://login.microsoftonline.com/dummy_tenant_id/oauth2/token" >> /databricks/spark/conf/spark-defaults.conf
