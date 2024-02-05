import pytest
from databricks.sdk.service.compute import DbfsStorageInfo, InitScriptInfo
from databricks.sdk.service.pipelines import PipelineCluster


@pytest.fixture(scope="function")
def mock_pipeline_cluster():
    yield [
        PipelineCluster(
            apply_policy_default_values=None,
            autoscale=None,
            aws_attributes=None,
            azure_attributes=None,
            cluster_log_conf=None,
            custom_tags={'cluster_type': 'default'},
            driver_instance_pool_id=None,
            driver_node_type_id=None,
            gcp_attributes=None,
            init_scripts=[
                InitScriptInfo(
                    dbfs=DbfsStorageInfo(destination="dbfs:/users/test@test.com/init_scripts/test.sh"),
                    s3=None,
                    volumes=None,
                    workspace=None,
                )
            ],
            instance_pool_id=None,
            label='default',
            node_type_id='Standard_F4s',
            num_workers=1,
            policy_id="single-user-with-spn",
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_env_vars=None,
            ssh_public_keys=None,
        )
    ]


@pytest.fixture(scope="function")
def mock_pipeline_cluster_with_no_config():
    yield [
        PipelineCluster(
            apply_policy_default_values=None,
            autoscale=None,
            aws_attributes=None,
            azure_attributes=None,
            cluster_log_conf=None,
            custom_tags={'cluster_type': 'default'},
            driver_instance_pool_id=None,
            driver_node_type_id=None,
            gcp_attributes=None,
            init_scripts=[],
            instance_pool_id=None,
            label='default',
            node_type_id='Standard_F4s',
            num_workers=1,
            policy_id=None,
            spark_conf=None,
            spark_env_vars=None,
            ssh_public_keys=None,
        )
    ]
