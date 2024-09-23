import pytest

from databricks.labs.ucx.assessment.secrets import SecretsMixin


@pytest.mark.parametrize(
    "key,expected",
    [
        ("spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL", "url"),
    ]
)
def test_secrets_mixin_gets_value_from_config_key(key, expected) -> None:
    config = {
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"value": "url"},
    }
    secrets_mixin = SecretsMixin()

    assert secrets_mixin._get_value_from_config_key(config, key) == expected

