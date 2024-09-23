import pytest

from databricks.labs.ucx.assessment.secrets import SecretsMixin


@pytest.mark.parametrize(
    "key,expected",
    [
        ("spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL", "url"),
        ("NonExistentKey", ""),
        ("spark_conf.invalid", "{'should_not': 'be_string'}"),
    ],
)
def test_secrets_mixin_gets_value_from_config_key(key, expected) -> None:
    config: dict[str, str | dict[str, str]] = {
        "spark_conf.invalid": "{'should_not': 'be_string'}",
        "spark_conf.spark.hadoop.javax.jdo.option.ConnectionURL": {"value": "url"},
    }
    secrets_mixin = SecretsMixin()

    value = secrets_mixin._get_value_from_config_key(config, key)

    assert value == expected
