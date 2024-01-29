from unittest.mock import MagicMock, patch

from databricks.sdk.service.compute import (
    GlobalInitScriptDetails,
    GlobalInitScriptDetailsWithContent,
)

from databricks.labs.ucx.hive_metastore.hms_lineage import HiveMetastoreLineageEnabler


def test_add_spark_config_exists_enabled_for_hms_lineage(mocker):
    ws = mocker.Mock()
    ginit_scripts = [
        GlobalInitScriptDetails(
            created_at=1695045723722,
            created_by="test@abc.com",
            enabled=True,
            name="test123",
            position=0,
            script_id="12345",
            updated_at=1695046359612,
            updated_by="test@abc.com",
        )
    ]
    ws.global_init_scripts.list.return_value = ginit_scripts
    ws.global_init_scripts.get.return_value = GlobalInitScriptDetailsWithContent(
        created_at=1695045723722,
        created_by="test@abc.com",
        enabled=True,
        name="test123",
        position=0,
        script="aWYgW1sgJERCX0lTX0RSSVZFUiA9ICJUUlVFIiB"
        "dXTsgdGhlbgogIGRyaXZlcl9jb25mPSR7REJfSE9NRX0"
        "vZHJpdmVyL2NvbmYvc3BhcmstYnJhbmNoLmNvbmYKICBpZ"
        "iBbICEgLWUgJGRyaXZlcl9jb25mIF0gOyB0aGVuCiAgICB0b"
        "3VjaCAkZHJpdmVyX2NvbmYKICBmaQpjYXQgPDwgRU9GID4+ICAkZ"
        "HJpdmVyX2NvbmYKICBbZHJpdmVyXSB7CiAgICJzcGFyay5kYXRhYnJpY2tzLm"
        "RhdGFMaW5lYWdlLmVuYWJsZWQiID0gdHJ1ZQogICB9CkVPRgpmaQ==",
        script_id="12C100F8BB38B002",
        updated_at=1695046359612,
        updated_by="test@abc.com",
    )

    hmle = HiveMetastoreLineageEnabler(ws)
    script = hmle.check_lineage_spark_config_exists()

    assert script is not None


def test_add_spark_config_not_exists_for_hms_lineage(mocker):
    ws = mocker.Mock()
    ginit_scripts = [
        GlobalInitScriptDetails(
            created_at=1695045723722,
            created_by="test@abc.com",
            enabled=False,
            name="test123",
            position=0,
            script_id="12345",
            updated_at=1695046359612,
            updated_by="test@abc.com",
        )
    ]
    ws.global_init_scripts.list.return_value = ginit_scripts
    ws.global_init_scripts.get.return_value = GlobalInitScriptDetailsWithContent(
        created_at=1695045723722,
        created_by="test@abc.com",
        enabled=False,
        name="test123",
        position=0,
        script="JXNoCmVjaG8gIj09PT0gQmVnaW4gb2YgdGhlIGd"
        "sb2JhbCBpbml0IHNjcmlwdCIKCmVjaG8gIj09PT0"
        "gTGlzdCBWb2x1bWUsIGl0IHdvbid0IHdvcmsiCmxzIC9Wb2x1bWVzL3lhbmd3"
        "YW5nX2RlbW8vZGVmYXVsdC92b2wxL21zb2RiY3NxbDE3LmRlYiAKc3VkbyB"
        "jcCAvVm9sdW1lcy95YW5nd2FuZ19kZW1vL2RlZmF1bHQvdm9sMS9tc29kYmNzcWw"
        "xNy5kZWIgLi8KCmVjaG8gIj09PT0gTGlzdCBEQkZTIgpscyAvZGJmcy9GaWxlU3R"
        "vcmUvbXNvZGJjc3FsMTcuZGViCnN1ZG8gY3AgL2RiZnMvRmlsZVN0b3JlL21zb2RiY3N"
        "xbDE3LmRlYiAuLwoKc3VkbyBBQ0NFUFRfRVVMQT1ZIGFwdCBpbnN0YWxsIC15IC4vbXNvZ"
        "GJjc3FsMTcuZGViCgplY2hvICI9PT09IEZpbmlzaGVkIEluc3RhbGwgLmRlYiI=",
        script_id="12C100F8BB38B002",
        updated_at=1695046359612,
        updated_by="test@abc.com",
    )
    hmle = HiveMetastoreLineageEnabler(ws)
    script = hmle.check_lineage_spark_config_exists()

    assert script is None


def test_add_spark_config_exists_disabled_for_hms_lineage(mocker):
    ws = mocker.Mock()
    ginit_scripts = [
        GlobalInitScriptDetails(
            created_at=1695045723722,
            created_by="test@abc.com",
            enabled=False,
            name="test123",
            position=0,
            script_id="12345",
            updated_at=1695046359612,
            updated_by="test@abc.com",
        )
    ]
    ws.global_init_scripts.list.return_value = ginit_scripts
    ws.global_init_scripts.get.return_value = GlobalInitScriptDetailsWithContent(
        created_at=1695045723722,
        created_by="test@abc.com",
        enabled=False,
        name="test123",
        position=0,
        script="aWYgW1sgJERCX0lTX0RSSVZFUiA9ICJUUlVFIiB"
        "dXTsgdGhlbgogIGRyaXZlcl9jb25mPSR7REJfSE9NRX0"
        "vZHJpdmVyL2NvbmYvc3BhcmstYnJhbmNoLmNvbmYKICBpZ"
        "iBbICEgLWUgJGRyaXZlcl9jb25mIF0gOyB0aGVuCiAgICB0b"
        "3VjaCAkZHJpdmVyX2NvbmYKICBmaQpjYXQgPDwgRU9GID4+ICAkZ"
        "HJpdmVyX2NvbmYKICBbZHJpdmVyXSB7CiAgICJzcGFyay5kYXRhYnJpY2tzLm"
        "RhdGFMaW5lYWdlLmVuYWJsZWQiID0gdHJ1ZQogICB9CkVPRgpmaQ==",
        script_id="12C100F8BB38B002",
        updated_at=1695046359612,
        updated_by="test@abc.com",
    )
    hmle = HiveMetastoreLineageEnabler(ws)
    script = hmle.check_lineage_spark_config_exists()

    assert script is not None
    assert script.name == "test123"


def test_add_spark_config_no_gscript_for_hms_lineage(mocker):
    ws = mocker.Mock()
    ginit_scripts = []
    ws.global_init_scripts.list.return_value = ginit_scripts
    hmle = HiveMetastoreLineageEnabler(ws)
    script_id = hmle.check_lineage_spark_config_exists()

    assert script_id is None


@patch("builtins.open", new_callable=MagicMock)
@patch("base64.b64encode")
def test_get_init_script_content(mock_open, mocker):
    expected_content = """if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  driver_conf=${DB_HOME}/driver/conf/spark-branch.conf
  if [ ! -e $driver_conf ] ; then
    touch $driver_conf
  fi
cat << EOF >>  $driver_conf
  [driver] {
   "spark.databricks.dataLineage.enabled" = true
   }
EOF
fi
    """
    ws = mocker.Mock()
    mock_file = MagicMock()
    mock_file.read.return_value = expected_content
    mock_open.return_value = mock_file
    hmle = HiveMetastoreLineageEnabler(ws)
    script_id = hmle.add_global_init_script()

    assert script_id is not None


def test_enable_gscript_for_hms_lineage(mocker):
    ws = mocker.Mock()
    gscript = GlobalInitScriptDetailsWithContent(
        created_at=1695045723722,
        created_by="test@abc.com",
        enabled=False,
        name="test123",
        position=0,
        script="aWYgW1sgJERCX0lTX0RSSVZFUiA9ICJUUlVFIiB"
        "dXTsgdGhlbgogIGRyaXZlcl9jb25mPSR7REJfSE9NRX0"
        "vZHJpdmVyL2NvbmYvc3BhcmstYnJhbmNoLmNvbmYKICBpZ"
        "iBbICEgLWUgJGRyaXZlcl9jb25mIF0gOyB0aGVuCiAgICB0b"
        "3VjaCAkZHJpdmVyX2NvbmYKICBmaQpjYXQgPDwgRU9GID4+ICAkZ"
        "HJpdmVyX2NvbmYKICBbZHJpdmVyXSB7CiAgICJzcGFyay5kYXRhYnJpY2tzLm"
        "RhdGFMaW5lYWdlLmVuYWJsZWQiID0gdHJ1ZQogICB9CkVPRgpmaQ==",
        script_id="12C100F8BB38B002",
        updated_at=1695046359612,
        updated_by="test@abc.com",
    )
    hmle = HiveMetastoreLineageEnabler(ws)
    script_id = hmle.enable_global_init_script(gscript)

    assert script_id is not None
    assert script_id == "12C100F8BB38B002"


def test_add_spark_config_no_matching_gscript_for_hms_lineage(mocker):
    ws = mocker.Mock()
    ginit_scripts = [
        GlobalInitScriptDetails(
            created_at=1695045723722,
            created_by="test@abc.com",
            enabled=False,
            name="test123",
            position=0,
            script_id="12345",
            updated_at=1695046359612,
            updated_by="test@abc.com",
        )
    ]
    ws.global_init_scripts.list.return_value = ginit_scripts
    ws.global_init_scripts.get.return_value = None
    hmle = HiveMetastoreLineageEnabler(ws)
    script_id = hmle.check_lineage_spark_config_exists()

    assert script_id is None
