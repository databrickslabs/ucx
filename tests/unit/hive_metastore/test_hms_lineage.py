from databricks.sdk.service.compute import (
    GlobalInitScriptDetails,
    GlobalInitScriptDetailsWithContent,
)

from databricks.labs.ucx.hive_metastore.hms_lineage import HiveMetastoreLineageEnabler


def test_add_spark_config_exists_for_hms_lineage(mocker):
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
        script="JXNoCmVjaG8gIj09PT0gQmVnaW4gb2YgdGhlIGdsb2Jh"
        "bCBpbml0IHNjcmlwdCIKCmVjaG8gIj09PT0gTGlzdCBWb2x1bWU"
        "sIGl0IHdvbid0IHdvcmsiCmxzIC9Wb2x1bWVzL3lhbmd3YW5nX2RlbW8vZG"
        "VmYXVsdC92b2wxL21zb2RiY3NxbDE3LmRlYiAKc3VkbyBjcCAvVm9sdW1lcy"
        "95YW5nd2FuZ19kZW1vL2RlZmF1bHQvdm9sMS9tc29kYmNzcWwxNy5kZWIgLi8KCmV"
        "jaG8gIj09PT0gTGlzdCBEQkZTIgpscyAvZGJmcy9GaWxlU3RvcmUvbXNvZGJjc3FsM"
        "TcuZGViCnN1ZG8gY3AgL2RiZnMvRmlsZVN0b3JlL21zb2RiY3NxbDE3LmRlYiAuLwoKc3Vkb"
        "yBBQ0NFUFRfRVVMQT1ZIGFwdCBpbnN0YWxsIC15IC4vbXNvZGJjc3FsMTcuZGViCgplY2hvICI9P"
        "T09IEZpbmlzaGVkIEluc3RhbGwgLmRlYiI=",
        script_id="12C100F8BB38B002",
        updated_at=1695046359612,
        updated_by="test@abc.com",
    )

    hmle = HiveMetastoreLineageEnabler(ws)
    script_id = hmle.add_spark_config_for_hms_lineage()

    assert script_id != ""


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
    script_id = hmle.add_spark_config_for_hms_lineage()

    assert script_id != ""
