from unittest.mock import create_autospec

from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    GlobalInitScriptDetails,
    GlobalInitScriptDetailsWithContent,
)
from databricks.sdk.errors import InvalidParameterValue

from databricks.labs.ucx.installer.hms_lineage import HiveMetastoreLineageEnabler


def test_already_exists_enabled(caplog):
    ws = create_autospec(WorkspaceClient)
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
    prompts = MockPrompts({})
    hmle = HiveMetastoreLineageEnabler(ws)
    with caplog.at_level('INFO'):
        hmle.apply(prompts)
        assert caplog.messages == ['HMS lineage init script already exists and enabled']


def test_not_exists_and_created(caplog):
    ws = create_autospec(WorkspaceClient)
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
    hmle.apply(MockPrompts({r'No HMS lineage collection init script exists.*': 'yes'}))

    ws.global_init_scripts.create.assert_called_once()


def test_disabled_and_then_enabled(caplog):
    ws = create_autospec(WorkspaceClient)
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
    hmle.apply(MockPrompts({r'HMS lineage collection init script is disabled.*': 'yes'}))

    ws.global_init_scripts.update.assert_called_once()


def test_get_script_fails_missing_script(caplog):
    ws = create_autospec(WorkspaceClient)
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
    ws.global_init_scripts.get.side_effect = InvalidParameterValue("INVALID_PARAMETER_VALUE")

    with caplog.at_level('WARN'):
        hmle = HiveMetastoreLineageEnabler(ws)
        hmle.apply(MockPrompts({r'No HMS lineage collection init script exists.*': 'yes'}))
        assert "Failed to get init script 12345: INVALID_PARAMETER_VALUE" in caplog.messages