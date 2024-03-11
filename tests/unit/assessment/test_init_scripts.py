import base64

from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.compute import GlobalInitScriptDetails

from databricks.labs.ucx.assessment.init_scripts import (
    GlobalInitScriptCrawler,
    GlobalInitScriptInfo,
)


def test_global_init_scripts_no_config(mocker):
    mock_ws = mocker.Mock()
    mocker.Mock()
    mock_ws.global_init_scripts.list.return_value = [
        GlobalInitScriptDetails(
            created_at=111,
            created_by="123@234.com",
            enabled=False,
            name="newscript",
            position=4,
            script_id="222",
            updated_at=111,
            updated_by="2123l@eee.com",
        )
    ]
    mock_ws.global_init_scripts.get().script = "JXNoCmVjaG8gIj0="
    crawler = GlobalInitScriptCrawler(mock_ws, MockBackend(), schema="UCX")
    result = crawler.snapshot()
    assert len(result) == 1
    assert result[0].success == 1


def test_global_init_scripts_with_config(mocker):
    mock_ws = mocker.Mock()
    mocker.Mock()
    mock_ws.global_init_scripts.list.return_value = [
        GlobalInitScriptDetails(
            created_at=111,
            created_by="123@234.com",
            enabled=False,
            name="newscript",
            position=4,
            script_id="222",
            updated_at=111,
            updated_by="2123l@eee.com",
        )
    ]
    mock_ws.global_init_scripts.get().script = (
        "IyEvYmluL2Jhc2gKCiMgU2V0IGEgY3"
        "VzdG9tIFNwYXJrIGNvbmZpZ3VyYXRpb24KZWNobyAic"
        "3BhcmsuZXhlY3V0b3IubWVtb3J5IDRnIiA+PiAvZGF0YWJyaWN"
        "rcy9zcGFyay9jb25mL3NwYXJrLWRlZmF1bHRzLmNvbmYKZWNobyAic3Bhcm"
        "suZHJpdmVyLm1lbW9yeSAyZyIgPj4gL2RhdGFicmlja3Mvc3BhcmsvY29uZi9zcGFy"
        "ay1kZWZhdWx0cy5jb25mCmVjaG8gInNwYXJrLmhhZG9vcC5mcy5henVyZS5hY2NvdW50LmF1"
        "dGgudHlwZS5hYmNkZS5kZnMuY29yZS53aW5kb3dzLm5ldCBPQXV0aCIgPj4gL2RhdGFic"
        "mlja3Mvc3BhcmsvY29uZi9zcGFyay1kZWZhdWx0cy5jb25mCmVjaG8gInNwYXJrLmhhZG9vc"
        "C5mcy5henVyZS5hY2NvdW50Lm9hdXRoLnByb3ZpZGVyLnR5cGUuYWJjZGUuZGZzLmNvcmUud2l"
        "uZG93cy5uZXQgb3JnLmFwYWNoZS5oYWRvb3AuZnMuYXp1cmViZnMub2F1dGgyLkNsaWVudENyZ"
        "WRzVG9rZW5Qcm92aWRlciIgPj4gL2RhdGFicmlja3Mvc3BhcmsvY29uZi9zcGFyay1kZWZhdWx0c"
        "y5jb25mCmVjaG8gInNwYXJrLmhhZG9vcC5mcy5henVyZS5hY2NvdW50Lm9hdXRoMi5jbGllbnQu"
        "aWQuYWJjZGUuZGZzLmNvcmUud2luZG93cy5uZXQgZHVtbXlfYXBwbGljYXRpb25faWQiID4+IC9"
        "kYXRhYnJpY2tzL3NwYXJrL2NvbmYvc3BhcmstZGVmYXVsdHMuY29uZgplY2hvICJzcGFyay5oY"
        "WRvb3AuZnMuYXp1cmUuYWNjb3VudC5vYXV0aDIuY2xpZW50LnNlY3JldC5hYmNkZS5kZnMuY29y"
        "ZS53aW5kb3dzLm5ldCBkZGRkZGRkZGRkZGRkZGRkZGRkIiA+PiAvZGF0YWJyaWNrcy9zcGFyay9j"
        "b25mL3NwYXJrLWRlZmF1bHRzLmNvbmYKZWNobyAic3BhcmsuaGFkb29wLmZzLmF6dXJlLmFjY291"
        "bnQub2F1dGgyLmNsaWVudC5lbmRwb2ludC5hYmNkZS5kZnMuY29yZS53aW5kb3dzLm5ldCBodHRw"
        "czovL2xvZ2luLm1pY3Jvc29mdG9ubGluZS5jb20vZHVtbXlfdGVuYW50X2lkL29hdXRoMi90b2tlb"
        "iIgPj4gL2RhdGFicmlja3Mvc3BhcmsvY29uZi9zcGFyay1kZWZhdWx0cy5jb25mCg=="
    )

    crawler = GlobalInitScriptCrawler(mock_ws, MockBackend(), schema="UCX")
    result = crawler.snapshot()
    assert len(result) == 1
    assert result[0].success == 0


def test_init_script_without_config_should_have_empty_creator_name(mocker):
    mock_ws = mocker.Mock()
    mocker.Mock()
    mock_ws.global_init_scripts.list.return_value = [
        GlobalInitScriptDetails(
            created_at=111,
            created_by=None,
            enabled=False,
            name="newscript",
            position=4,
            script_id="222",
            updated_at=111,
            updated_by="2123l@eee.com",
        )
    ]
    mock_ws.global_init_scripts.get().script = base64.b64encode(b"hello world")
    mockbackend = MockBackend()
    crawler = GlobalInitScriptCrawler(mock_ws, mockbackend, schema="ucx")
    result = crawler.snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.global_init_scripts", "append")

    assert result == [
        GlobalInitScriptInfo(
            script_id="222", script_name="newscript", enabled=False, created_by=None, success=1, failures="[]"
        ),
    ]


def test_missing_global_init_script(mocker, caplog):
    mock_ws = mocker.Mock()
    mock_ws.global_init_scripts.list.return_value = [
        GlobalInitScriptDetails(
            created_at=111,
            created_by=None,
            enabled=False,
            name="newscript",
            position=4,
            script_id="222",
            updated_at=111,
            updated_by="2123l@eee.com",
        )
    ]
    mock_ws.global_init_scripts.get.side_effect = ResourceDoesNotExist("RESOURCE_DOES_NOT_EXIST")
    mockbackend = MockBackend()
    crawler = GlobalInitScriptCrawler(mock_ws, mockbackend, schema="ucx")
    result = crawler.snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.global_init_scripts", "append")

    assert len(result) == 0
    assert "removed on the backend 222" in caplog.messages
