import datetime as dt
from unittest.mock import create_autospec


import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.core import Row

from databricks.labs.ucx.__about__ import __version__ as ucx_version
from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.progress.history import HistoryLog
from databricks.labs.ucx.source_code.base import LineageAtom
from databricks.labs.ucx.source_code.directfs_access import (
    DirectFsAccessCrawler,
    DirectFsAccess,
    DirectFsAccessOwnership,
)


def test_crawler_appends_dfsas() -> None:
    backend = MockBackend()
    crawler = DirectFsAccessCrawler.for_paths(backend, "schema")
    existing = list(crawler.snapshot())
    assert not existing
    now = dt.datetime.now(tz=dt.timezone.utc)
    dfsas = list(
        DirectFsAccess(
            path=path,
            is_read=False,
            is_write=False,
            source_id="ID",
            source_timestamp=now,
            source_lineage=[LineageAtom(object_type="LINEAGE", object_id="ID")],
            assessment_start_timestamp=now,
            assessment_end_timestamp=now,
        )
        for path in ("a", "b", "c")
    )
    crawler.dump_all(dfsas)
    rows = backend.rows_written_for(crawler.full_name, "append")
    assert len(rows) == 3


def test_directfs_access_ownership() -> None:
    """Verify that the owner for a direct-fs access record is an administrator."""
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "an_admin"

    ownership = DirectFsAccessOwnership(admin_locator)
    dfsa = DirectFsAccess()
    owner = ownership.owner_of(dfsa)

    assert owner == "an_admin"
    admin_locator.get_workspace_administrator.assert_called_once()


@pytest.mark.parametrize(
    "directfs_access_record,history_record",
    (
        (
            DirectFsAccess(
                source_id="an_id",
                source_timestamp=dt.datetime(
                    year=2024, month=10, day=21, hour=14, minute=45, second=10, tzinfo=dt.timezone.utc
                ),
                source_lineage=[
                    LineageAtom(object_type="WORKFLOW", object_id="a", other={"foo": "bar", "baz": "daz"}),
                    LineageAtom(object_type="FILE", object_id="b"),
                    LineageAtom(object_type="NOTEBOOK", object_id="c", other={}),
                ],
                assessment_start_timestamp=dt.datetime(
                    year=2024, month=10, day=22, hour=13, minute=45, second=10, tzinfo=dt.timezone.utc
                ),
                assessment_end_timestamp=dt.datetime(
                    year=2024, month=10, day=22, hour=14, minute=45, second=10, tzinfo=dt.timezone.utc
                ),
                path="/path",
                is_read=True,
                is_write=False,
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="DirectFsAccess",
                object_id=["an_id"],
                data={
                    "source_id": "an_id",
                    "source_timestamp": "2024-10-21T14:45:10Z",
                    "source_lineage": '[{"object_type":"WORKFLOW","object_id":"a","other":{"foo":"bar","baz":"daz"}},{"object_type":"FILE","object_id":"b","other":null},{"object_type":"NOTEBOOK","object_id":"c","other":{}}]',
                    "assessment_start_timestamp": "2024-10-22T13:45:10Z",
                    "assessment_end_timestamp": "2024-10-22T14:45:10Z",
                    "path": "/path",
                    "is_read": "true",
                    "is_write": "false",
                },
                failures=[],
                owner="user@domain",
                ucx_version=ucx_version,
            ),
        ),
        (
            DirectFsAccess(
                source_id="the_id",
                source_timestamp=dt.datetime(
                    year=2024, month=10, day=21, hour=14, minute=45, second=10, tzinfo=dt.timezone.utc
                ),
                source_lineage=[],
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="DirectFsAccess",
                object_id=["the_id"],
                data={
                    "source_id": "the_id",
                    "source_timestamp": "2024-10-21T14:45:10Z",
                    "source_lineage": "[]",
                    "assessment_start_timestamp": "1970-01-01T00:00:00Z",
                    "assessment_end_timestamp": "1970-01-01T00:00:00Z",
                    "path": "unknown",
                    "is_read": "false",
                    "is_write": "false",
                },
                failures=[],
                owner="user@domain",
                ucx_version=ucx_version,
            ),
        ),
    ),
)
def test_directfs_supports_history(mock_backend, directfs_access_record: DirectFsAccess, history_record: Row) -> None:
    """Verify that DirectFsAccess records are written as expected to the history log."""
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "user@domain"
    directfs_access_ownership = DirectFsAccessOwnership(admin_locator)
    history_log = HistoryLog[DirectFsAccess](
        mock_backend,
        directfs_access_ownership,
        DirectFsAccess,
        run_id=1,
        workspace_id=2,
        catalog="a_catalog",
    )

    history_log.append_inventory_snapshot([directfs_access_record])

    rows = mock_backend.rows_written_for("`a_catalog`.`multiworkspace`.`historical`", mode="append")

    assert rows == [history_record]
