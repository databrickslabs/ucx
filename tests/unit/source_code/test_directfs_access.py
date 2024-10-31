import datetime as dt
from unittest.mock import create_autospec

from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.owners import AdministratorLocator, WorkspacePathOwnership, LegacyQueryOwnership
from databricks.labs.ucx.source_code.base import LineageAtom
from databricks.labs.ucx.source_code.directfs_access import (
    DirectFsAccessCrawler,
    DirectFsAccess,
    DirectFsAccessOwnership,
)
from databricks.labs.ucx.source_code.python.python_ast import DfsaPyCollector


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
    ws = create_autospec(WorkspaceClient)
    admin_locator = create_autospec(AdministratorLocator)
    workspace_path_ownership = create_autospec(WorkspacePathOwnership)
    workspace_path_ownership.owner_of.return_value = "other_admin"
    legacy_query_ownership = create_autospec(LegacyQueryOwnership)

    ownership = DirectFsAccessOwnership(admin_locator, workspace_path_ownership, legacy_query_ownership, ws)
    dfsa = DirectFsAccess(source_lineage=[LineageAtom(object_type="NOTEBOOK", object_id="/x/y/z")])
    owner = ownership.owner_of(dfsa)

    assert owner == "other_admin"
    ws.queries.get.assert_not_called()
    legacy_query_ownership.owner_of.assert_not_called()
    admin_locator.get_workspace_administrator.assert_not_called()
