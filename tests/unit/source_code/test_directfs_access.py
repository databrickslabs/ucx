from datetime import datetime
from unittest.mock import create_autospec

from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.source_code.base import LineageAtom
from databricks.labs.ucx.source_code.directfs_access import (
    DirectFsAccessCrawler,
    DirectFsAccess,
    DirectFsAccessOwnership,
)


def test_crawler_appends_dfsas():
    backend = MockBackend()
    crawler = DirectFsAccessCrawler.for_paths(backend, "schema")
    existing = list(crawler.snapshot())
    assert not existing
    dfsas = list(
        DirectFsAccess(
            path=path,
            is_read=False,
            is_write=False,
            source_id="ID",
            source_timestamp=datetime.now(),
            source_lineage=[LineageAtom(object_type="LINEAGE", object_id="ID")],
            assessment_start_timestamp=datetime.now(),
            assessment_end_timestamp=datetime.now(),
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
