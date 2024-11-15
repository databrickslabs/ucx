import datetime as dt
from unittest.mock import create_autospec

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.directfs_access import DirectFsAccessProgressEncoder
from databricks.labs.ucx.source_code.base import DirectFsAccess, LineageAtom


def test_direct_filesystem_access_progress_encoder_failures(mock_backend) -> None:
    """A direct filesystem access is a failure by definition as it is not supported by Unity Catalog."""
    direct_filesystem_access = DirectFsAccess(
        path="dbfs://folder/file.csv",
        is_read=True,
        is_write=False,
        source_id="path/to/file.py",
        source_lineage=[LineageAtom(object_type="FILE", object_id="path/to/file.py")],
        source_timestamp=dt.datetime.now(dt.timezone.utc),
        assessment_start_timestamp=dt.datetime.now(dt.timezone.utc),
        assessment_end_timestamp=dt.datetime.now(dt.timezone.utc),
    )
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = DirectFsAccessProgressEncoder(
        mock_backend,
        ownership,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([direct_filesystem_access])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == ["Direct filesystem access is not supported in Unity Catalog"]
    ownership.owner_of.assert_called_once()
