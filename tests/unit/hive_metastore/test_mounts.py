from unittest.mock import MagicMock

from databricks.sdk.dbutils import MountInfo

from databricks.labs.ucx.hive_metastore.mounts import Mount, Mounts
from tests.unit.framework.mocks import MockBackend


def test_list_mounts_should_return_a_list_of_mount_without_encryption_type():
    client = MagicMock()
    client.dbutils.fs.mounts.return_value = [
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
    ]

    backend = MockBackend()
    instance = Mounts(backend, client, "test")

    instance.inventorize_mounts()

    expected = [Mount("mp_1", "path_1"), Mount("mp_2", "path_2")]
    assert expected == backend.rows_written_for("hive_metastore.test.mounts", "append")
