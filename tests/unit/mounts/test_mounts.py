from unittest.mock import MagicMock

from databricks.sdk.dbutils import MountInfo

from databricks.labs.ucx.mounts.list_mounts import Mount, Mounts


def test_list_mounts_should_return_a_list_of_mount_without_encryption_type():
    client = MagicMock()
    client.dbutils.fs.mounts.return_value = [
        MountInfo("mp_1", "path_1", "info_1"),
        MountInfo("mp_2", "path_2", "info_2"),
    ]

    backend = MagicMock()
    instance = Mounts(backend, client, "test")

    results = instance._list_mounts()

    assert results == [Mount("mp_1", "path_1"), Mount("mp_2", "path_2")]
