from unittest.mock import Mock
import pytest
from databricks.labs.ucx.hive_metastore.dbfs_files import DbfsFiles, DbfsFileInfo


@pytest.fixture
def dbfs_files(mocker):
    # dbfs_files is dependent on the jvm, so we need to mock that

    mock_spark = mocker.Mock()
    mock_jsc = mocker.Mock()
    mock_jvm = mocker.Mock()
    mock_filesystem = mocker.Mock()
    mock_jvm.jvm_filesystem.get.return_value = mock_filesystem
    mock_jvm.jvm_path.side_effect = lambda x: x
    mock_spark._jsc = mock_jsc  # pylint: disable=protected-access
    mock_spark.jvm = mock_jvm

    _dbfs_files = DbfsFiles(jvm_interface=mock_spark)

    return _dbfs_files


# use trie data structure to mock a backend file system, matching the general behavior of hadoop java library
# provide an elegant API for adding files into the mock filesystem, reducing human error
class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_path = False


class MockFs:
    def __init__(self):
        self.root = TrieNode()

    def put(self, path: str) -> None:
        node = self.root
        parts = path.removeprefix("dbfs:/").rstrip("/").split("/")
        for part in parts:
            if part not in node.children:
                node.children[part] = TrieNode()
            node = node.children[part]
        node.is_end_of_path = True

    @staticmethod
    def has_children(node: TrieNode) -> bool:
        return bool(node.children)

    def mock_path_component(self, path: str, name: str, node: TrieNode) -> Mock:
        if name:
            _path = path.rstrip("/") + '/' + name
        else:
            _path = path.rstrip("/")
            name = _path.rsplit('/')[-1]

        mock_status: Mock = Mock()
        mock_status.getPath.return_value.toString.return_value = _path
        mock_status.getPath.return_value.getName.return_value = name
        mock_status.isDir.return_value = self.has_children(node)
        mock_status.getModificationTime.return_value = 0

        return mock_status

    class IllegalArgumentException(ValueError):
        pass

    def list_dir(self, path: str) -> list[Mock]:
        node: TrieNode = self.root
        if path:
            parts: list[str] = path.removeprefix("dbfs:/").rstrip("/").split("/")
            for part in parts:
                if part == '':
                    continue
                if part in node.children:
                    node = node.children[part]
                else:
                    raise FileNotFoundError(f"'{path}' not found")

            # list_files will return identity if there are no child path components
            # note: in the actual api, listing an empty directory just results in an empty list, [], but
            # that functionality is not supported in the mock api
            if not self.has_children(node):
                return [self.mock_path_component(path, '', node)]

            # in typical case, return children
            return [self.mock_path_component(path, name, node) for name, node in node.children.items()]

        raise self.IllegalArgumentException("Can not create a Path from an empty string")


@pytest.fixture
def mock_hadoop_fs():
    return MockFs()


def test_mock_hadoop_fs_put(mock_hadoop_fs):
    mock_hadoop_fs.put("dbfs:/dir1/dir2/file")
    node = mock_hadoop_fs.root
    assert "dir1" in node.children
    assert "dir2" in node.children["dir1"].children
    assert "file" in node.children["dir1"].children["dir2"].children
    assert node.children["dir1"].children["dir2"].children["file"].is_end_of_path is True


def test_mock_hadoop_fs_listdir(dbfs_files, mock_hadoop_fs):
    mock_hadoop_fs.put("dbfs:/test/path_a")
    mock_hadoop_fs.put("dbfs:/test/path_a/file_a")
    mock_hadoop_fs.put("dbfs:/test/path_a/file_b")
    mock_hadoop_fs.put("dbfs:/test/path_b")

    dbfs_files._fs.listStatus.side_effect = mock_hadoop_fs.list_dir  # pylint: disable=protected-access

    result = dbfs_files.list_dir("dbfs:/")
    assert result == [DbfsFileInfo("dbfs:/test", "test", True, 0)]

    result = dbfs_files.list_dir("dbfs:/test")
    assert result == [
        DbfsFileInfo("dbfs:/test/path_a", "path_a", True, 0),
        DbfsFileInfo("dbfs:/test/path_b", "path_b", False, 0),
    ]

    result = dbfs_files.list_dir("dbfs:/test/path_a")
    assert result == [
        DbfsFileInfo("dbfs:/test/path_a/file_a", "file_a", False, 0),
        DbfsFileInfo("dbfs:/test/path_a/file_b", "file_b", False, 0),
    ]

    # ensure identity is passed back if there are no children
    result = dbfs_files.list_dir("dbfs:/test/path_b")
    assert result == [DbfsFileInfo("dbfs:/test/path_b", "path_b", False, 0)]


def test_mock_hadoop_fs_nonexistent_path(mock_hadoop_fs):
    with pytest.raises(FileNotFoundError, match="'dbfs:/nonexistent' not found"):
        mock_hadoop_fs.list_dir("dbfs:/nonexistent")


def test_mock_hadoop_fs_invalid_path(mock_hadoop_fs):
    with pytest.raises(mock_hadoop_fs.IllegalArgumentException, match="Can not create a Path from an empty string"):
        mock_hadoop_fs.list_dir("")


def test_list_dir(dbfs_files, mock_hadoop_fs):
    mock_hadoop_fs.put("dbfs:/test/path_a")
    mock_hadoop_fs.put("dbfs:/test/path_a/file_a")
    mock_hadoop_fs.put("dbfs:/test/path_b")

    dbfs_files._fs.listStatus.side_effect = mock_hadoop_fs.list_dir  # pylint: disable=protected-access

    result = dbfs_files.list_dir("dbfs:/test")
    assert result == [
        DbfsFileInfo("dbfs:/test/path_a", "path_a", True, 0),
        DbfsFileInfo("dbfs:/test/path_b", "path_b", False, 0),
    ]
    result = dbfs_files.list_dir("dbfs:/test/path_a")
    assert result == [DbfsFileInfo("dbfs:/test/path_a/file_a", "file_a", False, 0)]
    bad_path = "dbfs:/test/path_c"
    with pytest.raises(FileNotFoundError, match=f"'{bad_path}' not found"):
        dbfs_files.list_dir(bad_path)
    bad_path = "dbfs:/test/path_c"
    with pytest.raises(FileNotFoundError, match=f"'{bad_path}' not found"):
        dbfs_files.list_dir(bad_path)
    invalid_path = "/dbfs/test/path_a"
    with pytest.raises(
        DbfsFiles.InvalidPathFormatError,
        match=f"Input path should begin with 'dbfs:/' prefix. Input path: '{invalid_path}'",
    ):
        dbfs_files.list_dir(invalid_path)


def test_file_status_to_dbfs_file_info(mocker):
    # Create a mock status to simulate a file's metadata
    mock_status: mocker.Mock = mocker.Mock()
    mock_status.getPath.return_value.toString.return_value = "/test/path"
    mock_status.getPath.return_value.getName.return_value = "test"
    mock_status.isDir.return_value = False
    mock_status.getModificationTime.return_value = 1234567890

    # Convert this mock status to DbfsFileInfo using the method
    result = DbfsFiles._file_status_to_dbfs_file_info(mock_status)  # pylint: disable=protected-access

    # Assert that the DbfsFileInfo object has the expected values
    assert result.path == "/test/path"
    assert result.name == "test"
    assert not result.is_dir
    assert result.modification_time == 1234567890
