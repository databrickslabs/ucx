import datetime as dt
from unittest.mock import MagicMock, Mock, patch

from databricks.sdk.errors import InternalError, ResourceDoesNotExist
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

from databricks.labs.ucx.workspace_access import generic, listing


def test_workspace_listing():
    listing_instance = [
        generic.WorkspaceObjectInfo(object_type="NOTEBOOK", object_id=1, path="", language="PYTHON"),
        generic.WorkspaceObjectInfo(object_type="DIRECTORY", object_id=2, path="", language=""),
        generic.WorkspaceObjectInfo(object_type="LIBRARY", object_id=3, path="", language=""),
        generic.WorkspaceObjectInfo(object_type="REPO", object_id=4, path="", language=""),
        generic.WorkspaceObjectInfo(object_type="FILE", object_id=5, path="", language=""),
        generic.WorkspaceObjectInfo(object_type=None, object_id=6, path="", language=""),  # MLflow Experiment
    ]

    # TODO: there's a huge chance that we'll be rewriting this code to use WSFS FUSE listing, so it'll be irrelevant
    # pylint: disable-next=explicit-dependency-required
    with patch("databricks.labs.ucx.workspace_access.generic.WorkspaceListing.snapshot", return_value=listing_instance):
        # pylint: disable-next=obscure-mock
        results = generic.WorkspaceListing(ws=MagicMock(), sql_backend=MagicMock(), inventory_database=MagicMock())
        assert len(list(results)) == 4
        for res in results:
            assert res.request_type in {
                "notebooks",
                "directories",
                "repos",
                "files",
            }
            assert int(res.object_id) in {1, 2, 4, 5}


# Helper to compare an unordered list of objects
def compare(i, j):
    j = list(j)  # make a mutable copy
    try:
        for elem in i:
            j.remove(elem)
    except ValueError:
        return False
    return not j


def test_list_and_analyze_should_separate_folders_and_other_objects():
    rootobj = ObjectInfo(path="/rootPath")

    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)
    directory = ObjectInfo(path="/rootPath/directory", object_type=ObjectType.DIRECTORY)
    notebook = ObjectInfo(path="/rootPath/notebook", object_type=ObjectType.NOTEBOOK)

    client = Mock()
    client.workspace.list.return_value = [file, directory, notebook]

    listing_instance = listing.WorkspaceListing(client, 1)
    directories, others = listing_instance._list_and_analyze(rootobj)  # pylint: disable=protected-access

    assert compare(others, [file, notebook])
    assert compare(directories, [directory])


def test_walk_with_an_empty_folder_should_return_it():
    rootobj = ObjectInfo(path="/rootPath")

    client = Mock()
    client.workspace.list.return_value = []
    client.workspace.get_status.return_value = rootobj

    listing_instance = listing.WorkspaceListing(client, 1)
    listing_instance.walk("/rootPath")

    assert len(listing_instance.results) == 1
    assert listing_instance.results == [rootobj]


def test_walk_with_resource_missing(caplog):
    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)

    client = Mock()
    client.workspace.list.return_value = [file]
    client.workspace.get_status.side_effect = ResourceDoesNotExist("RESOURCE_DOES_NOT_EXIST")

    listing_instance = listing.WorkspaceListing(client, 1)
    listing_instance.walk("/rootPath")

    assert len(listing_instance.results) == 0
    assert "removed on the backend /rootPath" in caplog.messages


def test_walk_with_two_files_should_return_rootpath_and_two_files():
    rootobj = ObjectInfo(path="/rootPath")
    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)
    notebook = ObjectInfo(path="/rootPath/notebook", object_type=ObjectType.NOTEBOOK)

    client = Mock()
    client.workspace.list.return_value = [file, notebook]
    client.workspace.get_status.return_value = rootobj

    listing_instance = listing.WorkspaceListing(client, 1)
    listing_instance.walk("/rootPath")

    assert len(listing_instance.results) == 3
    assert compare(listing_instance.results, [rootobj, file, notebook])


def test_walk_with_nested_folders_should_return_nested_objects():
    rootobj = ObjectInfo(path="/rootPath")
    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)
    nested_folder = ObjectInfo(path="/rootPath/nested_folder", object_type=ObjectType.DIRECTORY)
    nested_notebook = ObjectInfo(path="/rootPath/nested_folder/notebook", object_type=ObjectType.NOTEBOOK)

    def my_side_effect(path, **_):
        if path == "/rootPath":
            return [file, nested_folder]
        if path == "/rootPath/nested_folder":
            return [nested_notebook]
        return None

    client = Mock()
    client.workspace.list.side_effect = my_side_effect
    client.workspace.get_status.return_value = rootobj

    listing_instance = listing.WorkspaceListing(client, 1)
    listing_instance.walk("/rootPath")

    assert len(listing_instance.results) == 4
    assert compare(listing_instance.results, [rootobj, file, nested_folder, nested_notebook])


def test_walk_with_three_level_nested_folders_returns_three_levels():
    rootobj = ObjectInfo(path="/rootPath")
    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)
    nested_folder = ObjectInfo(path="/rootPath/nested_folder", object_type=ObjectType.DIRECTORY)
    nested_notebook = ObjectInfo(path="/rootPath/nested_folder/notebook", object_type=ObjectType.NOTEBOOK)
    second_nested_folder = ObjectInfo(
        path="/rootPath/nested_folder/second_nested_folder", object_type=ObjectType.DIRECTORY
    )
    second_nested_notebook = ObjectInfo(
        path="/rootPath/nested_folder/second_nested_folder/notebook2", object_type=ObjectType.NOTEBOOK
    )

    def my_side_effect(path, **_):
        if path == "/rootPath":
            return [file, nested_folder]
        if path == "/rootPath/nested_folder":
            return [nested_notebook, second_nested_folder]
        if path == "/rootPath/nested_folder/second_nested_folder":
            return [second_nested_notebook]
        return None

    client = Mock()
    client.workspace.list.side_effect = my_side_effect
    client.workspace.get_status.return_value = rootobj
    listing_instance = listing.WorkspaceListing(client, 2)
    listing_instance.walk("/rootPath")

    assert len(listing_instance.results) == 6
    assert compare(
        listing_instance.results,
        [rootobj, file, nested_folder, nested_notebook, second_nested_folder, second_nested_notebook],
    )


def test_walk_should_retry_on_backend_exceptions_and_log_them():
    rootobj = ObjectInfo(path="/rootPath")
    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)
    first_folder = ObjectInfo(path="/rootPath/nested_folder", object_type=ObjectType.DIRECTORY)
    second_folder = ObjectInfo(path="/rootPath/nested_folder_2", object_type=ObjectType.DIRECTORY)
    second_folder_notebook = ObjectInfo(path="/rootPath/nested_folder_2/notebook2", object_type=ObjectType.NOTEBOOK)

    def my_side_effect(path, **_):
        if path == "/rootPath":
            return [file, first_folder, second_folder]
        if path == "/rootPath/nested_folder":
            raise InternalError(message="Backend dead")
        if path == "/rootPath/nested_folder_2":
            return [second_folder_notebook]
        return None

    client = Mock()
    client.workspace.list.side_effect = my_side_effect
    client.workspace.get_status.return_value = rootobj
    listing_instance = listing.WorkspaceListing(client, 2, verify_timeout=dt.timedelta(seconds=1))
    listing_instance.walk("/rootPath")

    assert len(listing_instance.results) == 5
    assert compare(
        listing_instance.results,
        [rootobj, file, first_folder, second_folder, second_folder_notebook],
    )
