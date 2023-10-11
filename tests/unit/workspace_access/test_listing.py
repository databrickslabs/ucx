import datetime as dt
from unittest.mock import MagicMock, Mock, patch

from databricks.sdk.service import workspace
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

from databricks.labs.ucx.workspace_access import generic, listing


def test_logging_calls():
    ws = MagicMock()
    workspace_listing = listing.WorkspaceListing(ws=ws, num_threads=1)
    workspace_listing.start_time = dt.datetime.now()
    workspace_listing._counter = 9
    # with patch.object(logger, "info") as mock_info:
    #     workspace_listing._progress_report(None)
    #     mock_info.assert_called_once()


def test_workspace_listing():
    listing_instance = MagicMock(spec=listing.WorkspaceListing)
    listing_instance.walk.return_value = [
        workspace.ObjectInfo(object_id=1, object_type=workspace.ObjectType.NOTEBOOK),
        workspace.ObjectInfo(object_id=2, object_type=workspace.ObjectType.DIRECTORY),
        workspace.ObjectInfo(object_id=3, object_type=workspace.ObjectType.LIBRARY),
        workspace.ObjectInfo(object_id=4, object_type=workspace.ObjectType.REPO),
        workspace.ObjectInfo(object_id=5, object_type=workspace.ObjectType.FILE),
        workspace.ObjectInfo(object_id=6, object_type=None),  # MLflow Experiment
    ]

    with patch("databricks.labs.ucx.workspace_access.listing.WorkspaceListing", return_value=listing_instance):
        results = generic.WorkspaceListing(ws=MagicMock())
        assert len(list(results)) == 4
        listing_instance.walk.assert_called_once()
        for res in results:
            assert res.request_type in [
                "notebooks",
                "directories",
                "repos",
                "files",
            ]
            assert int(res.object_id) in [1, 2, 4, 5]


# Helper to compare an unordered list of objects
def compare(s, t):
    t = list(t)  # make a mutable copy
    try:
        for elem in s:
            t.remove(elem)
    except ValueError:
        return False
    return not t


def test_list_and_analyze_should_separate_folders_and_other_objects():
    rootobj = ObjectInfo(path="/rootPath")

    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)
    directory = ObjectInfo(path="/rootPath/directory", object_type=ObjectType.DIRECTORY)
    notebook = ObjectInfo(path="/rootPath/notebook", object_type=ObjectType.NOTEBOOK)

    client = Mock()
    client.workspace.list.return_value = [file, directory, notebook]

    listing_instance = listing.WorkspaceListing(client, 1)
    directories, others = listing_instance._list_and_analyze(rootobj)

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

    def my_side_effect(path, **kwargs):
        if path == "/rootPath":
            return [file, nested_folder]
        elif path == "/rootPath/nested_folder":
            return [nested_notebook]

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

    def my_side_effect(path, **kwargs):
        if path == "/rootPath":
            return [file, nested_folder]
        elif path == "/rootPath/nested_folder":
            return [nested_notebook, second_nested_folder]
        elif path == "/rootPath/nested_folder/second_nested_folder":
            return [second_nested_notebook]

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
