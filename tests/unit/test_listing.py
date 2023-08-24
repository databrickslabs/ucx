from databricks.labs.ucx.inventory.listing import WorkspaceListing
from databricks.sdk.service.workspace import ObjectInfo, ObjectType
from unittest.mock import Mock


def test_list_and_analyze_should_separate_folders_and_other_objects():
    rootobj = ObjectInfo(path="/rootPath")

    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)
    directory = ObjectInfo(path="/rootPath/directory", object_type=ObjectType.DIRECTORY)
    notebook = ObjectInfo(path="/rootPath/notebook", object_type=ObjectType.NOTEBOOK)

    client = Mock()
    client.list_workspace.return_value = [file, directory, notebook]

    listing = WorkspaceListing(client, 1)
    directories, others = listing._list_and_analyze(rootobj)

    assert others == [file, notebook]
    assert directories == [directory]

def test_crawl():
    rootobj = ObjectInfo(path="/rootPath")

    file = ObjectInfo(path="/rootPath/file1", object_type=ObjectType.FILE)
    directory = ObjectInfo(path="/rootPath/directory", object_type=ObjectType.DIRECTORY)
    notebook = ObjectInfo(path="/rootPath/notebook", object_type=ObjectType.NOTEBOOK)

    client = Mock()
    client.list_workspace.return_value = [file, directory, notebook]
    client.workspace.get_status.return_value = rootobj

    listing = WorkspaceListing(client, 1)
    listing.walk_non_threaded("/rootPath")