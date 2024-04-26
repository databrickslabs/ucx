from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.tasks import (
    Task,
    parse_args,
    remove_extra_indentation,
)


def test_replace_pydoc():
    doc = remove_extra_indentation(
        """Test1
        Test2
    Test3"""
    )
    assert (
        doc
        == """Test1
    Test2
Test3"""
    )


def test_task_cloud():
    ws = create_autospec(WorkspaceClient)  # pylint: disable=mock-no-usage
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False

    tasks = [
        Task(workflow="wl_1", name="n3", doc="d3", fn=lambda: None, cloud="aws"),
        Task(workflow="wl_2", name="n2", doc="d2", fn=lambda: None, cloud="azure"),
        Task(workflow="wl_1", name="n1", doc="d1", fn=lambda: None, cloud="gcp"),
    ]

    filter_tasks = sorted([t.name for t in tasks if t.cloud_compatible(ws.config)])
    assert filter_tasks == ["n3"]


def _log_contents(tmp_path):
    contents = {}
    for path in tmp_path.glob("**/*"):
        if path.is_dir():
            continue
        contents[path.relative_to(tmp_path).as_posix()] = path.read_text()
    return contents


def test_parse_args():
    args = parse_args("--config=foo", "--task=test")
    assert args["config"] == "foo"
    assert args["task"] == "test"
    with pytest.raises(KeyError):
        parse_args("--foo=bar")
