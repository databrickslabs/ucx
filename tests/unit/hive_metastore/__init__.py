from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient


@pytest.fixture
def ws():
    client = create_autospec(WorkspaceClient)
    client.get_workspace_id.return_value = "12345"
    return client
