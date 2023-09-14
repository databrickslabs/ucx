from unittest.mock import Mock

from databricks.labs.ucx.assessment.assessment import AssessmentToolkit


def test_get_command():
    def _mock_load(name):
        return f"This is a test! Given name: {name}"

    ws = Mock()
    at = AssessmentToolkit(ws, cluster_id=1, inventory_catalog="foo", inventory_schema="bar")
    at._load_command_code = _mock_load
    c = at._get_command("ignored", {"test": "WIN"})
    assert c.startswith("This is a WIN!")
