from unittest.mock import MagicMock

from databricks.sdk.service import iam

from databricks.labs.ucx.support.group_level import ScimSupport


def test_scim_crawler():
    ws = MagicMock()
    ws.groups.list.return_value = [
        iam.Group(
            id="1",
            display_name="group1",
            roles=[],  # verify that empty roles and entitlements are not returned
        ),
        iam.Group(
            id="2",
            display_name="group2",
            roles=[iam.ComplexValue(value="role1")],
            entitlements=[iam.ComplexValue(value="entitlement1")],
        ),
        iam.Group(
            id="3",
            display_name="group3",
            roles=[iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")],
            entitlements=[],
        ),
    ]
    sup = ScimSupport(ws=ws)
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 3
    ws.groups.list.assert_called_once()
