import json

import pytest
from databricks.labs.lsql.lakeview import Dashboard as LsqlLakeviewDashboard, Dataset
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard, LegacyVisualization, LegacyQuery, Widget

from databricks.labs.ucx.assessment.dashboards import LakeviewDashboard, RedashDashboard


@pytest.mark.parametrize(
    "sdk_dashboard, expected",
    [
        (SdkRedashDashboard(id="id"), RedashDashboard("id")),
        (
            SdkRedashDashboard(
                id="did",
                name="name",
                parent="parent",
                tags=["tag1", "tag2"],
                widgets=[
                    Widget(visualization=LegacyVisualization(query=LegacyQuery(id="qid1"))),
                    Widget(visualization=LegacyVisualization(query=LegacyQuery(id="qid2"))),
                ],
            ),
            RedashDashboard("did", "name", "parent", ["qid1", "qid2"], ["tag1", "tag2"]),
        ),
        (
            SdkRedashDashboard(
                id="did",
                name="name",
                parent="parent",
                tags=["tag1", "tag2"],
                widgets=[
                    Widget(),
                    Widget(visualization=LegacyVisualization()),
                    Widget(visualization=LegacyVisualization(query=LegacyQuery(id="qid1"))),
                ],
            ),
            RedashDashboard("did", "name", "parent", ["qid1"], ["tag1", "tag2"]),
        ),
    ],
)
def test_redash_dashboard_from_sdk_dashboard(sdk_dashboard: SdkRedashDashboard, expected: RedashDashboard) -> None:
    dashboard = RedashDashboard.from_sdk_dashboard(sdk_dashboard)
    assert dashboard == expected


@pytest.mark.parametrize(
    "sdk_dashboard, expected",
    [
        (SdkLakeviewDashboard(dashboard_id="id"), LakeviewDashboard("id")),
        (
            SdkLakeviewDashboard(
                dashboard_id="did",
                display_name="name",
                parent_path="parent",
                serialized_dashboard=json.dumps(
                    LsqlLakeviewDashboard(
                        datasets=[Dataset("qid1", "SELECT 1"), Dataset("qid2", "SELECT 2")],
                        pages=[],
                    ).as_dict()
                ),
            ),
            LakeviewDashboard("did", "name", "parent", ["qid1", "qid2"]),
        ),
        (
            SdkLakeviewDashboard(
                dashboard_id="did",
                display_name="name",
                parent_path="parent",
                serialized_dashboard=json.dumps(LsqlLakeviewDashboard(datasets=[], pages=[]).as_dict()),
            ),
            LakeviewDashboard("did", "name", "parent", []),
        ),
    ],
)
def test_lakeview_dashboard_from_sdk_dashboard(
    sdk_dashboard: SdkLakeviewDashboard, expected: LakeviewDashboard
) -> None:
    dashboard = LakeviewDashboard.from_sdk_dashboard(sdk_dashboard)
    assert dashboard == expected
