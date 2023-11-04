import io
from dataclasses import dataclass
from pathlib import Path

import yaml
from databricks.sdk.service import iam
from databricks.sdk.service.sql import (
    Dashboard,
    DataSource,
    Query,
    Visualization,
    Widget,
)

from databricks.labs.ucx.config import GroupsConfig, WorkspaceConfig
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.install import WorkspaceInstaller


@dataclass
class BBox:
    name: str
    row: int
    col: int
    height: int
    width: int

    def intersect(self, other) -> bool:
        return not (
            self.col + self.width - 1 < other.col
            or self.col > other.col + other.width - 1
            or self.row + self.height - 1 < other.row
            or self.row > other.row + other.width - 1
        )

    def __lt__(self, other):
        return self.row < other.row or self.col < other.col

    def __repr__(self):
        return f"r{self.row: >2} c{self.col: >2} h{self.height:0>2} w{self.width:0>2} {self.name: >25}"


def get_mocker(mocker):
    ws = mocker.Mock()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")])
    ws.config.host = "https://foo"
    ws.config.is_aws = True
    config_bytes = yaml.dump(WorkspaceConfig(inventory_database="a", groups=GroupsConfig(auto=True)).as_dict()).encode(
        "utf8"
    )
    ws.workspace.download = lambda _: io.BytesIO(config_bytes)
    ws.data_sources.list = lambda: [DataSource(id="bcd", warehouse_id="000000")]
    ws.dashboards.create.return_value = Dashboard(id="abc")
    ws.queries.create.return_value = Query(id="abc")
    ws.query_visualizations.create.return_value = Visualization(id="abc")
    ws.dashboard_widgets.create.return_value = Widget(id="abc")
    installer = WorkspaceInstaller(ws)
    local_query_files = installer._find_project_root() / "src/databricks/labs/ucx/queries"
    return ws, local_query_files, installer


def test_dashboard(mocker):
    ws, local_query_files, installer = get_mocker(mocker)
    dash = DashboardFromFiles(
        ws,
        local_folder=local_query_files,
        remote_folder="/users/not_a_real_user/queries",
        name_prefix="Assessment",
        warehouse_id="000000",
        query_text_callback=installer._current_config.replace_inventory_variable,
    )
    dashboards = dash.create_dashboards()
    assert dashboards is not None
    assert dashboards["assessment_main"] == "abc"
    assert dashboards["assessment_azure"] == "abc"


def test_dashboard_layout(mocker):
    """Cursory check of dashboard widget location overlaps"""

    ws, local_query_files, installer = get_mocker(mocker)
    dash = DashboardFromFiles(
        ws,
        local_folder=local_query_files,
        remote_folder="/users/not_a_real_user/queries",
        name_prefix="Assessment",
        warehouse_id="000000",
        query_text_callback=installer._current_config.replace_inventory_variable,
    )

    path = "./src/databricks/labs/ucx/queries/assessment/main"
    key = "assessment_main"
    queries = dash._desired_queries(Path(path), key)
    assert queries is not None
    assert len(queries) > 10

    boxes = []
    for q in queries:
        bbox = BBox(
            name=q.name,
            row=int(q.widget.get("row")),
            col=int(q.widget.get("col")),
            height=int(q.widget.get("size_y")),
            width=int(q.widget.get("size_x")),
        )
        boxes.append(bbox)

    boxes = sorted(boxes)
    print()
    [print(f"{_}") for _ in boxes]
    intersects = []
    for i, box1 in enumerate(boxes):
        for j in range(i + 1, len(boxes)):
            box2 = boxes[j]
            if box1.intersect(box2):
                intersects.append(f"{box1} intersects {box2}")

    print()
    [print(f"{_}") for _ in intersects]
    assert [] == intersects
