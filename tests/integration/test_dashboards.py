import logging
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import AccessControl, ObjectTypePlural, PermissionLevel

from databricks.labs.ucx.providers.mixins.redash import (
    QueryVisualizationsExt,
    DashboardWidgetsAPI,
    WidgetPosition,
    WidgetOptions, VizColumn,
)

# os.environ['DATABRICKS_DEBUG_TRUNCATE_BYTES'] = '2048'
logging.getLogger("databricks").setLevel("DEBUG")


def test_creating_widgets(ws: WorkspaceClient):
    dashboard_widgets_api = DashboardWidgetsAPI(ws.api_client)
    query_visualizations_api = QueryVisualizationsExt(ws.api_client)

    x = ws.dashboards.create(name='test dashboard')
    ws.dbsql_permissions.set(
        ObjectTypePlural.DASHBOARDS,
        x.id,
        access_control_list=[AccessControl(group_name="users", permission_level=PermissionLevel.CAN_MANAGE)],
    )

    dashboard_widgets_api.create(x.id, WidgetOptions(
        title='first widget',
        description='description of the widget',
        position=WidgetPosition(col=0, row=0, size_x=3, size_y=3)
    ), text='this is _some_ **markdown**', width=1)

    dashboard_widgets_api.create(x.id, WidgetOptions(
        title='second',
        position=WidgetPosition(col=0, row=3, size_x=3, size_y=3)
    ), text='another text', width=1)

    data_sources = {x.warehouse_id: x.id for x in ws.data_sources.list()}
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    query = ws.queries.create(
        data_source_id=data_sources[warehouse_id],
        description="abc",
        name="this is a test query",
        query="SHOW DATABASES",
        run_as_role='viewer'
    )

    y = query_visualizations_api.create_table(query.id, 'ABC Viz', [VizColumn(name='databaseName', title='DB')])
    print(y)


def test_dash(ws):
    ws.config.debug_truncate_bytes = 204800
    d = ws.dashboards.get("e1fe28f5-4245-4215-9882-f218d08ec726")
    print(d)
