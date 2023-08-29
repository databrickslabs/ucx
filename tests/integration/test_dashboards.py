import logging
import os

from databricks.sdk.service.sql import AccessControl, ObjectTypePlural, PermissionLevel

from databricks.labs.ucx.providers.mixins.redash import (
    DashboardsExt,
    Position,
    WidgetOptions, VizColumn,
)

# os.environ['DATABRICKS_DEBUG_TRUNCATE_BYTES'] = '2048'
logging.getLogger("databricks").setLevel("DEBUG")


def test_dash2(ws):
    da = DashboardsExt(ws.api_client)
    x = da.create(name="foobar")
    ws.dbsql_permissions.set(
        ObjectTypePlural.DASHBOARDS,
        x.id,
        access_control_list=[AccessControl(group_name="users", permission_level=PermissionLevel.CAN_MANAGE)],
    )
    da.add_widget(
        x.id,
        WidgetOptions(
            title="abc",
            description="aaa",
            position=Position(
                col=0,
                row=0,
                sizeX=3,
                sizeY=3,
            ),
        ),
        text="this is some markdown",
    )

    da.add_widget(
        x.id,
        WidgetOptions(
            title="abc",
            description="aaa",
            position=Position(
                col=0,
                row=3,
                sizeX=3,
                sizeY=3,
            ),
        ),
        text="and this as well",
    )

    data_sources = {x.warehouse_id: x.id for x in ws.data_sources.list()}
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    query = ws.queries.create(
        data_source_id=data_sources[warehouse_id],
        description="abc",
        name="this is a test query",
        query="SHOW DATABASES",
    )

    viz = da.add_table_viz(query.id, 'ABC', [VizColumn(name='databaseName', title='DB')])

    da.add_widget(
        x.id,
        WidgetOptions(
            title="XXX",
            position=Position(
                col=3,
                row=0,
                sizeX=3,
                sizeY=6,
            ),
        ),
        visualization_id=viz.id,
    )

    print(x)


def test_dash(ws):
    ws.config.debug_truncate_bytes = 204800
    d = ws.dashboards.get("e1fe28f5-4245-4215-9882-f218d08ec726")
    print(d)
