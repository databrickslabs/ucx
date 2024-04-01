import dataclasses
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from databricks.labs.blueprint.installer import InstallState
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service import workspace
from databricks.sdk.service.sql import (
    AccessControl,
    ObjectTypePlural,
    PermissionLevel,
    RunAsRole,
    WidgetOptions,
    WidgetPosition,
)

logger = logging.getLogger(__name__)

# pylint: disable=invalid-name


@dataclass
class SimpleQuery:
    dashboard_ref: str
    name: str
    widget: dict[str, str]
    viz: dict[str, str]
    query: str | None = None
    text: str | None = None

    @property
    def key(self):
        return f"{self.dashboard_ref}_{self.name}"

    @property
    def viz_type(self) -> str:
        return self.viz.get("type", "UNKNOWN")

    @property
    def viz_args(self) -> dict:
        return {k: v for k, v in self.viz.items() if k not in ["type"]}


@dataclass
class VizColumn:  # pylint: disable=too-many-instance-attributes)
    name: str
    title: str
    type: str = "string"
    imageUrlTemplate: str = "{{ @ }}"  # noqa: N815
    imageTitleTemplate: str = "{{ @ }}"  # noqa: N815
    linkUrlTemplate: str = "{{ @ }}"  # noqa: N815
    linkTextTemplate: str = "{{ @ }}"  # noqa: N815
    linkTitleTemplate: str = "{{ @ }}"  # noqa: N815
    linkOpenInNewTab: bool = True  # noqa: N815
    displayAs: str = "string"  # noqa: N815
    visible: bool = True
    order: int = 100000
    allowSearch: bool = False  # noqa: N815
    alignContent: str = "left"  # noqa: N815
    allowHTML: bool = False  # noqa: N815
    highlightLinks: bool = False  # noqa: N815
    useMonospaceFont: bool = False  # noqa: N815
    preserveWhitespace: bool = False  # noqa: N815

    def as_dict(self):
        return dataclasses.asdict(self)


class DashboardFromFiles:
    def __init__(
        self,
        ws: WorkspaceClient,
        state: InstallState,
        local_folder: Path,
        remote_folder: str,
        name_prefix: str,
        query_text_callback: Callable[[str], str] | None = None,
        warehouse_id: str | None = None,
    ):
        self._ws = ws
        self._local_folder = local_folder
        self._remote_folder = remote_folder
        self._name_prefix = name_prefix
        self._query_text_callback = query_text_callback
        self._warehouse_id = warehouse_id
        self._state = state
        self._pos = 0

    def dashboard_link(self, dashboard_ref: str):
        dashboard_id = self._state.dashboards[dashboard_ref]
        return f"{self._ws.config.host}/sql/dashboards/{dashboard_id}"

    def create_dashboards(self) -> dict[str, str]:
        queries_per_dashboard = {}
        # Iterate over dashboards for each step, represented as first-level folders
        step_folders = [f for f in self._local_folder.glob("*") if f.is_dir()]
        for step_folder in step_folders:
            logger.debug(f"Reading step folder {step_folder}...")
            dashboard_folders = [f for f in step_folder.glob("*") if f.is_dir()]
            # Create separate dashboards per step, represented as second-level folders
            for dashboard_folder in dashboard_folders:
                logger.debug(f"Reading dashboard folder {dashboard_folder}...")
                main_name = step_folder.stem.title()
                sub_name = dashboard_folder.stem.title()
                dashboard_name = f"{self._name_prefix} {main_name} ({sub_name})"
                dashboard_ref = f"{step_folder.stem}_{dashboard_folder.stem}".lower()
                logger.info(f"Creating dashboard {dashboard_name}...")
                desired_queries = self._desired_queries(dashboard_folder, dashboard_ref)
                parent_folder_id = self._installed_query_state()
                data_source_id = self._dashboard_data_source()
                self._install_dashboard(dashboard_name, parent_folder_id, dashboard_ref)
                for query in desired_queries:
                    self._install_query(query, dashboard_name, data_source_id, parent_folder_id)
                    self._install_viz(query)
                    self._install_widget(query, dashboard_ref)
                queries_per_dashboard[dashboard_ref] = desired_queries
        self._store_query_state(queries_per_dashboard)
        return self._state.dashboards

    def validate(self):
        step_folders = [f for f in self._local_folder.glob("*") if f.is_dir()]
        for step_folder in step_folders:
            logger.info(f"Reading step folder {step_folder}...")
            dashboard_folders = [f for f in step_folder.glob("*") if f.is_dir()]
            # Create separate dashboards per step, represented as second-level folders
            for dashboard_folder in dashboard_folders:
                self._validate_folder(dashboard_folder, step_folder)

    def _validate_folder(self, dashboard_folder, step_folder):
        dashboard_ref = f"{step_folder.stem}_{dashboard_folder.stem}".lower()
        for query in self._desired_queries(dashboard_folder, dashboard_ref):
            if query.text:
                continue
            try:
                self._get_viz_options(query)
                self._get_widget_options(query)
            except Exception as err:
                msg = f"Error in {query.name}: {err}"
                raise AssertionError(msg) from err

    def _install_widget(self, query: SimpleQuery, dashboard_ref: str):
        dashboard_id = self._state.dashboards[dashboard_ref]
        widget_options = self._get_widget_options(query)
        # widgets are cleaned up every dashboard redeploy
        if query.query:
            widget = self._ws.dashboard_widgets.create(
                dashboard_id, widget_options, 1, visualization_id=self._state.viz[query.key]
            )
        elif query.text:
            text = query.text[query.text.index("\n") + 1 :]
            widget = self._ws.dashboard_widgets.create(dashboard_id, widget_options, 1, text=text)
        else:
            raise ValueError("Query or Text should be set")
        assert widget.id is not None
        self._state.widgets[query.key] = widget.id

    def _get_widget_options(self, query: SimpleQuery):
        self._pos += 1
        widget_options = WidgetOptions(
            title=query.widget.get("title", ""),
            description=query.widget.get("description", None),
            position=WidgetPosition(
                col=int(query.widget.get("col", 0)),
                row=int(query.widget.get("row", self._pos)),
                size_x=int(query.widget.get("size_x", 3)),
                size_y=int(query.widget.get("size_y", 3)),
            ),
        )
        return widget_options

    def _remote_folder_object(self) -> workspace.ObjectInfo:
        try:
            return self._ws.workspace.get_status(self._remote_folder)
        except NotFound:
            self._ws.workspace.mkdirs(self._remote_folder)
            return self._remote_folder_object()

    def _installed_query_state(self):
        object_info = self._remote_folder_object()
        parent = f"folders/{object_info.object_id}"
        return parent

    def _store_query_state(self, queries_per_dashboard: dict[str, list[SimpleQuery]]):  # pylint: disable=too-complex
        query_refs = set()
        dashboard_refs = queries_per_dashboard.keys()
        for queries in queries_per_dashboard.values():
            for query in queries:
                query_refs.add(query.key)

        def silent_destroy(fn, object_id):
            try:
                fn(object_id)
            except DatabricksError as err:
                logger.info(f"Failed to delete {object_id} --- {err.error_code}")

        for ref, object_id in self._state.dashboards.items():
            if ref in dashboard_refs:
                continue
            silent_destroy(self._ws.dashboards.delete, object_id)

        for ref, object_id in self._state.queries.items():
            if ref in query_refs:
                continue
            silent_destroy(self._ws.queries.delete, object_id)

        for ref, object_id in self._state.viz.items():
            if ref in query_refs:
                continue
            silent_destroy(self._ws.query_visualizations.delete, object_id)

        for ref, object_id in self._state.widgets.items():
            if ref in query_refs:
                continue
            silent_destroy(self._ws.dashboard_widgets.delete, object_id)

        self._state.save()

    def _install_dashboard(self, dashboard_name: str, parent_folder_id: str, dashboard_ref: str):
        if dashboard_ref in self._state.dashboards:
            dashboard = self._ws.dashboards.get(self._state.dashboards[dashboard_ref])
            assert dashboard.widgets is not None
            for widget in dashboard.widgets:
                assert widget.id is not None
                try:
                    self._ws.dashboard_widgets.delete(widget.id)
                except TypeError:
                    pass
                    # TODO: Tracking bug in ES-1061370. Remove after fix
            return
        dashboard = self._ws.dashboards.create(dashboard_name, run_as_role=RunAsRole.VIEWER, parent=parent_folder_id)
        assert dashboard.id is not None
        self._ws.dbsql_permissions.set(
            ObjectTypePlural.DASHBOARDS,
            dashboard.id,
            access_control_list=[AccessControl(group_name="users", permission_level=PermissionLevel.CAN_VIEW)],
        )
        self._state.dashboards[dashboard_ref] = dashboard.id

    def _desired_queries(self, local_folder: Path, dashboard_ref: str) -> list[SimpleQuery]:
        desired_queries = []
        for f in local_folder.glob("*.sql"):
            text = f.read_text("utf8")
            if self._query_text_callback is not None:
                text = self._query_text_callback(text)
            desired_queries.append(
                SimpleQuery(
                    dashboard_ref=dashboard_ref,
                    name=f.name,
                    query=text,
                    viz=self._parse_magic_comment(f, "-- viz ", text),
                    widget=self._parse_magic_comment(f, "-- widget ", text),
                )
            )
        for f in local_folder.glob("*.md"):
            if f.name == "README.md":
                continue
            text = f.read_text("utf8")
            desired_queries.append(
                SimpleQuery(
                    dashboard_ref=dashboard_ref,
                    name=f.name,
                    text=text,
                    widget=self._parse_magic_comment(f, "-- widget ", text),
                    viz={},
                )
            )
        return desired_queries

    def _install_viz(self, query: SimpleQuery):
        if query.text:
            logger.debug(f"Skipping viz {query.name} because it's a text widget")
            return None
        viz_args = self._get_viz_options(query)
        if query.key in self._state.viz:
            return self._ws.query_visualizations.update(self._state.viz[query.key], **viz_args)
        viz = self._ws.query_visualizations.create(self._state.queries[query.key], **viz_args)
        assert viz.id is not None
        self._state.viz[query.key] = viz.id
        return None

    def _get_viz_options(self, query: SimpleQuery):
        viz_types: dict[str, Callable[..., dict]] = {"table": self._table_viz_args, "counter": self._counter_viz_args}
        if query.viz_type not in viz_types:
            msg = f"{query.query}: unknown viz type: {query.viz_type}"
            raise SyntaxError(msg)
        viz_args = viz_types[query.viz_type](**query.viz_args)
        return viz_args

    def _install_query(self, query: SimpleQuery, dashboard_name: str, data_source_id: str, parent: str):
        if query.text:
            logger.debug(f"Skipping query {query.name} because it's a text widget")
            return None
        query_meta = {
            "data_source_id": data_source_id,
            "name": f"{dashboard_name} - {query.name}",
            "query": query.query,
        }
        if query.key in self._state.queries:
            return self._ws.queries.update(self._state.queries[query.key], **query_meta, run_as_role=None)

        deployed_query = self._ws.queries.create(parent=parent, run_as_role=RunAsRole.VIEWER, **query_meta)
        assert deployed_query.id is not None
        self._ws.dbsql_permissions.set(
            ObjectTypePlural.QUERIES,
            deployed_query.id,
            access_control_list=[AccessControl(group_name="users", permission_level=PermissionLevel.CAN_RUN)],
        )
        self._state.queries[query.key] = deployed_query.id
        return None

    @staticmethod
    def _table_viz_args(
        name: str,
        columns: str,
        *,
        items_per_page: int = 25,
        condensed=True,
        with_row_number=False,
        description: str | None = None,
        search_by: str | None = None,
    ) -> dict:
        return {
            "type": "TABLE",
            "name": name,
            "description": description,
            "options": {
                "itemsPerPage": items_per_page,
                "condensed": condensed,
                "withRowNumber": with_row_number,
                "version": 2,
                "columns": [
                    VizColumn(name=x, title=x, allowSearch=x == search_by).as_dict() for x in columns.split(",")
                ],
            },
        }

    @staticmethod
    def _counter_viz_args(  # pylint: disable=too-many-arguments
        name: str,
        value_column: str,
        *,
        description: str | None = None,
        counter_label: str | None = None,
        value_row_number: int = 1,
        target_row_number: int = 1,
        string_decimal: int = 0,
        string_decimal_char: str = ".",
        string_thousand_separator: str = ",",
        tooltip_format: str = "0,0.000",
        count_row: bool = False,
    ) -> dict:
        return {
            "type": "COUNTER",
            "name": name,
            "description": description,
            "options": {
                "counterLabel": counter_label,
                "counterColName": value_column,
                "rowNumber": value_row_number,
                "targetRowNumber": target_row_number,
                "stringDecimal": string_decimal,
                "stringDecChar": string_decimal_char,
                "stringThouSep": string_thousand_separator,
                "tooltipFormat": tooltip_format,
                "countRow": count_row,
            },
        }

    @staticmethod
    def _parse_magic_comment(f, magic_comment, text):
        viz_comment = next(_ for _ in text.splitlines() if _.startswith(magic_comment))
        if not viz_comment:
            msg = f'{f}: cannot find "{magic_comment}" magic comment'
            raise SyntaxError(msg)
        return dict(_.split("=") for _ in viz_comment.replace(magic_comment, "").split(", "))

    def _dashboard_data_source(self) -> str:
        data_sources = {_.warehouse_id: _.id for _ in self._ws.data_sources.list()}
        warehouses = list(self._ws.warehouses.list())
        warehouse_id = self._warehouse_id
        if not warehouse_id and not warehouses:
            msg = "need either configured warehouse_id or an existing SQL warehouse"
            raise ValueError(msg)
        if not warehouse_id:
            warehouse_id = warehouses[0].id
        data_source_id = data_sources[warehouse_id]
        return data_source_id
