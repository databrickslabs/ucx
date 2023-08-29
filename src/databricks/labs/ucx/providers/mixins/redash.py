import dataclasses
from dataclasses import dataclass
from typing import TypedDict

from databricks.sdk.service.sql import Dashboard, DashboardsAPI, Visualization


class Position(TypedDict):
    autoHeight: bool  # False
    sizeX: int  # 3
    sizeY: int  # 3
    minSizeX: int  # 1,
    maxSizeX: int  # 6,
    minSizeY: int  # 1,
    maxSizeY: int  # 1000,
    col: int  # 0,
    row: int  # 0,


class WidgetOptions(TypedDict):
    position: Position
    title: str
    description: str


@dataclass
class VizColumn:
    name: str
    title: str

    type: str = "string"
    imageUrlTemplate: str = "{{ @ }}"
    imageTitleTemplate: str = "{{ @ }}"
    linkUrlTemplate: str = "{{ @ }}"
    linkTextTemplate: str = "{{ @ }}"
    linkTitleTemplate: str = "{{ @ }}"
    linkOpenInNewTab: bool = True
    displayAs: str = "string"
    visible: bool = True
    order: int = 100000
    allowSearch: bool = False
    alignContent: str = "left"
    allowHTML: bool = False
    highlightLinks: bool = False
    useMonospaceFont: bool = False
    preserveWhitespace: bool = False

    def as_dict(self):
        return dataclasses.asdict(self)


class DashboardsExt(DashboardsAPI):
    def create(
        self,
        *,
        is_favorite: bool | None = None,
        name: str | None = None,
        parent: str | None = None,
        tags: list[str] | None = None,
    ) -> Dashboard:
        body = {}
        if is_favorite is not None:
            body["is_favorite"] = is_favorite
        if name is not None:
            body["name"] = name
        if parent is not None:
            body["parent"] = parent
        if tags is not None:
            body["tags"] = list(tags)
        body["run_as_role"] = "viewer"
        body["dashboard_filters_enabled"] = False
        json = self._api.do("POST", "/api/2.0/preview/sql/dashboards", body=body)
        return Dashboard.from_dict(json)

    def add_widget(
        self,
        dashboard_id: str,
        options: WidgetOptions = None,
        *,
        text: str | None = None,
        visualization_id: str | None = None,
        width: int = 1,
    ):
        body = {}
        body["dashboard_id"] = dashboard_id
        body["options"] = options
        if text is not None:
            body["text"] = text
        if width is not None:
            body["width"] = width
        if visualization_id is not None:
            body["visualization_id"] = visualization_id
        self._api.do("POST", "/api/2.0/preview/sql/widgets", body=body)

    def add_viz(self, query_id: str, name: str, type: str, options: dict, *, description: str | None = None):
        body = {"query_id": query_id, "name": name, "type": type, "options": options}
        if description is not None:
            body["description"] = description
        res = self._api.do("POST", "/api/2.0/preview/sql/visualizations", body=body)
        return Visualization.from_dict(res)

    def add_table_viz(
        self,
        query_id: str,
        name: str,
        columns: list[VizColumn],
        *,
        itemsPerPage: int = 25,
        condensed=True,
        withRowNumber=False,
        description: str | None = None,
    ):
        return self.add_viz(
            query_id,
            name,
            "TABLE",
            options={
                "itemsPerPage": itemsPerPage,
                "condensed": condensed,
                "withRowNumber": withRowNumber,
                "version": 2,
                "columns": [x.as_dict() for x in columns],
            },
            description=description,
        )
