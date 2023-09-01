import dataclasses
from dataclasses import dataclass
from typing import TypedDict, Optional, Any, Dict

from databricks.sdk.service._internal import Wait, _enum, _from_dict, _repeated
from databricks.sdk.service.sql import Dashboard, DashboardsAPI, Visualization, Widget


@dataclass
class WidgetOptions:
    created_at: Optional[str] = None
    description: Optional[str] = None
    is_hidden: Optional[bool] = None
    parameter_mappings: Optional[Any] = None
    position: Optional['WidgetPosition'] = None
    title: Optional[str] = None
    updated_at: Optional[str] = None

    def as_dict(self) -> dict:
        body = {}
        if self.created_at is not None: body['created_at'] = self.created_at
        if self.description is not None: body['description'] = self.description
        if self.is_hidden is not None: body['isHidden'] = self.is_hidden
        if self.parameter_mappings: body['parameterMappings'] = self.parameter_mappings
        if self.position: body['position'] = self.position.as_dict()
        if self.title is not None: body['title'] = self.title
        if self.updated_at is not None: body['updated_at'] = self.updated_at
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, any]) -> 'WidgetOptions':
        return cls(created_at=d.get('created_at', None),
                   description=d.get('description', None),
                   is_hidden=d.get('isHidden', None),
                   parameter_mappings=d.get('parameterMappings', None),
                   position=_from_dict(d, 'position', WidgetPosition),
                   title=d.get('title', None),
                   updated_at=d.get('updated_at', None))


@dataclass
class WidgetPosition:
    """Coordinates of this widget on a dashboard. This portion of the API changes frequently and is
    unsupported."""

    auto_height: Optional[bool] = None
    col: Optional[int] = None
    row: Optional[int] = None
    size_x: Optional[int] = None
    size_y: Optional[int] = None

    def as_dict(self) -> dict:
        body = {}
        if self.auto_height is not None: body['autoHeight'] = self.auto_height
        if self.col is not None: body['col'] = self.col
        if self.row is not None: body['row'] = self.row
        if self.size_x is not None: body['sizeX'] = self.size_x
        if self.size_y is not None: body['sizeY'] = self.size_y
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, any]) -> 'WidgetPosition':
        return cls(auto_height=d.get('autoHeight', None),
                   col=d.get('col', None),
                   row=d.get('row', None),
                   size_x=d.get('sizeX', None),
                   size_y=d.get('sizeY', None))


class DashboardWidgetsAPI:
    """This is an evolving API that facilitates the addition and removal of widgets from existing dashboards
    within the Databricks Workspace. Data structures may change over time."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self,
               dashboard_id: str,
               options: WidgetOptions,
               *,
               text: Optional[str] = None,
               visualization_id: Optional[str] = None,
               width: Optional[int] = None) -> Widget:
        """Add widget to a dashboard.

        :param dashboard_id: str
          Dashboard ID returned by :method:dashboards/create.
        :param options: :class:`WidgetOptions` (optional)
        :param text: str (optional)
          If this is a textbox widget, the application displays this text. This field is ignored if the widget
          contains a visualization in the `visualization` field.
        :param visualization_id: str (optional)
          Query Vizualization ID returned by :method:queryvisualizations/create.
        :param width: int (optional)
          Width of a widget

        :returns: :class:`Widget`
        """
        body = {}
        if dashboard_id is not None: body['dashboard_id'] = dashboard_id
        if options is not None: body['options'] = options.as_dict()
        if text is not None: body['text'] = text
        if visualization_id is not None: body['visualization_id'] = visualization_id
        if width is not None: body['width'] = width
        res = self._api.do('POST', '/api/2.0/preview/sql/widgets', body=body)
        return Widget.from_dict(res)

    def delete(self, id: str):
        self._api.do('DELETE', f'/api/2.0/preview/sql/widgets/{id}')

    def update(self,
               dashboard_id: str,
               id: str,
               *,
               options: Optional[WidgetOptions] = None,
               text: Optional[str] = None,
               visualization_id: Optional[str] = None,
               width: Optional[int] = None) -> Widget:
        """Update existing widget.

        :param dashboard_id: str
          Dashboard ID returned by :method:dashboards/create.
        :param id: str
        :param options: :class:`WidgetOptions` (optional)
        :param text: str (optional)
          If this is a textbox widget, the application displays this text. This field is ignored if the widget
          contains a visualization in the `visualization` field.
        :param visualization_id: str (optional)
          Query Vizualization ID returned by :method:queryvisualizations/create.
        :param width: int (optional)
          Width of a widget

        :returns: :class:`Widget`
        """
        body = {}
        if dashboard_id is not None: body['dashboard_id'] = dashboard_id
        if options is not None: body['options'] = options.as_dict()
        if text is not None: body['text'] = text
        if visualization_id is not None: body['visualization_id'] = visualization_id
        if width is not None: body['width'] = width
        res = self._api.do('POST', f'/api/2.0/preview/sql/widgets/{id}', body=body)
        return Widget.from_dict(res)

class QueryVisualizationsAPI:
    """This is an evolving API that facilitates the addition and removal of vizualisations from existing queries
    within the Databricks Workspace. Data structures may change over time."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self,
               query_id: str,
               type: str,
               options: dict,
               *,
               created_at: Optional[str] = None,
               description: Optional[str] = None,
               name: Optional[str] = None,
               updated_at: Optional[str] = None) -> Visualization:
        body = {}
        if query_id is not None: body['query_id'] = query_id
        if type is not None: body['type'] = type
        if options is not None: body['options'] = options
        if name is not None: body['name'] = name
        if created_at is not None: body['created_at'] = created_at
        if description is not None: body['description'] = description
        if updated_at is not None: body['updated_at'] = updated_at
        res = self._api.do('POST', '/api/2.0/preview/sql/visualizations', body=body)
        return Visualization.from_dict(res)

    def delete(self, id: str):
        """Remove visualization.

        :param id: str
        """

        headers = {'Accept': 'application/json', }
        self._api.do('DELETE', f'/api/2.0/preview/sql/visualizations/{id}', headers=headers)

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


class QueryVisualizationsExt(QueryVisualizationsAPI):
    def create_table(
        self,
        query_id: str,
        name: str,
        columns: list[VizColumn],
        *,
        items_per_page: int = 25,
        condensed=True,
        with_row_number=False,
        description: str | None = None,
    ):
        return self.create(query_id, 'TABLE', {
            "itemsPerPage": items_per_page,
            "condensed": condensed,
            "withRowNumber": with_row_number,
            "version": 2,
            "columns": [x.as_dict() for x in columns],
        }, name=name, description=description)
