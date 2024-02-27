from dataclasses import dataclass
from typing import Any

from databricks.sdk.service._internal import _from_dict
from databricks.sdk.service.sql import Visualization, Widget, WidgetPosition

# this file is going away soon
# pylint: disable=redefined-builtin


@dataclass
class WidgetOptions:
    created_at: str | None = None
    description: str | None = None
    is_hidden: bool | None = None
    parameter_mappings: Any | None = None
    position: WidgetPosition | None = None
    title: str | None = None
    updated_at: str | None = None

    def as_dict(self) -> dict:
        body: dict[str, Any] = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.description is not None:
            body["description"] = self.description
        if self.is_hidden is not None:
            body["isHidden"] = self.is_hidden
        if self.parameter_mappings:
            body["parameterMappings"] = self.parameter_mappings
        if self.position:
            body["position"] = self.position.as_dict()
        if self.title is not None:
            body["title"] = self.title
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WidgetOptions":
        return cls(
            created_at=data.get("created_at", None),
            description=data.get("description", None),
            is_hidden=data.get("isHidden", None),
            parameter_mappings=data.get("parameterMappings", None),
            position=_from_dict(data, "position", WidgetPosition),
            title=data.get("title", None),
            updated_at=data.get("updated_at", None),
        )


class DashboardWidgetsAPI:
    """This is an evolving API that facilitates the addition and removal of widgets from existing dashboards
    within the Databricks Workspace. Data structures may change over time."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        dashboard_id: str,
        options: WidgetOptions,
        *,
        text: str | None = None,
        visualization_id: str | None = None,
        width: int | None = None,
    ) -> Widget:
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
        body: dict[str, str | dict | int] = {}
        if dashboard_id is not None:
            body["dashboard_id"] = dashboard_id
        if options is not None:
            body["options"] = options.as_dict()
        if text is not None:
            body["text"] = text
        if visualization_id is not None:
            body["visualization_id"] = visualization_id
        if width is not None:
            body["width"] = width
        res = self._api.do("POST", "/api/2.0/preview/sql/widgets", body=body)
        return Widget.from_dict(res)

    def delete(self, widget_id: str):
        self._api.do("DELETE", f"/api/2.0/preview/sql/widgets/{widget_id}")

    def update(
        self,
        dashboard_id: str,
        widget_id: str,
        *,
        options: WidgetOptions | None = None,
        text: str | None = None,
        visualization_id: str | None = None,
        width: int | None = None,
    ) -> Widget:
        """Update existing widget.

        :param dashboard_id: str
          Dashboard ID returned by :method:dashboards/create.
        :param widget_id: str
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
        body: dict[str, str | dict | int] = {}
        if dashboard_id is not None:
            body["dashboard_id"] = dashboard_id
        if options is not None:
            body["options"] = options.as_dict()
        if text is not None:
            body["text"] = text
        if visualization_id is not None:
            body["visualization_id"] = visualization_id
        if width is not None:
            body["width"] = width
        res = self._api.do("POST", f"/api/2.0/preview/sql/widgets/{widget_id}", body=body)
        return Widget.from_dict(res)


class QueryVisualizationsAPI:
    """This is an evolving API that facilitates the addition and removal of vizualisations from existing queries
    within the Databricks Workspace. Data structures may change over time."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        query_id: str,
        type: str,
        options: dict,
        *,
        created_at: str | None = None,
        description: str | None = None,
        name: str | None = None,
        updated_at: str | None = None,
    ) -> Visualization:
        body: dict[str, Any] = {}
        if query_id is not None:
            body["query_id"] = query_id
        if type is not None:
            body["type"] = type
        if options is not None:
            body["options"] = options
        if name is not None:
            body["name"] = name
        if created_at is not None:
            body["created_at"] = created_at
        if description is not None:
            body["description"] = description
        if updated_at is not None:
            body["updated_at"] = updated_at
        res = self._api.do("POST", "/api/2.0/preview/sql/visualizations", body=body)
        return Visualization.from_dict(res)

    def delete(self, widget_id: str):
        """Remove visualization.

        :param widget_id: str
        """

        headers = {
            "Accept": "application/json",
        }
        self._api.do("DELETE", f"/api/2.0/preview/sql/visualizations/{widget_id}", headers=headers)
