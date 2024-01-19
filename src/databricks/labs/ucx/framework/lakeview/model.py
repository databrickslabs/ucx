# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from databricks.sdk.service._internal import (
    _enum,
    _from_dict,
    _repeated_dict,
)

Json = dict[str, Any]


class Alignment(Enum):
    CENTER = "center"
    LEFT = "left"
    RIGHT = "right"


@dataclass
class AngleAxisSpec:
    hide_title: bool | None = None
    title: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.hide_title is not None:
            body["hideTitle"] = self.hide_title
        if self.title is not None:
            body["title"] = self.title
        return body

    @classmethod
    def from_dict(cls, d: Json) -> AngleAxisSpec:
        return cls(hide_title=d.get("hideTitle", None), title=d.get("title", None))


@dataclass
class AngleFieldEncoding:
    field_name: str
    scale: QuantitativeScale
    axis: AngleAxisSpec | None = None
    display_name: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.axis:
            body["axis"] = self.axis.as_dict()
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        if self.scale:
            body["scale"] = self.scale.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> AngleFieldEncoding:
        return cls(
            axis=_from_dict(d, "axis", AngleAxisSpec),
            display_name=d.get("displayName", None),
            field_name=d.get("fieldName", None),
            scale=_from_dict(d, "scale", QuantitativeScale),
        )


@dataclass
class AreaSpec:
    encodings: Any
    """Encoding map for the most common form of charts, which can have either a multi-X or multi-Y
    encoding."""
    format: FormatConfig | None = None
    frame: WidgetFrameSpec | None = None
    mark: MarkSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 3,
            "widgetType": "area",
        }
        if self.encodings:
            body["encodings"] = self.encodings
        if self.format:
            body["format"] = self.format.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        if self.mark:
            body["mark"] = self.mark.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> AreaSpec:
        return cls(
            encodings=d.get("encodings", None),
            format=_from_dict(d, "format", FormatConfig),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
            mark=_from_dict(d, "mark", MarkSpec),
        )


@dataclass
class AxisSpec:
    hide_labels: bool | None = None
    hide_title: bool | None = None
    title: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.hide_labels is not None:
            body["hideLabels"] = self.hide_labels
        if self.hide_title is not None:
            body["hideTitle"] = self.hide_title
        if self.title is not None:
            body["title"] = self.title
        return body

    @classmethod
    def from_dict(cls, d: Json) -> AxisSpec:
        return cls(
            hide_labels=d.get("hideLabels", None), hide_title=d.get("hideTitle", None), title=d.get("title", None)
        )


@dataclass
class BarSpec:
    encodings: Any
    """Encoding map for the most common form of charts, which can have either a multi-X or multi-Y
    encoding."""
    format: FormatConfig | None = None
    frame: WidgetFrameSpec | None = None
    mark: MarkSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 3,
            "widgetType": "bar",
        }
        if self.encodings:
            body["encodings"] = self.encodings
        if self.format:
            body["format"] = self.format.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        if self.mark:
            body["mark"] = self.mark.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> BarSpec:
        return cls(
            encodings=d.get("encodings", None),
            format=_from_dict(d, "format", FormatConfig),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
            mark=_from_dict(d, "mark", MarkSpec),
        )


@dataclass
class CategoricalColorScaleMappingEntry:
    value: DataDomainValue
    color: str

    def as_dict(self) -> Json:
        body: Json = {}
        if self.color is not None:
            body["color"] = self.color
        if self.value is not None:
            body["value"] = self.value.value
        return body

    @classmethod
    def from_dict(cls, d: Json) -> CategoricalColorScaleMappingEntry:
        return cls(color=d.get("color", None), value=_enum(d, "value", DataDomainValue))


@dataclass
class CategoricalScale:
    mappings: list[CategoricalColorScaleMappingEntry] | None = None
    sort: Sort | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "type": "categorical",
        }
        if self.mappings:
            body["mappings"] = [v.as_dict() for v in self.mappings]
        if self.sort:
            body["sort"] = self.sort.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> CategoricalScale:
        return cls(
            mappings=_repeated_dict(d, "mappings", CategoricalColorScaleMappingEntry), sort=_from_dict(d, "sort", Sort)
        )


@dataclass
class ChartEncodingMapWithMultiX:
    x: MultiFieldAxisEncoding
    color: ColorEncodingForMultiSeries | None = None
    label: LabelEncoding | None = None
    y: SingleFieldAxisEncoding | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.color:
            body["color"] = self.color.as_dict()
        if self.label:
            body["label"] = self.label.as_dict()
        if self.x:
            body["x"] = self.x.as_dict()
        if self.y:
            body["y"] = self.y.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> ChartEncodingMapWithMultiX:
        return cls(
            color=_from_dict(d, "color", ColorEncodingForMultiSeries),
            label=_from_dict(d, "label", LabelEncoding),
            x=_from_dict(d, "x", MultiFieldAxisEncoding),
            y=_from_dict(d, "y", SingleFieldAxisEncoding),
        )


@dataclass
class ChartEncodingMapWithMultiY:
    y: MultiFieldAxisEncoding
    color: ColorEncodingForMultiSeries | None = None
    label: LabelEncoding | None = None
    x: SingleFieldAxisEncoding | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.color:
            body["color"] = self.color.as_dict()
        if self.label:
            body["label"] = self.label.as_dict()
        if self.x:
            body["x"] = self.x.as_dict()
        if self.y:
            body["y"] = self.y.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> ChartEncodingMapWithMultiY:
        return cls(
            color=_from_dict(d, "color", ColorEncodingForMultiSeries),
            label=_from_dict(d, "label", LabelEncoding),
            x=_from_dict(d, "x", SingleFieldAxisEncoding),
            y=_from_dict(d, "y", MultiFieldAxisEncoding),
        )


@dataclass
class ChartEncodingMapWithSingleXy:
    x: SingleFieldAxisEncoding
    color: ColorFieldEncoding | None = None
    label: LabelEncoding | None = None
    y: SingleFieldAxisEncoding | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.color:
            body["color"] = self.color.as_dict()
        if self.label:
            body["label"] = self.label.as_dict()
        if self.x:
            body["x"] = self.x.as_dict()
        if self.y:
            body["y"] = self.y.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> ChartEncodingMapWithSingleXy:
        return cls(
            color=_from_dict(d, "color", ColorFieldEncoding),
            label=_from_dict(d, "label", LabelEncoding),
            x=_from_dict(d, "x", SingleFieldAxisEncoding),
            y=_from_dict(d, "y", SingleFieldAxisEncoding),
        )


@dataclass
class ColorEncodingForMultiSeries:
    legend: LegendSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.legend:
            body["legend"] = self.legend.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> ColorEncodingForMultiSeries:
        return cls(legend=_from_dict(d, "legend", LegendSpec))


@dataclass
class ColorFieldEncoding:
    field_name: str
    scale: CategoricalScale | QuantitativeScale | TemporalScale
    display_name: str | None = None
    legend: LegendSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        if self.legend:
            body["legend"] = self.legend.as_dict()
        if self.scale:
            body["scale"] = self.scale.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> ColorFieldEncoding:
        return cls(
            display_name=d.get("displayName", None),
            field_name=d.get("fieldName", None),
            legend=_from_dict(d, "legend", LegendSpec),
            scale=_from_dict(d, "scale", Scale),
        )


class ColumnType(Enum):
    BOOLEAN = "boolean"
    COMPLEX = "complex"
    DATE = "date"
    DATETIME = "datetime"
    DECIMAL = "decimal"
    FLOAT = "float"
    INTEGER = "integer"
    STRING = "string"


@dataclass
class ControlEncodingMap:
    fields: Any | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.fields:
            body["fields"] = self.fields
        return body

    @classmethod
    def from_dict(cls, d: Json) -> ControlEncodingMap:
        return cls(fields=d.get("fields", None))


@dataclass
class CounterEncodingMap:
    target: CounterFieldEncoding | None = None
    value: CounterFieldEncoding | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.target:
            body["target"] = self.target.as_dict()
        if self.value:
            body["value"] = self.value.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> CounterEncodingMap:
        return cls(
            target=_from_dict(d, "target", CounterFieldEncoding), value=_from_dict(d, "value", CounterFieldEncoding)
        )


@dataclass
class CounterFieldEncoding:
    field_name: str
    display_name: str | None = None
    row_number: int | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        if self.row_number is not None:
            body["rowNumber"] = self.row_number
        return body

    @classmethod
    def from_dict(cls, d: Json) -> CounterFieldEncoding:
        return cls(
            display_name=d.get("displayName", None),
            field_name=d.get("fieldName", None),
            row_number=d.get("rowNumber", None),
        )


@dataclass
class CounterSpec:
    encodings: CounterEncodingMap
    format: FormatConfig | None = None
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "counter",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.format:
            body["format"] = self.format.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> CounterSpec:
        return cls(
            encodings=_from_dict(d, "encodings", CounterEncodingMap),
            format=_from_dict(d, "format", FormatConfig),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
        )


@dataclass
class Dashboard:
    datasets: list[Dataset]
    pages: list[Page]

    def as_dict(self) -> Json:
        body: Json = {}
        if self.datasets:
            body["datasets"] = [v.as_dict() for v in self.datasets]
        if self.pages:
            body["pages"] = [v.as_dict() for v in self.pages]
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Dashboard:
        return cls(datasets=_repeated_dict(d, "datasets", Dataset), pages=_repeated_dict(d, "pages", Page))


class DataDomainValue(Enum):
    BOOLEAN = "boolean"
    NULL = "null"
    NUMBER = "number"
    STRING = "string"


@dataclass
class Dataset:
    name: str
    query: str
    display_name: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.name is not None:
            body["name"] = self.name
        if self.query is not None:
            body["query"] = self.query
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Dataset:
        return cls(display_name=d.get("displayName", None), name=d.get("name", None), query=d.get("query", None))


@dataclass
class DatePickerSpec:
    encodings: ControlEncodingMap
    exclude: bool | None = None
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "filter-date-picker",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.exclude is not None:
            body["exclude"] = self.exclude
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> DatePickerSpec:
        return cls(
            encodings=_from_dict(d, "encodings", ControlEncodingMap),
            exclude=d.get("exclude", None),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
        )


@dataclass
class DateRangePickerSpec:
    encodings: ControlEncodingMap
    exclude: bool | None = None
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "filter-date-range-picker",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.exclude is not None:
            body["exclude"] = self.exclude
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> DateRangePickerSpec:
        return cls(
            encodings=_from_dict(d, "encodings", ControlEncodingMap),
            exclude=d.get("exclude", None),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
        )


@dataclass
class DetailsV1ColumnEncoding:
    field_name: str
    display_name: str | None = None
    title: str | None = None
    type: ColumnType | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        if self.title is not None:
            body["title"] = self.title
        if self.type is not None:
            body["type"] = self.type.value
        return body

    @classmethod
    def from_dict(cls, d: Json) -> DetailsV1ColumnEncoding:
        return cls(
            display_name=d.get("displayName", None),
            field_name=d.get("fieldName", None),
            title=d.get("title", None),
            type=_enum(d, "type", ColumnType),
        )


@dataclass
class DetailsV1EncodingMap:
    columns: list[DetailsV1ColumnEncoding] | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        return body

    @classmethod
    def from_dict(cls, d: Json) -> DetailsV1EncodingMap:
        return cls(columns=_repeated_dict(d, "columns", DetailsV1ColumnEncoding))


@dataclass
class DetailsV1Spec:
    encodings: DetailsV1EncodingMap
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 1,
            "widgetType": "details",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> DetailsV1Spec:
        return cls(
            encodings=_from_dict(d, "encodings", DetailsV1EncodingMap), frame=_from_dict(d, "frame", WidgetFrameSpec)
        )


class Direction(Enum):
    ASC = "ASC"
    DESC = "DESC"
    DIRECTION_UNSPECIFIED = "DIRECTION_UNSPECIFIED"


class DisplayType(Enum):
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    IMAGE = "image"
    JSON = "json"
    LINK = "link"
    NUMBER = "number"
    STRING = "string"


@dataclass
class DropdownSpec:
    encodings: ControlEncodingMap
    exclude: bool | None = None
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "filter-single-select",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.exclude is not None:
            body["exclude"] = self.exclude
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> DropdownSpec:
        return cls(
            encodings=_from_dict(d, "encodings", ControlEncodingMap),
            exclude=d.get("exclude", None),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
        )


@dataclass
class Field:
    name: str
    expression: str

    def as_dict(self) -> Json:
        body: Json = {}
        if self.expression is not None:
            body["expression"] = self.expression
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Field:
        return cls(expression=d.get("expression", None), name=d.get("name", None))


@dataclass
class FieldEncodingWithDataFor:
    field_name: str
    query_name: str
    display_name: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        if self.query_name is not None:
            body["queryName"] = self.query_name
        return body

    @classmethod
    def from_dict(cls, d: Json) -> FieldEncodingWithDataFor:
        return cls(
            display_name=d.get("displayName", None),
            field_name=d.get("fieldName", None),
            query_name=d.get("queryName", None),
        )


@dataclass
class Format:
    foreground_color: str

    def as_dict(self) -> Json:
        body: Json = {}
        if self.foreground_color is not None:
            body["foregroundColor"] = self.foreground_color
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Format:
        return cls(foreground_color=d.get("foregroundColor", None))


@dataclass
class FormatConfig:
    number_format: NumeralNumberFormat | None = None
    percent_format: NumeralNumberFormat | None = None
    time_format: MomentTimeFormat | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.number_format:
            body["numberFormat"] = self.number_format.as_dict()
        if self.percent_format:
            body["percentFormat"] = self.percent_format.as_dict()
        if self.time_format:
            body["timeFormat"] = self.time_format.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> FormatConfig:
        return cls(
            number_format=_from_dict(d, "numberFormat", NumeralNumberFormat),
            percent_format=_from_dict(d, "percentFormat", NumeralNumberFormat),
            time_format=_from_dict(d, "timeFormat", MomentTimeFormat),
        )


@dataclass
class LabelEncoding:
    show: bool

    def as_dict(self) -> Json:
        body: Json = {}
        if self.show is not None:
            body["show"] = self.show
        return body

    @classmethod
    def from_dict(cls, d: Json) -> LabelEncoding:
        return cls(show=d.get("show", None))


@dataclass
class Layout:
    widget: Widget
    position: Position

    def as_dict(self) -> Json:
        body: Json = {}
        if self.position:
            body["position"] = self.position.as_dict()
        if self.widget:
            body["widget"] = self.widget.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Layout:
        return cls(position=_from_dict(d, "position", Position), widget=_from_dict(d, "widget", Widget))


@dataclass
class LegendSpec:
    hide_title: bool | None = None
    position: LegendSpecPosition | None = None
    title: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.hide_title is not None:
            body["hideTitle"] = self.hide_title
        if self.position is not None:
            body["position"] = self.position.value
        if self.title is not None:
            body["title"] = self.title
        return body

    @classmethod
    def from_dict(cls, d: Json) -> LegendSpec:
        return cls(
            hide_title=d.get("hideTitle", None),
            position=_enum(d, "position", LegendSpecPosition),
            title=d.get("title", None),
        )


class LegendSpecPosition(Enum):
    BOTTOM = "bottom"
    RIGHT = "right"


@dataclass
class LineSpec:
    encodings: Any
    """Encoding map for the most common form of charts, which can have either a multi-X or multi-Y
    encoding."""
    format: FormatConfig | None = None
    frame: WidgetFrameSpec | None = None
    mark: MarkSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 3,
            "widgetType": "line",
        }
        if self.encodings:
            body["encodings"] = self.encodings
        if self.format:
            body["format"] = self.format.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        if self.mark:
            body["mark"] = self.mark.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> LineSpec:
        return cls(
            encodings=d.get("encodings", None),
            format=_from_dict(d, "format", FormatConfig),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
            mark=_from_dict(d, "mark", MarkSpec),
        )


class MarkLayout(Enum):
    GROUP = "group"
    LAYER = "layer"
    STACK = "stack"


@dataclass
class MarkSpec:
    colors: list[str] | None = None
    layout: MarkLayout | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.colors:
            body["colors"] = [v for v in self.colors]
        if self.layout is not None:
            body["layout"] = self.layout.value
        return body

    @classmethod
    def from_dict(cls, d: Json) -> MarkSpec:
        return cls(colors=d.get("colors", None), layout=_enum(d, "layout", MarkLayout))


@dataclass
class MomentTimeFormat:
    format: str | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "formatType": "moment",
        }
        if self.format is not None:
            body["format"] = self.format
        return body

    @classmethod
    def from_dict(cls, d: Json) -> MomentTimeFormat:
        return cls(format=d.get("format", None))


@dataclass
class MultiFieldAxisEncoding:
    fields: list[RenderFieldEncoding]
    scale: QuantitativeScale
    axis: AxisSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.axis:
            body["axis"] = self.axis.as_dict()
        if self.fields:
            body["fields"] = [v.as_dict() for v in self.fields]
        if self.scale:
            body["scale"] = self.scale.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> MultiFieldAxisEncoding:
        return cls(
            axis=_from_dict(d, "axis", AxisSpec),
            fields=_repeated_dict(d, "fields", RenderFieldEncoding),
            scale=_from_dict(d, "scale", QuantitativeScale),
        )


@dataclass
class MultiSelectSpec:
    encodings: ControlEncodingMap
    exclude: bool | None = None
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "filter-multi-select",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.exclude is not None:
            body["exclude"] = self.exclude
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> MultiSelectSpec:
        return cls(
            encodings=_from_dict(d, "encodings", ControlEncodingMap),
            exclude=d.get("exclude", None),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
        )


@dataclass
class NamedQuery:
    name: str
    query: Order

    def as_dict(self) -> Json:
        body: Json = {}
        if self.name is not None:
            body["name"] = self.name
        if self.query:
            body["query"] = self.query.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> NamedQuery:
        return cls(name=d.get("name", None), query=_from_dict(d, "query", Order))


@dataclass
class NumeralNumberFormat:
    format: str | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "formatType": "numeral",
        }
        if self.format is not None:
            body["format"] = self.format
        return body

    @classmethod
    def from_dict(cls, d: Json) -> NumeralNumberFormat:
        return cls(format=d.get("format", None))


@dataclass
class Order:
    direction: Direction | None = None
    expression: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.direction is not None:
            body["direction"] = self.direction.value
        if self.expression is not None:
            body["expression"] = self.expression
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Order:
        return cls(direction=_enum(d, "direction", Direction), expression=d.get("expression", None))


@dataclass
class Page:
    name: str
    layout: list[Layout]
    display_name: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.layout:
            body["layout"] = [v.as_dict() for v in self.layout]
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Page:
        return cls(
            display_name=d.get("displayName", None),
            layout=_repeated_dict(d, "layout", Layout),
            name=d.get("name", None),
        )


class PaginationSize(Enum):
    DEFAULT = "default"
    SMALL = "small"


@dataclass
class ParameterEncoding:
    dataset_name: str
    parameter_keyword: str

    def as_dict(self) -> Json:
        body: Json = {}
        if self.dataset_name is not None:
            body["datasetName"] = self.dataset_name
        if self.parameter_keyword is not None:
            body["parameterKeyword"] = self.parameter_keyword
        return body

    @classmethod
    def from_dict(cls, d: Json) -> ParameterEncoding:
        return cls(dataset_name=d.get("datasetName", None), parameter_keyword=d.get("parameterKeyword", None))


@dataclass
class PieEncodingMap:
    angle: AngleFieldEncoding | None = None
    color: ColorFieldEncoding | None = None
    label: LabelEncoding | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.angle:
            body["angle"] = self.angle.as_dict()
        if self.color:
            body["color"] = self.color.as_dict()
        if self.label:
            body["label"] = self.label.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> PieEncodingMap:
        return cls(
            angle=_from_dict(d, "angle", AngleFieldEncoding),
            color=_from_dict(d, "color", ColorFieldEncoding),
            label=_from_dict(d, "label", LabelEncoding),
        )


@dataclass
class PieSpec:
    encodings: PieEncodingMap
    format: FormatConfig | None = None
    frame: WidgetFrameSpec | None = None
    mark: MarkSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 3,
            "widgetType": "pie",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.format:
            body["format"] = self.format.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        if self.mark:
            body["mark"] = self.mark.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> PieSpec:
        return cls(
            encodings=_from_dict(d, "encodings", PieEncodingMap),
            format=_from_dict(d, "format", FormatConfig),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
            mark=_from_dict(d, "mark", MarkSpec),
        )


@dataclass
class PivotCellEncoding:
    field_name: str
    display_name: str | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "cellType": "text",
        }
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        return body

    @classmethod
    def from_dict(cls, d: Json) -> PivotCellEncoding:
        return cls(display_name=d.get("displayName", None), field_name=d.get("fieldName", None))


@dataclass
class PivotEncodingMap:
    cell: PivotCellEncoding | None = None
    columns: list[RenderFieldEncoding] | None = None
    rows: list[RenderFieldEncoding] | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.cell:
            body["cell"] = self.cell.as_dict()
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        if self.rows:
            body["rows"] = [v.as_dict() for v in self.rows]
        return body

    @classmethod
    def from_dict(cls, d: Json) -> PivotEncodingMap:
        return cls(
            cell=_from_dict(d, "cell", PivotCellEncoding),
            columns=_repeated_dict(d, "columns", RenderFieldEncoding),
            rows=_repeated_dict(d, "rows", RenderFieldEncoding),
        )


@dataclass
class PivotSpec:
    encodings: PivotEncodingMap
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 3,
            "widgetType": "pivot",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> PivotSpec:
        return cls(
            encodings=_from_dict(d, "encodings", PivotEncodingMap), frame=_from_dict(d, "frame", WidgetFrameSpec)
        )


@dataclass
class Position:
    x: int
    y: int
    width: int
    height: int

    def as_dict(self) -> Json:
        body: Json = {}
        if self.height is not None:
            body["height"] = self.height
        if self.width is not None:
            body["width"] = self.width
        if self.x is not None:
            body["x"] = self.x
        if self.y is not None:
            body["y"] = self.y
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Position:
        return cls(height=d.get("height", None), width=d.get("width", None), x=d.get("x", None), y=d.get("y", None))


@dataclass
class QuantitativeDomain:
    max: int | None = None
    min: int | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.max is not None:
            body["max"] = self.max
        if self.min is not None:
            body["min"] = self.min
        return body

    @classmethod
    def from_dict(cls, d: Json) -> QuantitativeDomain:
        return cls(max=d.get("max", None), min=d.get("min", None))


@dataclass
class QuantitativeScale:
    domain: QuantitativeDomain | None = None
    """If not specified, domain.min/max is the minimum/maximum value of the data. If specified, any
    value < domain.min or > domain.max will be clipped for x/y axes."""
    reverse: bool | None = None
    """If not specified or false, domainMin is mapped to the range start value, and domainMax is mapped
    to the range end value. If true, domainMin is mapped to the range end value, and domainMax is
    mapped to the range start value. Take x-axis for example, if reverse = undefined, domainMin is
    mapped to x=0 and domainMax is mapped to x=width. If reverse = true, domainMin is mapped to
    x=width and domainMax is mapped to x=0."""

    def as_dict(self) -> Json:
        body: Json = {
            "type": "quantitative",
        }
        if self.domain:
            body["domain"] = self.domain.as_dict()
        if self.reverse is not None:
            body["reverse"] = self.reverse
        return body

    @classmethod
    def from_dict(cls, d: Json) -> QuantitativeScale:
        return cls(domain=_from_dict(d, "domain", QuantitativeDomain), reverse=d.get("reverse", None))


@dataclass
class Query:
    dataset_name: str
    fields: list[Field]
    disaggregated: bool | None = None
    orders: list[Order] | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.dataset_name is not None:
            body["datasetName"] = self.dataset_name
        if self.disaggregated is not None:
            body["disaggregated"] = self.disaggregated
        if self.fields:
            body["fields"] = [v.as_dict() for v in self.fields]
        if self.orders:
            body["orders"] = [v.as_dict() for v in self.orders]
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Query:
        return cls(
            dataset_name=d.get("datasetName", None),
            disaggregated=d.get("disaggregated", None),
            fields=_repeated_dict(d, "fields", Field),
            orders=_repeated_dict(d, "orders", Order),
        )


@dataclass
class RenderFieldEncoding:
    """Common type that a single-field encoding should (conceptually) extend from"""

    field_name: str
    display_name: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        return body

    @classmethod
    def from_dict(cls, d: Json) -> RenderFieldEncoding:
        return cls(display_name=d.get("displayName", None), field_name=d.get("fieldName", None))


class Scale:
    @classmethod
    def from_dict(cls, d: Json) -> CategoricalScale | QuantitativeScale | TemporalScale:
        if d["type"] == "categorical":
            return CategoricalScale.from_dict(d)
        elif d["type"] == "quantitative":
            return QuantitativeScale.from_dict(d)
        elif d["type"] == "temporal":
            return TemporalScale.from_dict(d)
        else:
            raise KeyError(...)


@dataclass
class ScatterSpec:
    encodings: Any
    """Encoding map for the most common form of charts, which can have either a multi-X or multi-Y
    encoding."""
    format: FormatConfig | None = None
    frame: WidgetFrameSpec | None = None
    mark: MarkSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 3,
            "widgetType": "scatter",
        }
        if self.encodings:
            body["encodings"] = self.encodings
        if self.format:
            body["format"] = self.format.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        if self.mark:
            body["mark"] = self.mark.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> ScatterSpec:
        return cls(
            encodings=d.get("encodings", None),
            format=_from_dict(d, "format", FormatConfig),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
            mark=_from_dict(d, "mark", MarkSpec),
        )


@dataclass
class SingleFieldAxisEncoding:
    field_name: str
    scale: CategoricalScale | QuantitativeScale | TemporalScale
    axis: AxisSpec | None = None
    display_name: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.axis:
            body["axis"] = self.axis.as_dict()
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        if self.scale:
            body["scale"] = self.scale.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> SingleFieldAxisEncoding:
        return cls(
            axis=_from_dict(d, "axis", AxisSpec),
            display_name=d.get("displayName", None),
            field_name=d.get("fieldName", None),
            scale=_from_dict(d, "scale", Scale),
        )


@dataclass
class Sort:
    by: SortBy

    def as_dict(self) -> Json:
        body: Json = {}
        if self.by is not None:
            body["by"] = self.by.value
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Sort:
        return cls(by=_enum(d, "by", SortBy))


class SortBy(Enum):
    NATURAL_ORDER = "natural-order"
    NATURAL_ORDER_REVERSED = "natural-order-reversed"
    X = "x"
    X_REVERSED = "x-reversed"
    Y = "y"
    Y_REVERSED = "y-reversed"


@dataclass
class SymbolMapEncodingMap:
    latitude: RenderFieldEncoding | None = None
    longitude: RenderFieldEncoding | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.latitude:
            body["latitude"] = self.latitude.as_dict()
        if self.longitude:
            body["longitude"] = self.longitude.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> SymbolMapEncodingMap:
        return cls(
            latitude=_from_dict(d, "latitude", RenderFieldEncoding),
            longitude=_from_dict(d, "longitude", RenderFieldEncoding),
        )


@dataclass
class SymbolMapSpec:
    encodings: SymbolMapEncodingMap
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "symbol-map",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> SymbolMapSpec:
        return cls(
            encodings=_from_dict(d, "encodings", SymbolMapEncodingMap), frame=_from_dict(d, "frame", WidgetFrameSpec)
        )


@dataclass
class TableEncodingMap:
    columns: list[RenderFieldEncoding] | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        return body

    @classmethod
    def from_dict(cls, d: Json) -> TableEncodingMap:
        return cls(columns=_repeated_dict(d, "columns", RenderFieldEncoding))


@dataclass
class TableV1ColumnEncoding:
    """FieldEncoding for TableV1. Note that `visible?` is true by default, which is `false` in the
    legacy v1 table."""

    boolean_values: list[str]
    display_as: DisplayType
    field_name: str
    title: str
    type: ColumnType
    align_content: Alignment | None = None
    allow_html: bool | None = None
    allow_search: bool | None = None
    date_time_format: str | None = None
    decimal_format: str | None = None
    default_column_width: int | None = None
    description: str | None = None
    display_name: str | None = None
    highlight_links: bool | None = None
    image_height: str | None = None
    image_title_template: str | None = None
    image_url_template: str | None = None
    image_width: str | None = None
    link_open_in_new_tab: bool | None = None
    link_text_template: str | None = None
    link_title_template: str | None = None
    link_url_template: str | None = None
    number_format: str | None = None
    order: int | None = None
    preserve_whitespace: bool | None = None
    use_monospace_font: bool | None = None
    visible: bool | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.align_content is not None:
            body["alignContent"] = self.align_content.value
        if self.allow_html is not None:
            body["allowHTML"] = self.allow_html
        if self.allow_search is not None:
            body["allowSearch"] = self.allow_search
        if self.boolean_values:
            body["booleanValues"] = [v for v in self.boolean_values]
        if self.date_time_format is not None:
            body["dateTimeFormat"] = self.date_time_format
        if self.decimal_format is not None:
            body["decimalFormat"] = self.decimal_format
        if self.default_column_width is not None:
            body["defaultColumnWidth"] = self.default_column_width
        if self.description is not None:
            body["description"] = self.description
        if self.display_as is not None:
            body["displayAs"] = self.display_as.value
        if self.display_name is not None:
            body["displayName"] = self.display_name
        if self.field_name is not None:
            body["fieldName"] = self.field_name
        if self.highlight_links is not None:
            body["highlightLinks"] = self.highlight_links
        if self.image_height is not None:
            body["imageHeight"] = self.image_height
        if self.image_title_template is not None:
            body["imageTitleTemplate"] = self.image_title_template
        if self.image_url_template is not None:
            body["imageUrlTemplate"] = self.image_url_template
        if self.image_width is not None:
            body["imageWidth"] = self.image_width
        if self.link_open_in_new_tab is not None:
            body["linkOpenInNewTab"] = self.link_open_in_new_tab
        if self.link_text_template is not None:
            body["linkTextTemplate"] = self.link_text_template
        if self.link_title_template is not None:
            body["linkTitleTemplate"] = self.link_title_template
        if self.link_url_template is not None:
            body["linkUrlTemplate"] = self.link_url_template
        if self.number_format is not None:
            body["numberFormat"] = self.number_format
        if self.order is not None:
            body["order"] = self.order
        if self.preserve_whitespace is not None:
            body["preserveWhitespace"] = self.preserve_whitespace
        if self.title is not None:
            body["title"] = self.title
        if self.type is not None:
            body["type"] = self.type.value
        if self.use_monospace_font is not None:
            body["useMonospaceFont"] = self.use_monospace_font
        if self.visible is not None:
            body["visible"] = self.visible
        return body

    @classmethod
    def from_dict(cls, d: Json) -> TableV1ColumnEncoding:
        return cls(
            align_content=_enum(d, "alignContent", Alignment),
            allow_html=d.get("allowHTML", None),
            allow_search=d.get("allowSearch", None),
            boolean_values=d.get("booleanValues", None),
            date_time_format=d.get("dateTimeFormat", None),
            decimal_format=d.get("decimalFormat", None),
            default_column_width=d.get("defaultColumnWidth", None),
            description=d.get("description", None),
            display_as=_enum(d, "displayAs", DisplayType),
            display_name=d.get("displayName", None),
            field_name=d.get("fieldName", None),
            highlight_links=d.get("highlightLinks", None),
            image_height=d.get("imageHeight", None),
            image_title_template=d.get("imageTitleTemplate", None),
            image_url_template=d.get("imageUrlTemplate", None),
            image_width=d.get("imageWidth", None),
            link_open_in_new_tab=d.get("linkOpenInNewTab", None),
            link_text_template=d.get("linkTextTemplate", None),
            link_title_template=d.get("linkTitleTemplate", None),
            link_url_template=d.get("linkUrlTemplate", None),
            number_format=d.get("numberFormat", None),
            order=d.get("order", None),
            preserve_whitespace=d.get("preserveWhitespace", None),
            title=d.get("title", None),
            type=_enum(d, "type", ColumnType),
            use_monospace_font=d.get("useMonospaceFont", None),
            visible=d.get("visible", None),
        )


@dataclass
class TableV1EncodingMap:
    columns: list[TableV1ColumnEncoding] | None = None
    """Used columns. These columns are visible or used for search."""

    def as_dict(self) -> Json:
        body: Json = {}
        if self.columns:
            body["columns"] = [v.as_dict() for v in self.columns]
        return body

    @classmethod
    def from_dict(cls, d: Json) -> TableV1EncodingMap:
        return cls(columns=_repeated_dict(d, "columns", TableV1ColumnEncoding))


@dataclass
class TableV1Spec:
    allow_html_by_default: bool
    """V1 uses `version` to determine if the v1 editor should set `allowHTML` by default."""
    condensed: bool
    encodings: TableV1EncodingMap
    invisible_columns: list[TableV1SpecInvisibleColumnsItem]
    """Unused columns. These columns are invisible and not referred, and thus should not be include in
    the queries (be outside of `encodings`). Even when the base query changes not to include these
    columns, the table still can work without throwing errors."""
    items_per_page: int
    frame: WidgetFrameSpec | None = None
    pagination_size: PaginationSize | None = None
    with_row_number: bool | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 1,
            "widgetType": "table",
        }
        if self.allow_html_by_default is not None:
            body["allowHTMLByDefault"] = self.allow_html_by_default
        if self.condensed is not None:
            body["condensed"] = self.condensed
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        if self.invisible_columns:
            body["invisibleColumns"] = [v.as_dict() for v in self.invisible_columns]
        if self.items_per_page is not None:
            body["itemsPerPage"] = self.items_per_page
        if self.pagination_size is not None:
            body["paginationSize"] = self.pagination_size.value
        if self.with_row_number is not None:
            body["withRowNumber"] = self.with_row_number
        return body

    @classmethod
    def from_dict(cls, d: Json) -> TableV1Spec:
        return cls(
            allow_html_by_default=d.get("allowHTMLByDefault", None),
            condensed=d.get("condensed", None),
            encodings=_from_dict(d, "encodings", TableV1EncodingMap),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
            invisible_columns=_repeated_dict(d, "invisibleColumns", TableV1SpecInvisibleColumnsItem),
            items_per_page=d.get("itemsPerPage", None),
            pagination_size=_enum(d, "paginationSize", PaginationSize),
            with_row_number=d.get("withRowNumber", None),
        )


@dataclass
class TableV1SpecInvisibleColumnsItem:
    name: str
    display_as: DisplayType
    type: ColumnType
    title: str
    boolean_values: list[str]
    align_content: Alignment | None = None
    allow_html: bool | None = None
    allow_search: bool | None = None
    date_time_format: str | None = None
    decimal_format: str | None = None
    default_column_width: int | None = None
    description: str | None = None
    highlight_links: bool | None = None
    image_height: str | None = None
    image_title_template: str | None = None
    image_url_template: str | None = None
    image_width: str | None = None
    link_open_in_new_tab: bool | None = None
    link_text_template: str | None = None
    link_title_template: str | None = None
    link_url_template: str | None = None
    number_format: str | None = None
    order: int | None = None
    preserve_whitespace: bool | None = None
    use_monospace_font: bool | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.align_content is not None:
            body["alignContent"] = self.align_content.value
        if self.allow_html is not None:
            body["allowHTML"] = self.allow_html
        if self.allow_search is not None:
            body["allowSearch"] = self.allow_search
        if self.boolean_values:
            body["booleanValues"] = [v for v in self.boolean_values]
        if self.date_time_format is not None:
            body["dateTimeFormat"] = self.date_time_format
        if self.decimal_format is not None:
            body["decimalFormat"] = self.decimal_format
        if self.default_column_width is not None:
            body["defaultColumnWidth"] = self.default_column_width
        if self.description is not None:
            body["description"] = self.description
        if self.display_as is not None:
            body["displayAs"] = self.display_as.value
        if self.highlight_links is not None:
            body["highlightLinks"] = self.highlight_links
        if self.image_height is not None:
            body["imageHeight"] = self.image_height
        if self.image_title_template is not None:
            body["imageTitleTemplate"] = self.image_title_template
        if self.image_url_template is not None:
            body["imageUrlTemplate"] = self.image_url_template
        if self.image_width is not None:
            body["imageWidth"] = self.image_width
        if self.link_open_in_new_tab is not None:
            body["linkOpenInNewTab"] = self.link_open_in_new_tab
        if self.link_text_template is not None:
            body["linkTextTemplate"] = self.link_text_template
        if self.link_title_template is not None:
            body["linkTitleTemplate"] = self.link_title_template
        if self.link_url_template is not None:
            body["linkUrlTemplate"] = self.link_url_template
        if self.name is not None:
            body["name"] = self.name
        if self.number_format is not None:
            body["numberFormat"] = self.number_format
        if self.order is not None:
            body["order"] = self.order
        if self.preserve_whitespace is not None:
            body["preserveWhitespace"] = self.preserve_whitespace
        if self.title is not None:
            body["title"] = self.title
        if self.type is not None:
            body["type"] = self.type.value
        if self.use_monospace_font is not None:
            body["useMonospaceFont"] = self.use_monospace_font
        return body

    @classmethod
    def from_dict(cls, d: Json) -> TableV1SpecInvisibleColumnsItem:
        return cls(
            align_content=_enum(d, "alignContent", Alignment),
            allow_html=d.get("allowHTML", None),
            allow_search=d.get("allowSearch", None),
            boolean_values=d.get("booleanValues", None),
            date_time_format=d.get("dateTimeFormat", None),
            decimal_format=d.get("decimalFormat", None),
            default_column_width=d.get("defaultColumnWidth", None),
            description=d.get("description", None),
            display_as=_enum(d, "displayAs", DisplayType),
            highlight_links=d.get("highlightLinks", None),
            image_height=d.get("imageHeight", None),
            image_title_template=d.get("imageTitleTemplate", None),
            image_url_template=d.get("imageUrlTemplate", None),
            image_width=d.get("imageWidth", None),
            link_open_in_new_tab=d.get("linkOpenInNewTab", None),
            link_text_template=d.get("linkTextTemplate", None),
            link_title_template=d.get("linkTitleTemplate", None),
            link_url_template=d.get("linkUrlTemplate", None),
            name=d.get("name", None),
            number_format=d.get("numberFormat", None),
            order=d.get("order", None),
            preserve_whitespace=d.get("preserveWhitespace", None),
            title=d.get("title", None),
            type=_enum(d, "type", ColumnType),
            use_monospace_font=d.get("useMonospaceFont", None),
        )


@dataclass
class TableV2Spec:
    encodings: TableEncodingMap
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "table",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> TableV2Spec:
        return cls(
            encodings=_from_dict(d, "encodings", TableEncodingMap), frame=_from_dict(d, "frame", WidgetFrameSpec)
        )


@dataclass
class TemporalScale:
    def as_dict(self) -> Json:
        body: Json = {
            "type": "temporal",
        }
        return body

    @classmethod
    def from_dict(cls, d: Json) -> TemporalScale:
        return cls()


@dataclass
class TextEntrySpec:
    encodings: ControlEncodingMap
    exclude: bool | None = None
    frame: WidgetFrameSpec | None = None
    is_case_sensitive: bool | None = None
    match_mode: TextEntrySpecMatchMode | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "filter-text-entry",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.exclude is not None:
            body["exclude"] = self.exclude
        if self.frame:
            body["frame"] = self.frame.as_dict()
        if self.is_case_sensitive is not None:
            body["isCaseSensitive"] = self.is_case_sensitive
        if self.match_mode is not None:
            body["matchMode"] = self.match_mode.value
        return body

    @classmethod
    def from_dict(cls, d: Json) -> TextEntrySpec:
        return cls(
            encodings=_from_dict(d, "encodings", ControlEncodingMap),
            exclude=d.get("exclude", None),
            frame=_from_dict(d, "frame", WidgetFrameSpec),
            is_case_sensitive=d.get("isCaseSensitive", None),
            match_mode=_enum(d, "matchMode", TextEntrySpecMatchMode),
        )


class TextEntrySpecMatchMode(Enum):
    CONTAINS = "contains"
    EXACT_MATCH = "exact-match"
    STARTS_WITH = "starts-with"


@dataclass
class Widget:
    name: str
    queries: list[NamedQuery] | None = None
    spec: DetailsV1Spec | TableV1Spec | CounterSpec | DatePickerSpec | DateRangePickerSpec | MultiSelectSpec | DropdownSpec | TextEntrySpec | SymbolMapSpec | TableV2Spec | WordCloudSpec | AreaSpec | BarSpec | LineSpec | PieSpec | PivotSpec | ScatterSpec | None = (
        None
    )
    textbox_spec: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.name is not None:
            body["name"] = self.name
        if self.queries:
            body["queries"] = [v.as_dict() for v in self.queries]
        if self.spec:
            body["spec"] = self.spec.as_dict()
        if self.textbox_spec is not None:
            body["textbox_spec"] = self.textbox_spec
        return body

    @classmethod
    def from_dict(cls, d: Json) -> Widget:
        return cls(
            name=d.get("name", None),
            queries=_repeated_dict(d, "queries", NamedQuery),
            spec=_from_dict(d, "spec", WidgetSpec),
            textbox_spec=d.get("textbox_spec", None),
        )


@dataclass
class WidgetFrameSpec:
    description: str | None = None
    show_description: bool | None = None
    show_title: bool | None = None
    title: str | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.description is not None:
            body["description"] = self.description
        if self.show_description is not None:
            body["showDescription"] = self.show_description
        if self.show_title is not None:
            body["showTitle"] = self.show_title
        if self.title is not None:
            body["title"] = self.title
        return body

    @classmethod
    def from_dict(cls, d: Json) -> WidgetFrameSpec:
        return cls(
            description=d.get("description", None),
            show_description=d.get("showDescription", None),
            show_title=d.get("showTitle", None),
            title=d.get("title", None),
        )


class WidgetSpec:
    @classmethod
    def from_dict(
        cls, d: Json
    ) -> (
        DetailsV1Spec
        | TableV1Spec
        | CounterSpec
        | DatePickerSpec
        | DateRangePickerSpec
        | MultiSelectSpec
        | DropdownSpec
        | TextEntrySpec
        | SymbolMapSpec
        | TableV2Spec
        | WordCloudSpec
        | AreaSpec
        | BarSpec
        | LineSpec
        | PieSpec
        | PivotSpec
        | ScatterSpec
    ):
        if d["version"] == 1 and d["widgetType"] == "details":
            return DetailsV1Spec.from_dict(d)
        elif d["version"] == 1 and d["widgetType"] == "table":
            return TableV1Spec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "counter":
            return CounterSpec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "filter-date-picker":
            return DatePickerSpec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "filter-date-range-picker":
            return DateRangePickerSpec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "filter-multi-select":
            return MultiSelectSpec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "filter-single-select":
            return DropdownSpec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "filter-text-entry":
            return TextEntrySpec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "symbol-map":
            return SymbolMapSpec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "table":
            return TableV2Spec.from_dict(d)
        elif d["version"] == 2 and d["widgetType"] == "word-cloud":
            return WordCloudSpec.from_dict(d)
        elif d["version"] == 3 and d["widgetType"] == "area":
            return AreaSpec.from_dict(d)
        elif d["version"] == 3 and d["widgetType"] == "bar":
            return BarSpec.from_dict(d)
        elif d["version"] == 3 and d["widgetType"] == "line":
            return LineSpec.from_dict(d)
        elif d["version"] == 3 and d["widgetType"] == "pie":
            return PieSpec.from_dict(d)
        elif d["version"] == 3 and d["widgetType"] == "pivot":
            return PivotSpec.from_dict(d)
        elif d["version"] == 3 and d["widgetType"] == "scatter":
            return ScatterSpec.from_dict(d)
        else:
            raise KeyError(...)


@dataclass
class WordCloudEncodingMap:
    size: RenderFieldEncoding | None = None
    text: RenderFieldEncoding | None = None

    def as_dict(self) -> Json:
        body: Json = {}
        if self.size:
            body["size"] = self.size.as_dict()
        if self.text:
            body["text"] = self.text.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> WordCloudEncodingMap:
        return cls(size=_from_dict(d, "size", RenderFieldEncoding), text=_from_dict(d, "text", RenderFieldEncoding))


@dataclass
class WordCloudSpec:
    encodings: WordCloudEncodingMap
    frame: WidgetFrameSpec | None = None

    def as_dict(self) -> Json:
        body: Json = {
            "version": 2,
            "widgetType": "word-cloud",
        }
        if self.encodings:
            body["encodings"] = self.encodings.as_dict()
        if self.frame:
            body["frame"] = self.frame.as_dict()
        return body

    @classmethod
    def from_dict(cls, d: Json) -> WordCloudSpec:
        return cls(
            encodings=_from_dict(d, "encodings", WordCloudEncodingMap), frame=_from_dict(d, "frame", WidgetFrameSpec)
        )
