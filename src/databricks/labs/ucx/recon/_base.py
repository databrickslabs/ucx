from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class TableDescriptor:
    catalog: str
    schema: str
    table: str


@dataclass
class ColumnMetadata:
    name: str
    data_type: str


@dataclass
class TableMetadata:
    descriptor: TableDescriptor
    columns: list[ColumnMetadata]

    def get_column_metadata(self, column_name: str) -> ColumnMetadata | None:
        for column in self.columns:
            if column.name == column_name:
                return column
        return None


@dataclass
class DataProfilingResult:
    row_count: int
    table_metadata: TableMetadata


@dataclass
class SchemaComparisonEntry:
    source_column: str | None
    source_datatype: str | None
    target_column: str | None
    target_datatype: str | None
    is_matching: bool
    notes: str | None


@dataclass
class SchemaComparisonResult:
    is_matching: bool
    data: list[SchemaComparisonEntry]


@dataclass
class DataComparisonResult:
    pass


class TableMetadataRetriever(ABC):
    @abstractmethod
    def get_metadata(self, source: TableDescriptor) -> TableMetadata:
        pass


class DataProfiler(ABC):
    @abstractmethod
    def profile_data(self, source: TableDescriptor) -> DataProfilingResult:
        pass


class SchemaComparator(ABC):
    @abstractmethod
    def compare_schema(self, source: TableDescriptor, target: TableDescriptor) -> SchemaComparisonResult:
        pass


class DataComparator(ABC):
    @abstractmethod
    def compare_data(self, source: TableDescriptor, target: TableDescriptor) -> DataComparisonResult:
        pass
