from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class TableIdentifier:
    catalog: str
    schema: str
    table: str

    @property
    def fqn(self):
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class ColumnMetadata:
    name: str
    data_type: str


@dataclass
class TableMetadata:
    identifier: TableIdentifier
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
    source_row_count: int
    target_row_count: int
    source_to_target_mismatch_count: int
    target_to_source_mismatch_count: int


class TableMetadataRetriever(ABC):
    @abstractmethod
    def get_metadata(self, entity: TableIdentifier) -> TableMetadata:
        pass


class DataProfiler(ABC):
    @abstractmethod
    def profile_data(self, entity: TableIdentifier) -> DataProfilingResult:
        pass


class SchemaComparator(ABC):
    @abstractmethod
    def compare_schema(self, source: TableIdentifier, target: TableIdentifier) -> SchemaComparisonResult:
        pass


class DataComparator(ABC):
    @abstractmethod
    def compare_data(self, source: TableIdentifier, target: TableIdentifier) -> DataComparisonResult:
        pass
