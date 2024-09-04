import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class TableIdentifier:
    catalog: str
    schema: str
    table: str

    @property
    def catalog_escaped(self):
        return f"`{self.catalog.replace('`','``')}`"

    @property
    def schema_escaped(self):
        return f"`{self.schema.replace('`','``')}`"

    @property
    def table_escaped(self):
        return f"`{self.table.replace('`','``')}`"

    @property
    def fqn_escaped(self):
        return f"{self.catalog_escaped}.{self.schema_escaped}.{self.table_escaped}"


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

    def as_dict(self):
        return dataclasses.asdict(self)


@dataclass
class DataComparisonResult:
    source_row_count: int
    target_row_count: int
    target_missing_count: int = 0
    source_missing_count: int = 0

    def as_dict(self):
        return dataclasses.asdict(self)


class TableMetadataRetriever(ABC):
    @abstractmethod
    def get_metadata(self, entity: TableIdentifier) -> TableMetadata:
        """
        Get metadata for a given table
        """


class DataProfiler(ABC):
    @abstractmethod
    def profile_data(self, entity: TableIdentifier) -> DataProfilingResult:
        """
        Profile data for a given table
        """


class SchemaComparator(ABC):
    @abstractmethod
    def compare_schema(self, source: TableIdentifier, target: TableIdentifier) -> SchemaComparisonResult:
        """
        Compare schema for two tables
        """


class DataComparator(ABC):
    @abstractmethod
    def compare_data(
        self,
        source: TableIdentifier,
        target: TableIdentifier,
        compare_rows: bool,
    ) -> DataComparisonResult:
        """
        Compare data for two tables
        """
