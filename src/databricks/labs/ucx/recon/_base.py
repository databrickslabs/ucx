from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class DataProfilingResult:
    row_count: int


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


class DataProfiler(ABC):
    @abstractmethod
    def profile_data(self, source: DataFrame) -> DataProfilingResult:
        pass


class SchemaComparator(ABC):
    @abstractmethod
    def compare_schema(self, source: DataFrame, target: DataFrame) -> SchemaComparisonResult:
        pass


class DataComparator(ABC):
    @abstractmethod
    def compare_data(self, source: DataFrame, target: DataFrame) -> DataComparisonResult:
        pass
