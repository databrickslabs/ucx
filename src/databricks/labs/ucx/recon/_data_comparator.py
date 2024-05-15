from pyspark.sql import DataFrame

from databricks.labs.ucx.recon import DataComparator, DataComparisonResult


class StandardDataComparator(DataComparator):
    def compare_data(self, source: DataFrame, target: DataFrame) -> DataComparisonResult:
        return DataComparisonResult()
