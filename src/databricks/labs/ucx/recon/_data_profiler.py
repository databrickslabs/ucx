from pyspark.sql import DataFrame

from databricks.labs.ucx.recon import DataProfiler, DataProfilingResult


class StandardDataProfiler(DataProfiler):
    def profile_data(self, source: DataFrame) -> DataProfilingResult:
        row_count = source.count()
        return DataProfilingResult(row_count=row_count)
