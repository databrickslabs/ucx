from databricks.labs.ucx.recon import DataComparator, DataComparisonResult, TableDescriptor


class StandardDataComparator(DataComparator):

    def compare_data(self, source: TableDescriptor, target: TableDescriptor) -> DataComparisonResult:
        return DataComparisonResult()
