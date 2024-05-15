from databricks.labs.ucx.recon import (
    SchemaComparator,
    SchemaComparisonEntry,
    SchemaComparisonResult,
    TableMetadataRetriever,
    TableDescriptor,
    ColumnMetadata,
)


class StandardSchemaComparator(SchemaComparator):
    def __init__(self, metadata_retriever: TableMetadataRetriever):
        self._metadata_retriever = metadata_retriever

    def compare_schema(self, source: TableDescriptor, target: TableDescriptor) -> SchemaComparisonResult:
        comparison_result = self._eval_schema_diffs(source, target)
        is_matching = all(entry.is_matching for entry in comparison_result)
        return SchemaComparisonResult(is_matching, comparison_result)

    def _eval_schema_diffs(self, source: TableDescriptor, target: TableDescriptor) -> list[SchemaComparisonEntry]:
        source_metadata = self._metadata_retriever.get_metadata(source)
        target_metadata = self._metadata_retriever.get_metadata(target)
        source_column_names = {column.name for column in source_metadata.columns}
        target_column_names = {column.name for column in target_metadata.columns}
        all_column_names = source_column_names.union(target_column_names)
        comparison_result = []
        for field_name in all_column_names:
            source_col = source_metadata.get_column_metadata(field_name)
            target_col = target_metadata.get_column_metadata(field_name)
            comparison_result.append(_build_comparison_result_entry(source_col, target_col))
        return comparison_result


def _build_comparison_result_entry(
    source_col: ColumnMetadata | None, target_col: ColumnMetadata | None
) -> SchemaComparisonEntry:
    if source_col and target_col:
        is_matching = _compare_col_metadata(source_col, target_col)
        notes = None
    else:
        is_matching = False
        notes = "Column is missing in " + ("target" if source_col else "source")

    return SchemaComparisonEntry(
        source_column=source_col.name if source_col else None,
        source_datatype=source_col.data_type if source_col else None,
        target_column=target_col.name if target_col else None,
        target_datatype=target_col.data_type if target_col else None,
        is_matching=is_matching,
        notes=notes,
    )


def _compare_col_metadata(source_col: ColumnMetadata, target_col: ColumnMetadata) -> bool:
    if source_col is None and target_col is None:
        return True
    if source_col is None or target_col is None:
        return False
    return source_col.name == target_col.name and source_col.data_type == target_col.data_type
