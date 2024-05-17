from .base import (
    SchemaComparator,
    SchemaComparisonEntry,
    SchemaComparisonResult,
    TableMetadataRetriever,
    ColumnMetadata,
    TableIdentifier,
)


class StandardSchemaComparator(SchemaComparator):
    def __init__(self, metadata_retriever: TableMetadataRetriever):
        self._metadata_retriever = metadata_retriever

    def compare_schema(self, source: TableIdentifier, target: TableIdentifier) -> SchemaComparisonResult:
        """
        This method compares the schema of two tables. It takes two TableIdentifier objects as input, which represent
        the source and target tables for which the schemas are to be compared.

        Note: This method does not handle exceptions raised during the execution of the SQL query or the retrieval
        of the table metadata. These exceptions are expected to be handled by the caller in a manner appropriate for
        their context.
        """
        comparison_result = self._eval_schema_diffs(source, target)
        is_matching = all(entry.is_matching for entry in comparison_result)
        return SchemaComparisonResult(is_matching, comparison_result)

    def _eval_schema_diffs(self, source: TableIdentifier, target: TableIdentifier) -> list[SchemaComparisonEntry]:
        source_metadata = self._metadata_retriever.get_metadata(source)
        target_metadata = self._metadata_retriever.get_metadata(target)
        # Combine the sets of column names for both the source and target tables
        # to create a set of all unique column names from both tables.
        source_column_names = {column.name for column in source_metadata.columns}
        target_column_names = {column.name for column in target_metadata.columns}
        all_column_names = source_column_names.union(target_column_names)
        comparison_result = []
        # Compare the column metadata from each table with a logic similar to full outer join.
        for field_name in sorted(all_column_names):
            source_col = source_metadata.get_column_metadata(field_name)
            target_col = target_metadata.get_column_metadata(field_name)
            comparison_result.append(_build_comparison_result_entry(source_col, target_col))
        return comparison_result


def _build_comparison_result_entry(
    source_col: ColumnMetadata | None,
    target_col: ColumnMetadata | None,
) -> SchemaComparisonEntry:
    if source_col and target_col:
        is_matching = source_col == target_col
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
