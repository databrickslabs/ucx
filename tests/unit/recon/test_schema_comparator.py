from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.recon.base import TableIdentifier, SchemaComparisonResult, SchemaComparisonEntry
from databricks.labs.ucx.recon.metadata_retriever import DatabricksTableMetadataRetriever
from databricks.labs.ucx.recon.schema_comparator import StandardSchemaComparator


def test_schema_comparison_success(metadata_row_factory):
    source = TableIdentifier("hive_metastore", "db1", "table1")
    target = TableIdentifier("catalog1", "schema1", "table1")
    sql_backend = MockBackend(
        rows={
            "DESCRIBE TABLE": metadata_row_factory[
                ("col1", "int"),
                ("col2", "string"),
            ],
            f"{target.catalog}\\.information_schema\\.columns": metadata_row_factory[
                ("col1", "int"),
                ("col2", "string"),
            ],
        }
    )

    expected_comparison_result = SchemaComparisonResult(
        is_matching=True,
        data=[
            SchemaComparisonEntry(
                source_column="col1",
                source_datatype="int",
                target_column="col1",
                target_datatype="int",
                is_matching=True,
                notes=None,
            ),
            SchemaComparisonEntry(
                source_column="col2",
                source_datatype="string",
                target_column="col2",
                target_datatype="string",
                is_matching=True,
                notes=None,
            ),
        ],
    )

    metadata_retriever = DatabricksTableMetadataRetriever(sql_backend)
    schema_comparator = StandardSchemaComparator(metadata_retriever)
    actual_comparison_result = schema_comparator.compare_schema(source, target)
    assert actual_comparison_result == expected_comparison_result


def test_schema_comparison_failure(metadata_row_factory):
    source = TableIdentifier("hive_metastore", "db1", "table1")
    target = TableIdentifier("catalog1", "schema1", "table1")
    sql_backend = MockBackend(
        rows={
            "DESCRIBE TABLE": metadata_row_factory[
                ("col1", "int"),
                ("col3", "array<string>"),
                ("# col_name", "data_type"),
            ],
            f"{target.catalog}\\.information_schema\\.columns": metadata_row_factory[
                ("col1", "int"),
                ("col2", "string"),
            ],
        }
    )

    expected_comparison_result = SchemaComparisonResult(
        is_matching=False,
        data=[
            SchemaComparisonEntry(
                source_column="col1",
                source_datatype="int",
                target_column="col1",
                target_datatype="int",
                is_matching=True,
                notes=None,
            ),
            SchemaComparisonEntry(
                source_column=None,
                source_datatype=None,
                target_column="col2",
                target_datatype="string",
                is_matching=False,
                notes="Column is missing in source",
            ),
            SchemaComparisonEntry(
                source_column="col3",
                source_datatype="array<string>",
                target_column=None,
                target_datatype=None,
                is_matching=False,
                notes="Column is missing in target",
            ),
        ],
    )

    metadata_retriever = DatabricksTableMetadataRetriever(sql_backend)
    schema_comparator = StandardSchemaComparator(metadata_retriever)
    actual_comparison_result = schema_comparator.compare_schema(source, target)
    assert actual_comparison_result == expected_comparison_result
