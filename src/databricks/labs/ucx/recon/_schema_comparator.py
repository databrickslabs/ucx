from itertools import zip_longest

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType, DataType, ArrayType

from databricks.labs.ucx.recon import SchemaComparator, SchemaComparisonEntry, SchemaComparisonResult


class StandardSchemaComparator(SchemaComparator):

    def compare_schema(self, source: DataFrame, target: DataFrame) -> SchemaComparisonResult:
        comparison_result = _compare_schema(source, target)
        is_matching = all(entry.is_matching for entry in comparison_result)
        return SchemaComparisonResult(is_matching, comparison_result)


def _compare_schema(source: DataFrame, target: DataFrame) -> list[SchemaComparisonEntry]:
    all_field_names = set(source.schema.fieldNames()).union(target.schema.fieldNames())
    comparison_result = []
    for field_name in all_field_names:
        source_field = _get_struct_field(source.schema, field_name)
        target_field = _get_struct_field(target.schema, field_name)
        comparison_result.append(_build_comparison_result_entry(source_field, target_field))
    return comparison_result


def _get_struct_field(struct: StructType, field_name: str) -> StructField | None:
    for field in struct.fields:
        if field.name == field_name:
            return field
    return None


def _build_comparison_result_entry(
    source_field: StructField | None, target_field: StructField | None
) -> SchemaComparisonEntry:
    if source_field and target_field:
        return SchemaComparisonEntry(
            source_column=source_field.name,
            source_datatype=source_field.dataType.simpleString(),
            target_column=target_field.name,
            target_datatype=target_field.dataType.simpleString(),
            is_matching=_compare_struct_fields(source_field, target_field),
            notes=None,
        )
    elif source_field:
        return SchemaComparisonEntry(
            source_column=source_field.name,
            source_datatype=source_field.dataType.simpleString(),
            target_column=None,
            target_datatype=None,
            is_matching=False,
            notes="Column is missing in target.",
        )
    elif target_field:
        return SchemaComparisonEntry(
            source_column=None,
            source_datatype=None,
            target_column=target_field.name,
            target_datatype=target_field.dataType.simpleString(),
            is_matching=False,
            notes="Column is missing in source.",
        )
    else:
        raise ValueError("Both source and target fields are None.")


def _compare_schemas(s1: StructType, s2: StructType) -> bool:
    if len(s1) != len(s2):
        return False
    zipped = zip_longest(s1, s2)
    for sf1, sf2 in zipped:
        if not _compare_struct_fields(sf1, sf2):
            return False
    return True


def _compare_struct_fields(source_sf: StructField, target_sf: StructField) -> bool:
    if source_sf is None and target_sf is None:
        return True
    elif source_sf is None or target_sf is None:
        return False
    if source_sf.name != target_sf.name:
        return False
    else:
        return _compare_datatypes(source_sf.dataType, target_sf.dataType)


def _compare_datatypes(dt1: DataType, dt2: DataType) -> bool:
    if dt1.typeName() == dt2.typeName():
        if isinstance(dt1, ArrayType) and isinstance(dt2, ArrayType):
            return _compare_datatypes(dt1.elementType, dt2.elementType)
        elif isinstance(dt1, StructType) and isinstance(dt2, StructType):
            return _compare_schemas(dt1, dt2)
        else:
            return True
    else:
        return False
