# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+3:9:+3:34] Table old.things is migrated to brand.new.stuff in Unity Catalog
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:9:+1:34] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.table("old.things")
    do_stuff_with(df)

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:9:+1:51] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.table("table.we.know.nothing.about")
    do_stuff_with(df)

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    df = spark.table("old.things", "extra-argument")
    do_stuff_with(df)

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+3:9:+3:26] Can't migrate 'spark.table(name)' because its table name argument cannot be computed
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:9:+1:26] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.table(name)
    do_stuff_with(df)
    # ucx[table-migrate:+3:9:+3:36] Can't migrate 'spark.table(f'boop{stuff}')' because its table name argument cannot be computed
    # TODO: Fix false positive:
    # ucx[table-migrate:+1:9:+1:36] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    df = spark.table(f"boop{stuff}")
    do_stuff_with(df)

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    # FIXME: These are false positives; any method named 'table' is triggering the warnings.
    # ucx[table-migrate:+2:4:+2:38] Table old.things is migrated to brand.new.stuff in Unity Catalog
    # ucx[table-migrate:+1:4:+1:38] The default format changed in Databricks Runtime 8.0, from Parquet to Delta
    something_else.table("old.things")
    a_function("old.things")
