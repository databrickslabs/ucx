# ucx[direct-file-system-access:+1:0:+1:34] The use of direct file system access is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrated-to-uc:+1:14:+1:53] Table old.things is migrated to brand.new.stuff in Unity Catalog
    columns = spark.catalog.listColumns("old.things")
    # TODO: Fix missing migration warning:
    # #ucx[table-migrated-to-uc:+1:1:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    columns = spark.catalog.listColumns("things", "old")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    columns = spark.catalog.listColumns("table.we.know.nothing.about")
    columns = spark.catalog.listColumns("other.things")
    columns = spark.catalog.listColumns("old.socks")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    columns = spark.catalog.listColumns("old.things", None, "extra-argument")

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # TODO: Fix missing migration warning:
    # #ucx[table-migrated-to-uc:+1:1:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    columns = spark.catalog.listColumns(dbName="old", name="things")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[cannot-autofix-table-reference:+1:14:+1:45] Can't migrate 'spark.catalog.listColumns(name)' because its table name argument cannot be computed
    columns = spark.catalog.listColumns(name)
    # ucx[cannot-autofix-table-reference:+1:14:+1:55] Can't migrate 'spark.catalog.listColumns(f'boop{stuff}')' because its table name argument cannot be computed
    columns = spark.catalog.listColumns(f"boop{stuff}")

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    something_else.listColumns("old.things")
    a_function("old.things")
