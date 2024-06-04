# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    columns = spark.catalog.listColumns("old.things")
    # TODO: Fix missing migration warning:
    # #ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    columns = spark.catalog.listColumns("things", "old")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    columns = spark.catalog.listColumns("table.we.know.nothing.about")
    columns = spark.catalog.listColumns("other.things")
    columns = spark.catalog.listColumns("old.socks")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    columns = spark.catalog.listColumns("old.things", None, "extra-argument")

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # TODO: Fix missing migration warning:
    # #ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    columns = spark.catalog.listColumns(dbName="old", name="things")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+1:0:+1:0] Can't migrate 'listColumns' because its table name argument is not a constant
    columns = spark.catalog.listColumns(name)
    # ucx[table-migrate:+1:0:+1:0] Can't migrate 'listColumns' because its table name argument is not a constant
    columns = spark.catalog.listColumns(f"boop{stuff}")
