# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:7:+1:46] Table old.things is migrated to brand.new.stuff in Unity Catalog
    if spark.catalog.tableExists("old.things"):
        pass
    # TODO: Fix missing migration warning:
    # #ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    if spark.catalog.tableExists("things", "old"):
        pass

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    if spark.catalog.tableExists("table.we.know.nothing.about"):
        pass

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    if spark.catalog.tableExists("old.things", None, "extra-argument"):
        pass

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # TODO: Fix missing migration warning
    # # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    if spark.catalog.tableExists(dbName="old", name="things"):
        pass

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+1:7:+1:38] Can't migrate 'spark.catalog.tableExists(name)' because its table name argument cannot be computed
    if spark.catalog.tableExists(name):
        pass
    # ucx[table-migrate:+1:7:+1:48] Can't migrate 'spark.catalog.tableExists(f'boot{stuff}')' because its table name argument cannot be computed
    if spark.catalog.tableExists(f"boot{stuff}"):
        pass

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    # FIXME: This is a false positive; any method named 'tableExists' is triggering the warning.
    # ucx[table-migrate:+1:4:+1:44] Table old.things is migrated to brand.new.stuff in Unity Catalog
    something_else.tableExists("old.things")
    a_function("old.things")
