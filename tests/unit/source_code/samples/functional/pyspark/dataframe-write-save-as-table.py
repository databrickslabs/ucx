# ucx[direct-file-system-access:+1:5:+1:39] The use of direct file system access is deprecated: s3://bucket/path
df = spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrated-to-uc:+1:4:+1:54] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df.write.format("delta").saveAsTable("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    df.write.format("delta").saveAsTable("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    df.write.format("delta").saveAsTable("old.things", None, None, None, "extra-argument")

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # ucx[table-migrated-to-uc:+1:4:+1:57] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df.write.saveAsTable(format="xyz", name="old.things")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[cannot-autofix-table-reference:+1:4:+1:46] Can't migrate 'df.write.format('delta').saveAsTable(name)' because its table name argument cannot be computed
    df.write.format("delta").saveAsTable(name)
    # ucx[cannot-autofix-table-reference:+1:4:+1:56] Can't migrate 'df.write.format('delta').saveAsTable(f'boop{stuff}')' because its table name argument cannot be computed
    df.write.format("delta").saveAsTable(f"boop{stuff}")

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    something_else.saveAsTable("old.things")
    a_function("old.things")
