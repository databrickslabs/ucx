# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
df = spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df.write.format("delta").saveAsTable("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    df.write.format("delta").saveAsTable("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    df.write.format("delta").saveAsTable("old.things", None, None, None, "extra-argument")

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df.write.saveAsTable(format="xyz", name="old.things")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate:+1:0:+1:0] Can't migrate 'saveAsTable' because its table name argument is not a constant
    df.write.format("delta").saveAsTable(name)
    # ucx[table-migrate:+1:0:+1:0] Can't migrate 'saveAsTable' because its table name argument is not a constant
    df.write.format("delta").saveAsTable(f"boop{stuff}")
