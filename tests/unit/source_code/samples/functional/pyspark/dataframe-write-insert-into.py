# ucx[direct-filesystem-access:+1:5:+1:39] The use of direct filesystem references is deprecated: s3://bucket/path
df = spark.read.csv("s3://bucket/path")
for i in range(10):

    ## Check a literal reference to a known table that is migrated.
    # ucx[table-migrate:+1:4:+1:37] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df.write.insertInto("old.things")

    ## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
    df.write.insertInto("table.we.know.nothing.about")

    ## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
    df.write.insertInto("old.things", None, "extra-argument")

    ## Check a call with an out-of-position named argument referencing a table known to be migrated.
    # ucx[table-migrate:+1:4:+1:63] Table old.things is migrated to brand.new.stuff in Unity Catalog
    df.write.insertInto(overwrite=None, tableName="old.things")

    ## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
    # ucx[table-migrate-cannot-compute-value:+1:4:+1:29] Can't migrate 'df.write.insertInto(name)' because its table name argument cannot be computed
    df.write.insertInto(name)
    # ucx[table-migrate-cannot-compute-value:+1:4:+1:39] Can't migrate 'df.write.insertInto(f'boop{stuff}')' because its table name argument cannot be computed
    df.write.insertInto(f"boop{stuff}")

    ## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
    something_else.insertInto("old.things")
    a_function("old.things")
