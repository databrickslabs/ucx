## Check a literal reference to a known table that is migrated.
# ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
spark.catalog.cacheTable("old.things")

## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
spark.catalog.cacheTable("table.we.know.nothing.about")

## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
spark.catalog.cacheTable("old.things", None, "extra-argument")

## Check a call with an out-of-position named argument referencing a table known to be migrated.
# ucx[table-migrate:+1:0:+1:0] Table old.things is migrated to brand.new.stuff in Unity Catalog
spark.catalog.cacheTable(storageLevel=None, tableName="old.things")

## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
# ucx[table-migrate:+1:0:+1:0] Can't migrate 'cacheTable' because its table name argument is not a constant
spark.catalog.cacheTable(name)
# ucx[table-migrate:+1:0:+1:0] Can't migrate 'cacheTable' because its table name argument is not a constant
spark.catalog.cacheTable(f"boop{stuff}")
