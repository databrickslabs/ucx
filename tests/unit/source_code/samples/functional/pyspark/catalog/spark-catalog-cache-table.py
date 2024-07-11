## Check a literal reference to a known table that is migrated.
# ucx[table-migrate:+1:0:+1:38] Table old.things is migrated to brand.new.stuff in Unity Catalog
spark.catalog.cacheTable("old.things")

## Check a literal reference to an unknown table (that is not migrated); we expect no warning.
spark.catalog.cacheTable("table.we.know.nothing.about")

## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
spark.catalog.cacheTable("old.things", None, "extra-argument")

## Check a call with an out-of-position named argument referencing a table known to be migrated.
# ucx[table-migrate:+1:0:+1:67] Table old.things is migrated to brand.new.stuff in Unity Catalog
spark.catalog.cacheTable(storageLevel=None, tableName="old.things")

## Some calls that use a variable whose value is unknown: they could potentially reference a migrated table.
# ucx[table-migrate-cannot-compute-value:+1:0:+1:30] Can't migrate 'spark.catalog.cacheTable(name)' because its table name argument cannot be computed
spark.catalog.cacheTable(name)
# ucx[table-migrate-cannot-compute-value:+1:0:+1:40] Can't migrate 'spark.catalog.cacheTable(f'boop{stuff}')' because its table name argument cannot be computed
spark.catalog.cacheTable(f"boop{stuff}")

## Some trivial references to the method or table in unrelated contexts that should not trigger warnigns.
something_else.cacheTable("old.things")
a_function("old.things")
