# ucx[direct-filesystem-access:+1:0:+1:0] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")

## Check that we generate a warning when the return value (which has changed) is used.
# ucx[table-migrate:+1:0:+1:0] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
for table in spark.catalog.listTables():
    do_stuff_with_table(table)

## Check that we generate a warning when we reference a migrated database.
# ucx[table-migrate:+3:0:+3:0] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
## TODO: The following isn't yet implemented:
## ucx[table-migrate:+1:0:+1:0] Database 'old' is migrated to 'brand.new' in Unity Catalog
for table in spark.catalog.listTables("old"):
    do_stuff_with_table(table)

## Check that we do not generate a warning when we reference a non-migrated database.
# ucx[table-migrate:+1:0:+1:0] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
for table in spark.catalog.listTables("unknown"):
    do_stuff_with_table(table)

## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
# ucx[table-migrate:+1:0:+1:0] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
for table in spark.catalog.listTables("old", None, "extra-argument"):
    do_stuff_with_table(table)

## Check a call with an out-of-position named argument referencing a database known to be migrated.
# ucx[table-migrate:+3:0:+3:0] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
## TODO: The following isn't yet implemented:
## ucx[table-migrate:+1:0:+1:0] Database 'old' is migrated to 'brand.new' in Unity Catalog
for table in spark.catalog.listTables(pattern="st*", dbName="old"):
    do_stuff_with_table(table)
