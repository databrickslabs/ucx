# ucx[direct-filesystem-access:+1:0:+1:34] The use of direct filesystem references is deprecated: s3://bucket/path
spark.read.csv("s3://bucket/path")

## Check that we generate a warning when the return value (which has changed) is used.
# ucx[changed-result-format-in-uc:+1:13:+1:39] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
for table in spark.catalog.listTables():
    do_stuff_with_table(table)

## Check that we generate a warning when we reference a migrated database.
# ucx[changed-result-format-in-uc:+3:13:+3:44] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
## TODO: The following isn't yet implemented:
## ucx[changed-result-format-in-uc:+1:13:+1:0] Database 'old' is migrated to 'brand.new' in Unity Catalog
for table in spark.catalog.listTables("old"):
    do_stuff_with_table(table)

## Check that we do not generate a warning when we reference a non-migrated database.
# ucx[changed-result-format-in-uc:+1:13:+1:48] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
for table in spark.catalog.listTables("unknown"):
    do_stuff_with_table(table)

## Check that a call with too many positional arguments is ignored as (presumably) something else; we expect no warning.
# ucx[changed-result-format-in-uc:+1:13:+1:68] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
for table in spark.catalog.listTables("old", None, "extra-argument"):
    do_stuff_with_table(table)

## Check a call with an out-of-position named argument referencing a database known to be migrated.
# ucx[changed-result-format-in-uc:+3:13:+3:66] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
## TODO: The following isn't yet implemented:
## ucx[changed-result-format-in-uc:+1:13:+1:0] Database 'old' is migrated to 'brand.new' in Unity Catalog
for table in spark.catalog.listTables(pattern="st*", dbName="old"):
    do_stuff_with_table(table)

## Some calls that use a variable whose value is unknown: they could potentially reference a migrated database.
# ucx[changed-result-format-in-uc:+3:13:+3:43] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
## TODO: The following isn't yet implemented:
## ucx[changed-result-format-in-uc:+1:13:+1:0] Can't migrate 'listTables' because its database name argument cannot be computed
for table in spark.catalog.listTables(name):
    do_stuff_with_table(table)
# ucx[changed-result-format-in-uc:+3:13:+3:53] Call to 'listTables' will return a list of <catalog>.<database>.<table> instead of <database>.<table>.
## TODO: The following isn't yet implemented:
## ucx[changed-result-format-in-uc:+1:13:+1:0] Can't migrate 'listTables' because its database name argument cannot be computed
for table in spark.catalog.listTables(f"boop{stuff}"):
    do_stuff_with_table(table)

## Some trivial references to the method or table in unrelated contexts that should not trigger warnings.
something_else.listTables("old.things")
a_function("old.things")
