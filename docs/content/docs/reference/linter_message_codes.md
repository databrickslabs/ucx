# Linter message codes

![code compatibility problems](/images/code_compatibility_problems.png)

Here's the detailed explanation of the linter message codes:

## `cannot-autofix-table-reference`

This indicates that the linter has found a table reference that cannot be automatically fixed. The user must manually
update the table reference to point to the correct table in Unity Catalog. This mostly occurs, when table name is
computed dynamically, and it's too complex for our static code analysis to detect it. We detect this problem anywhere
where table name could be used: `spark.sql`, `spark.catalog.*`, `spark.table`, `df.write.*` and many more. Code examples
that trigger this problem:

```python
spark.table(f"foo_{some_table_name}")
# ..
df = spark.range(10)
df.write.saveAsTable(f"foo_{some_table_name}")
# .. or even
df.write.insertInto(f"foo_{some_table_name}")
```

Here the `some_table_name` variable is not defined anywhere in the visible scope. Though, the analyser would
successfully detect table name if it is defined:

```python
some_table_name = 'bar'
spark.table(f"foo_{some_table_name}")
```

We even detect string constants when coming either from `dbutils.widgets.get` (via job named parameters) or through
loop variables. If `old.things` table is migrated to `brand.new.stuff` in Unity Catalog, the following code will
trigger two messages: [`table-migrated-to-uc`](#table-migrated-to-uc) for the first query, as the contents are clearly
analysable, and `cannot-autofix-table-reference` for the second query.

```python
# ucx[table-migrated-to-uc:+4:4:+4:20] Table old.things is migrated to brand.new.stuff in Unity Catalog
# ucx[cannot-autofix-table-reference:+3:4:+3:20] Can't migrate table_name argument in 'spark.sql(query)' because its value cannot be computed
table_name = f"table_{index}"
for query in ["SELECT * FROM old.things", f"SELECT * FROM {table_name}"]:
    spark.sql(query).collect()
```



## `catalog-api-in-shared-clusters`

`spark.catalog.*` functions require Databricks Runtime 14.3 LTS or above on Unity Catalog clusters in Shared access
mode, so of your code has `spark.catalog.tableExists("table")` or `spark.catalog.listDatabases()`, you need to ensure
that your cluster is running the correct runtime version and data security mode.



## `changed-result-format-in-uc`

Calls to these functions would return a list of `<catalog>.<database>.<table>` instead of `<database>.<table>`. So if
you have code like this:

```python
for table in spark.catalog.listTables():
    do_stuff_with_table(table)
```

you need to make sure that `do_stuff_with_table` can handle the new format.



## `direct-filesystem-access-in-sql-query`

Direct filesystem access is deprecated in Unity Catalog.
DBFS is no longer supported, so if you have code like this:

```python
df = spark.sql("SELECT * FROM parquet.`/mnt/foo/path/to/parquet.file`")
```

you need to change it to use UC tables.



## `direct-filesystem-access`

Direct filesystem access is deprecated in Unity Catalog.
DBFS is no longer supported, so if you have code like this:

```python
display(spark.read.csv('/mnt/things/data.csv'))
```

or this:

```python
display(spark.read.csv('s3://bucket/folder/data.csv'))
```

You need to change it to use UC tables or UC volumes.



## `dependency-not-found`

This message indicates that the linter has found a dependency, like Python source file or a notebook, that is not
available in the workspace. The user must ensure that the dependency is available in the workspace. This usually
means an error in the user code.



## `jvm-access-in-shared-clusters`

You cannot access Spark Driver JVM on Unity Catalog clusters in Shared Access mode. If you have code like this:

```python
spark._jspark._jvm.com.my.custom.Name()
```

or like this:

```python
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
```

you need to change it to use Python equivalents.



## `legacy-context-in-shared-clusters`

SparkContext (`sc`) is not supported on Unity Catalog clusters in Shared access mode. Rewrite it using SparkSession
(`spark`). Example code that triggers this message:

```python
df = spark.createDataFrame(sc.emptyRDD(), schema)
```

or this:

```python
sc.parallelize([1, 2, 3])
```



## `not-supported`

Installing eggs is no longer supported on Databricks 14.0 or higher.



## `notebook-run-cannot-compute-value`

Path for `dbutils.notebook.run` cannot be computed and requires adjusting the notebook path.
It is not clear for automated code analysis where the notebook is located, so you need to simplify the code like:

```python
b = some_function()
dbutils.notebook.run(b)
```

to something like this:

```python
a = "./leaf1.py"
dbutils.notebook.run(a)
```



## `python-udf-in-shared-clusters`

`applyInPandas` requires DBR 14.3 LTS or above on Unity Catalog clusters in Shared access mode. Example:

```python
df.groupby("id").applyInPandas(subtract_mean, schema="id long, v double").show()
```

Arrow UDFs require DBR 14.3 LTS or above on Unity Catalog clusters in Shared access mode.

```python
@udf(returnType='int', useArrow=True)
def arrow_slen(s):
    return len(s)
```

It is not possible to register Java UDF from Python code on Unity Catalog clusters in Shared access mode. Use a
`%scala` cell to register the Scala UDF using `spark.udf.register`. Example code that triggers this message:

```python
spark.udf.registerJavaFunction("func", "org.example.func", IntegerType())
```



## `rdd-in-shared-clusters`

RDD APIs are not supported on Unity Catalog clusters in Shared access mode. Use mapInArrow() or Pandas UDFs instead.

```python
df.rdd.mapPartitions(myUdf)
```



## `spark-logging-in-shared-clusters`

Cannot set Spark log level directly from code on Unity Catalog clusters in Shared access mode. Remove the call and set
the cluster spark conf `spark.log.level` instead:

```python
sc.setLogLevel("INFO")
setLogLevel("WARN")
```

Another example could be:

```python
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
```

or

```python
sc._jvm.org.apache.log4j.LogManager.getLogger(__name__).info("test")
```



## `sql-parse-error`

This is a generic message indicating that the SQL query could not be parsed. The user must manually check the SQL query.



## `sys-path-cannot-compute-value`

Path for `sys.path.append` cannot be computed and requires adjusting the path. It is not clear for automated code
analysis where the path is located.



## `table-migrated-to-uc`

This message indicates that the linter has found a table that has been migrated to Unity Catalog. The user must ensure
that the table is available in Unity Catalog.



## `to-json-in-shared-clusters`

`toJson()` is not available on Unity Catalog clusters in Shared access mode. Use `toSafeJson()` on DBR 13.3 LTS or
above to get a subset of command context information. Example code that triggers this message:

```python
dbutils.notebook.entry_point.getDbutils().notebook().getContext().toSafeJson()
```



## `unsupported-magic-line`

This message indicates the code that could not be analysed by UCX. User must check the code manually.


