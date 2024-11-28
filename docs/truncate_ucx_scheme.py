#Script to truncate UCX tables as a prerequisite for UCX Assessment rerun 
#to run in any Notebook
#%python
from pyspark.sql import SparkSession

# Initialize SparkSession (if not already initialized)
spark = SparkSession.builder.appName("TruncateAllTables").getOrCreate()

# Set the database context
spark.sql("USE hive_metastore.ucx")

# Get list of all tables
tables = spark.catalog.listTables()
tables_only = [table for table in tables if table.tableType != "VIEW"]
print("Tables to be truncated:")
for table in tables_only:
    print(table.name)

print("\n Truncation is started:")
# Truncate each table
for table in tables_only:
    try:
        print(f"Truncating table: {table.name}")
        spark.sql(f"TRUNCATE TABLE {table.name}")
        print(f"Successfully truncated table: {table.name}")
    except Exception as e:
        print(f"Error truncating table {table.name}: {str(e)}")

print("Truncation process completed.")