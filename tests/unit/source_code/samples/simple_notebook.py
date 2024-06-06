# Databricks notebook source
# MAGIC %md # Simple Notebook (Multiple cell types)

# COMMAND ----------

# This is a python cell, the default (non-magic) type.
print(sys.version)

# COMMAND ----------

# MAGIC %md And this is a Markdown cell.
# MAGIC
# MAGIC It has two paragraphs.

# COMMAND ----------

# MAGIC %sql select 1 -- This is a single-line SQL cell.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This is a multi-line SQL statement.
# MAGIC %sql
# MAGIC select foo
# MAGIC from values('baz', 'daz'),('diz', 'fiz') as (foo, bar)
# MAGIC where bar <> 'daz'

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %scala
# MAGIC println(sys.props)

# COMMAND ----------

# MAGIC %sh ls -ltr /

# COMMAND ----------

# MAGIC %pip install tqdm

# COMMAND ----------

# MAGIC %python
# MAGIC # Explicitly-marked python cell.
# MAGIC from tqdm.notebook import tqdm
# MAGIC count = 0
# MAGIC with open("/etc/passwd") as f:
# MAGIC     lines = list(f)
# MAGIC for line in tqdm(lines):
# MAGIC     fields = line.split(":")
# MAGIC     if len(fields) == 7:
# MAGIC         count += 1
# MAGIC print(f"System seems to have {count} local accounts.")

# COMMAND ----------

# MAGIC %lsmagic
