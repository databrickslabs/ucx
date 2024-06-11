# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install \
# MAGIC    distribution/dist/thingy-0.0.1-py2.py3-none-any.whl \
# MAGIC    -t/non/existing/path \
# MAGIC    --no-deps \
# MAGIC    --force-reinstall \
# MAGIC    --require-virtualenv \
# MAGIC    --debug \
# MAGIC    --no-cache-dir \
# MAGIC    --no-color

# COMMAND ----------

import thingy
