from functools import lru_cache


@lru_cache(maxsize=10_000)
def get_dbutils():
    from databricks.sdk.runtime import dbutils

    return dbutils
