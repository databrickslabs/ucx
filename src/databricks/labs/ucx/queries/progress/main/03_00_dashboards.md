# Dashboards

This section shows Unity Catalog compatability issues found while linting dashboards. There are two kinds of changes to
perform:
- Data asset reference, i.e. references to Hive metastore tables and views or direct filesystem access (dfsa), these
  references should be updated to refer to their Unity Catalog counterparts.
- Linting compatability issues, e.g. using RDDs or directly accessing the Spark context, these issues should be resolved
  by following the instructions stated with the issue.
