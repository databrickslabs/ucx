path = dbutils.widgets.get("my-widget")
dbutils.notebook.run(path)
path = dbutils.widgets.get("no-widget")
# ucx[notebook-run-cannot-compute-value:+1:0:+1:26] Path for 'dbutils.notebook.run' cannot be computed and requires adjusting the notebook path(s)
dbutils.notebook.run(path)
values = dbutils.widgets.getAll()
path = values["my-widget"]
dbutils.notebook.run(path)
path = values["no-widget"]
# ucx[notebook-run-cannot-compute-value:+1:0:+1:26] Path for 'dbutils.notebook.run' cannot be computed and requires adjusting the notebook path(s)
dbutils.notebook.run(path)
