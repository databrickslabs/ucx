# Databricks notebook source
import datetime
# Updated List of Notebooks
path = './leaf2.py'
notebooks_list = [
                  './leaf1.py',
                  f"{path}",
                  './leaf3.py',
                  ]
# Execution:
for notebook in notebooks_list:
  try:
    start_time = datetime.datetime.now()
    print("Running the report of " + str(notebook).split('/')[len(str(notebook).split('/'))-1])
    status = dbutils.notebook.run(notebook,100000)
    end_time = datetime.datetime.now()
    print("Finished, time taken: " + str(start_time-end_time))
  except:
    print("The notebook {0} failed to run".format(notebook))
