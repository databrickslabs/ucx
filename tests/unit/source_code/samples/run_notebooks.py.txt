import datetime
# Updated List of Notebooks
notebooks_list = [
                  '/Production/data_solutions/accounts/companyA_10011111/de/companyA_de_report',
                  '/Production/data_solutions/accounts/companyB_10022222/de/companyB_de_report',
                  '/Production/data_solutions/accounts/companyC_10033333/de/companyC_de_report',
                  '/Production/data_solutions/accounts/companyD_10044444/de/companyD_de_report',  
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
