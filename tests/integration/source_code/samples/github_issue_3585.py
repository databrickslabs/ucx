# Databricks notebook source
import os
import datetime
import time
import logging
import re
import pandas as pd
import pyspark
from pyspark.sql.functions import max
from datetime import datetime, timedelta, date, timezone
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import max   # problems from jvm access
from os import path
from pytz import timezone

# email_sender = email_receiver = email_sender_name = 'xxx'
email_sender = email_receiver = email_sender_name = 'xxx'

# COMMAND ----------

# current date and time
now = datetime.now()

# Init Job Monitor Vars
job_id = 1
job_start_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
job_rows = 0
job_name = 'DBKS_DATALAKE_TO_MONGO_PUSH'

# COMMAND ----------

# MAGIC %run /UTILITIES/COMMON_UTILS

# COMMAND ----------

dbutils.widgets.text("MONGO_PORT", "xxx")
dbutils.widgets.text("dbname", "xxx")
dbutils.widgets.text("collection", "xxx")
data_logpath = dbutils.widgets.get("logpath")

# COMMAND ----------

central = timezone('US/Central')
current_time = datetime.now()
current_date = current_time.astimezone(central)
log_folder_path = data_logpath + str(current_date.year) + '/' + str(current_date.month) + '/'
if not os.path.exists(log_folder_path):
    dbutils.fs.mkdirs(log_folder_path)
log_dt = datetime.today()
cst_time = log_dt.astimezone(central)
log_dt = cst_time.strftime("%Y_%m_%d_%H%M%S")
log_base_path = '/dbfs' + log_folder_path
log_file = log_base_path + "activity_log" + str(log_dt) + '.txt'
log_name = "Data_load"
logger = setup_logging(log_file, log_name, "INFO", "file_and_stream")
log_string = ''
log_string += "Job process execution started at :: {0}".format(current_date) + "<br>" + "<br>"

# COMMAND ----------

ENV = dbutils.widgets.get("ENV")
MONGO_PORT = dbutils.widgets.get("MONGO_PORT")
dbname = dbutils.widgets.get("dbname")
collection = dbutils.widgets.get("collection")

if ENV == 'DEV':
    username = dbutils.secrets.get(scope="key-vault-secrets", key="xxx")
    password = dbutils.secrets.get(scope="key-vault-secrets", key="xxx")
    hostname = dbutils.secrets.get(scope="key-vault-secrets", key="xxx")
elif ENV == 'PROD':
    username = dbutils.secrets.get(scope="key-vault-secrets", key="xxx")
    password = dbutils.secrets.get(scope="key-vault-secrets", key="xxx")
    hostname = dbutils.secrets.get(scope="key-vault-secrets", key="xxx")
else:
    raise Exception("config error: ENV is not defined correctly")

logger.info("Environment - {}".format(ENV))

# COMMAND ----------

logger.info("Reading the data from checkpoint_staging_tranx table")
df_staging_tranx = spark.sql("""select * from digital.checkpoint_staging_tranx""")
logger.info("Getting the checkpoint date")
if df_staging_tranx.count() == 0:
    checkpoint_staging_date = '2023-01-28T20:04:40.000+0000'
else:
    checkpoint_staging_date = df_staging_tranx.agg({'MAX_LOAD_DATETIME': 'max'}).collect()[0][0]
logger.info("Checkpoint date is {date}".format(date=checkpoint_staging_date))


# COMMAND ----------

def get_staging_data():
    '''
    method to get the staging data
    '''
    try:
        logger.info("Creating dataframe from staging table")
        checkpoint_staging_data = spark.sql(
            """select * from (select * from (select *, row_number() over(partition by transactionId,beginDateTime,salesQuantity,totalAmount,storeId, payment.accountId,item.fuelgradeid order by loadDate) as rw from digital.fpis_discount_partner_transactions) where rw=1) where loadDate>'{}' order by loadDate""".format(
                checkpoint_staging_date))
        logger.info("dataframe created on the staging table with load date as :: {0} and count ::{1}".format(
            checkpoint_staging_date, checkpoint_staging_data.count()))
        return checkpoint_staging_data
    except Exception as e:
        raise e


# COMMAND ----------

def write_mongo():
    global log_string
    global status
    global job_rows
    try:
        try:
            staging_data = get_staging_data()
            staging_count = staging_data.count()
            if staging_count != 0:
                staging_data = staging_data.withColumn("creationDateTime", staging_data.loadDate)
                temp_df = staging_data.drop('updatedDate', 'loadDate', 'posType', 'rw', 'transactionPk', 'loadType',
                                            'uniqueRecords', 'year', 'month', 'date')
                finaldf_to_push_mongo = temp_df.dropDuplicates()
                finaldf_to_push_mongo = finaldf_to_push_mongo.withColumn("updateDateTime", current_timestamp())
                display(finaldf_to_push_mongo)
                finaldf_count = finaldf_to_push_mongo.count()
                logger.info("staging data count :: {count}".format(count=staging_count))
                logger.info("loading data to the mongo started with the count :: {count}".format(count=finaldf_count))
                mongourl = "mongodb+srv://" + username + ":" + password + "@" + hostname
                finaldf_to_push_mongo.write.format("mongodb") \
                    .option("spark.mongodb.write.connection.uri", mongourl) \
                    .option("database", dbname) \
                    .option("replicaSet", "globaldb") \
                    .option("authSource", "admin") \
                    .option("port", MONGO_PORT) \
                    .option("maxBatchSize", 512) \
                    .option("collection", collection) \
                    .mode("append").save()
                logger.info("updating the checkpoint date")
                checkpoint_date_updated = staging_data.agg({'loadDate': 'max'}).collect()[0][0]
                df_checkpoint = spark.sql(
                    """select '{0}' as MAX_LOAD_DATETIME, {1} as RECORD_COUNT""".format(checkpoint_date_updated,
                                                                                        finaldf_count))
                df_checkpoint.write.saveAsTable('digital.checkpoint_staging_tranx', mode='append', format='delta')
                logger.info(
                    "loading data to the mongo completed and table checkpoint_staging_tranx updated with max load date as :: {0} and count :: {1}".format(
                        checkpoint_date_updated, finaldf_count))
                log_string += "loading data to the mongo completed and table checkpoint_staging_tranx updated with max load date as :: {0} and count :: {1}".format(
                    checkpoint_date_updated, finaldf_count) + "<br>" + "<br>"
                status = 'success'
                job_rows = finaldf_count
            else:
                print("checkpoint_staging_date", checkpoint_staging_date)
                df_checkpoint = spark.sql(
                    """select '{0}' as MAX_LOAD_DATETIME, {1} as RECORD_COUNT""".format(checkpoint_staging_date,
                                                                                        staging_count))
                df_checkpoint.write.saveAsTable('digital.checkpoint_staging_tranx', mode='append', format='delta')
                log_msg = "There is no data found and table checkpoint_staging_tranx updated with max load date as :: {0} and count :: {1}".format(
                    checkpoint_staging_date, staging_count)
                logger.info(log_msg)
                log_string += log_msg + "<br>" + "<br>"
                status = 'success'
        except Exception as error:
            status = 'failed'
            logger.error("Something went wrong while loading data to the mongo")

            client = {"xxx": "database"}
            db = client[dbname]
            collection_data = db[collection]
            logger.error("clearing the data from the mongo from the date: {}".format(checkpoint_staging_date))
            datetime_object = datetime.strptime(checkpoint_staging_date, '%Y-%m-%d %H:%M:%S.%f')
            query_data = {'creationDateTime': {"$gt": datetime_object}}
            collection_data.delete_many(query_data)
            logger.error("data cleared from the mongo from the date: {}".format(checkpoint_staging_date))

            logger.error("Exception while loading data to the mongo: {e}".format(e=error))
            raise "Exception in writing datalake to mongodb got failed"
        finally:
            logger.info("process completed")

    except Exception as e:
        status = 'failed'
        logger.error("Exception while loading data to the mongo: {e}".format(e=e))
        log_string += "Exception while loading data to the mongo: {e}".format(e=e) + "<br>" + "<br>"
        raise "writing datalake to mongodb got failed"


if __name__ == '__main__':
    write_mongo()
    log_string += "Job process execution completed at :: {0}".format(
        datetime.now().astimezone(central)) + "<br>" + "<br>"
    log_string += "Total Time taken to complete the process is - {0} ".format(
        datetime.now().astimezone(central) - current_date) + "<br>"
    if status == 'success':
        sendmail(sender_email=email_sender,
                 receiver=email_receiver,
                 sender_name=email_sender_name,
                 subject='{status}-staging Log File'.format(status='success'),
                 body="Sucessfully loaded the staging data to mongo table. <br><br> {0} <br> Check the log files /mnt/BATCH_RESULTS/FUEL_PRICING/STAGING_TRANX_LOGS".format(
                     log_string))
    else:
        sendmail(sender_email=email_sender,
                 receiver=email_receiver,
                 sender_name=email_sender_name,
                 subject='{status}-staging Log File'.format(status='failed'),
                 body="Failed loading the staging data to mongo table. <br> {0} <br> Check the log files /mnt/BATCH_RESULTS/FUEL_PRICING/STAGING_TRANX_LOGS".format(
                     log_string))

    # Update Job Monitor tables
    insert_sql = """\
  insert into digital.fps_ops_DL_monitor_history
  (JobID, JobName, StartDateTime, EndDateTime, Status, RowsUpdated, insert_src, insert_dttm, update_src, update_dttm)
  values
  (""" + format(job_id) + """,'""" + job_name + """',from_utc_timestamp('""" + format(
        job_start_time) + """', 'US/Central'), from_utc_timestamp(current_timestamp(), 'US/Central'),'""" + format(
        status) + """',""" + format(job_rows) + """,'""" + format(
        job_id) + """', from_utc_timestamp(current_timestamp(), 'US/Central'),'""" + format(
        job_id) + """', from_utc_timestamp(current_timestamp(), 'US/Central'))"""

    sql_df = spark.sql(insert_sql)
