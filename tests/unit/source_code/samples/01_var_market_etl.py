# Databricks notebook source
# MAGIC %md
# MAGIC # Create portfolio
# MAGIC In this notebook, we will use `yfinance` to download stock data for 40 equities in an equal weighted hypothetical Latin America portfolio. We show how to use pandas UDFs to better distribute this process efficiently and store all of our output data as a Delta table.

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC For the purpose of this exercise, we create a equal weighted portfolio available in our config folder. Once we get the foundations ready, We will be able to adjust weights to minimize our risk exposure accordingly.

# COMMAND ----------

display(portfolio_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download stock data
# MAGIC We download stock market data (end of day) from yahoo finance, ensure time series are properly indexed and complete.

# COMMAND ----------

import datetime as dt

y_min_date = dt.datetime.strptime(config['yfinance']['mindate'], "%Y-%m-%d").date()
y_max_date = dt.datetime.strptime(config['yfinance']['maxdate'], "%Y-%m-%d").date()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from utils.var_utils import download_market_data

schema = StructType(
    [
        StructField('ticker', StringType(), True),
        StructField('date', TimestampType(), True),
        StructField('open', DoubleType(), True),
        StructField('high', DoubleType(), True),
        StructField('low', DoubleType(), True),
        StructField('close', DoubleType(), True),
        StructField('volume', DoubleType(), True),
    ]
)


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def download_market_data_udf(group, pdf):
    tick = group[0]
    return download_market_data(tick, y_min_date, y_max_date)


# COMMAND ----------

_ = (
    spark.createDataFrame(portfolio_df)
    .groupBy('ticker')
    .apply(download_market_data_udf)
    .write.format('delta')
    .mode('overwrite')
    .saveAsTable(config['database']['tables']['stocks'])
)

# COMMAND ----------

display(spark.read.table(config['database']['tables']['stocks']))

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks runtime come prepackaged with many python libraries such as plotly. We can represent a given instrument through a candlestick visualization

# COMMAND ----------

from pyspark.sql import functions as F

stock_df = (
    spark.read.table(config['database']['tables']['stocks'])
    .filter(F.col('ticker') == portfolio_df.iloc[0].ticker)
    .orderBy(F.asc('date'))
    .toPandas()
)

# COMMAND ----------

from utils.var_viz import plot_candlesticks

plot_candlesticks(stock_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download market factors
# MAGIC We assume that our various assets can be better described by market indicators and various indices, such as the S&P500, crude oil, treasury, or the dow. These indicators will be used later to create input features for our risk models

# COMMAND ----------

# Create a pandas dataframe where each column contain close index
market_indicators_df = pd.DataFrame()
for indicator in market_indicators.keys():
    close_df = download_market_data(indicator, y_min_date, y_max_date)['close'].copy()
    market_indicators_df[market_indicators[indicator]] = close_df

# Pandas does not keep index (date) when converted into spark dataframe
market_indicators_df['date'] = market_indicators_df.index

# COMMAND ----------

_ = (
    spark.createDataFrame(market_indicators_df)
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(config['database']['tables']['indicators'])
)

# COMMAND ----------

display(spark.read.table(config['database']['tables']['indicators']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute market volatility
# MAGIC As mentioned in the introduction, the whole concept of parametric VaR is to learn from past volatility. Instead of processing each day against its closest history sequentially, we can apply a simple window function to compute last X days' worth of market volatility at every single point in time, learning statistics behind those multi variate distributions

# COMMAND ----------

import numpy as np


def get_market_returns():

    f_ret_pdf = spark.table(config['database']['tables']['indicators']).orderBy('date').toPandas()

    # add date column as pandas index for sliding window
    f_ret_pdf.index = f_ret_pdf['date']
    f_ret_pdf = f_ret_pdf.drop(columns=['date'])

    # compute daily log returns
    f_ret_pdf = np.log(f_ret_pdf.shift(1) / f_ret_pdf)

    # add date columns
    f_ret_pdf['date'] = f_ret_pdf.index
    f_ret_pdf = f_ret_pdf.dropna()

    return spark.createDataFrame(f_ret_pdf).select(
        F.array(list(market_indicators.values())).alias('features'), F.col('date')
    )


# COMMAND ----------

# MAGIC %md
# MAGIC Instead of recursively querying our data, we can apply a window function so that each insert of our table is "joined" with last X days worth of observations. We can compute statistics of market volatility for each window using simple UDFs

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from utils.var_udf import *

days = lambda i: i * 86400
volatility_window = Window.orderBy(F.col('date').cast('long')).rangeBetween(
    -days(config['monte-carlo']['volatility']), 0
)

volatility_df = (
    get_market_returns()
    .select(F.col('date'), F.col('features'), F.collect_list('features').over(volatility_window).alias('volatility'))
    .filter(F.size('volatility') > 1)
    .select(
        F.col('date'),
        F.col('features'),
        compute_avg(F.col('volatility')).alias('vol_avg'),
        compute_cov(F.col('volatility')).alias('vol_cov'),
    )
)

# COMMAND ----------

volatility_df.write.format('delta').mode('overwrite').saveAsTable(config['database']['tables']['volatility'])

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we now have access to up to date indicators at every single point in time. For each day, we know the average of returns and our covariance matrix. These statistics will be used to generate random market conditions in our next notebook.

# COMMAND ----------

display(spark.read.table(config['database']['tables']['volatility']))
