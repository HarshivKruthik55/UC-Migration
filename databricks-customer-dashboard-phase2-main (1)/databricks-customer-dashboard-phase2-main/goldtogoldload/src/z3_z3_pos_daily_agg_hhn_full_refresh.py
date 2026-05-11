# Databricks notebook source
# MAGIC %md
# MAGIC ### This Notebook performs full refresh of aggregate table
# MAGIC * Creates pos_daily_agg_hhn aggregate table

# COMMAND ----------

import pyspark.sql.functions as f
from datetime import datetime, timedelta
from delta.tables import *
from utils import common
from pyspark.sql.window import Window
import json
from dateutil.relativedelta import relativedelta

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# COMMAND ----------

dbutils.widgets.text("Environment", "", "")
Environment = dbutils.widgets.get("Environment").upper()

# COMMAND ----------

if not Environment:
  raise Exception("Environment - Mandatory parameter is not passed")
  
if Environment != 'DEV' and Environment != 'QA' and Environment != 'PROD':
  raise Exception(f"Invalid Environment : {Environment}. Valid values are DEV or QA or PROD")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set Mount Path
# MAGIC * Initiate to set Silver and Gold destination ADLS paths

# COMMAND ----------

config = open("../../configs/config.json")
settings = json.load(config)


customer_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/customer_dim"
pos_daily_agg_card = settings[Environment]['GoldMountPath'] + "/source/sales/agg/pos_daily_agg_card"
pos_daily_agg_hhn = settings[Environment]['GoldMountPath'] + "/source/sales/agg/pos_daily_agg_hhn"

now = common.get_now_pst()

#Get last saturday's date for the current run date
run_date = now.date()
last_saturday_date = common.get_last_saturday(run_date)
print(f"last saturday's date {last_saturday_date} for the running date {run_date} ")

#Get the first date of the month after going back 31 months
first_day_of_month_31_month_past = run_date.replace(day=1) - relativedelta(months=31)
print(f"First day of the month after going back 31 months from {run_date} is {first_day_of_month_31_month_past}")

#As week starts from sunday we need to get the last sunday of "first date of the month after going back 31 months"
start_sunday = common.get_last_sunday(first_day_of_month_31_month_past)
print(f"Last sundays's date is {start_sunday} for the running date {first_day_of_month_31_month_past} ")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold POS Daily Aggregate at HHN Level Table
# MAGIC * Fetch pos daily aggregate card level data from gold layer for the current month and back to the last 31 months.
# MAGIC * Perform Aggregate at HHN and Daily Level
# MAGIC * This table refreshes on every run

# COMMAND ----------

try:
  # Fetch pos daily aggregate card level data from gold layer for the current month and back to the last 31 months.
  posDailyAggCardDF = spark.read.format("delta").load(pos_daily_agg_card)\
                            .filter((f.col("fiscal_date") >= start_sunday) & (f.col("fiscal_date") <= last_saturday_date))
                                    
  customerDF = spark.read.format("delta").load(customer_dim).select("customer_hk", "current_card_type", "current_household_id")
  
  # Build aggregate table at HHN level
  posDailyAggHHNDF = posDailyAggCardDF.join(customerDF, posDailyAggCardDF.customer_hk == customerDF.customer_hk, "inner")\
                                                 .select(posDailyAggCardDF["*"], customerDF.current_household_id.alias("hhn"))\
                                                 .groupBy("fiscal_date", "hhn", "location_hk", "channel")\
                                                 .agg(f.sum("transaction_count").alias("transaction_count")
                                                    , f.sum("sales").alias("sales")
                                                    , f.sum("scan").alias("scan")
                                                    , f.sum("units").alias("units")
                                                    , f.sum("items").alias("items")
                                                    )\
                                                 .withColumn("dl_load_dt", f.lit(now))\
                                                 .select("fiscal_date", "hhn", "location_hk", "channel", "transaction_count", "sales", "scan", "units", "items", "dl_load_dt")
  
  # Load data to target table
  posDailyAggHHNDF.write.mode("overwrite")\
                    .format("delta")\
                    .save(pos_daily_agg_hhn)
  
except Exception as ex:
  raise str(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 0 HOURS
# MAGIC * Maintain one snapshot data for this table to support External tables in Azure Synapse.

# COMMAND ----------

DeltaTable = DeltaTable.forPath(spark, pos_daily_agg_hhn)
DeltaTable.vacuum(0)

# COMMAND ----------

# DBTITLE 1,Create hive_metastore table
common.create_hive_metastore_table(spark, database = 'sales', table = 'pos_daily_agg_hhn', location = pos_daily_agg_hhn)
