# Databricks notebook source
# MAGIC %md
# MAGIC ### This Notebook performs full refresh of aggregate table
# MAGIC * Creates household_transactions aggregate table

# COMMAND ----------

import pyspark.sql.functions as f
from datetime import datetime, timedelta
from delta.tables import *
from utils import common
import json
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
spark.conf.set("spark.databricks.io.cache.enabled", True)

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

date_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/date_dim"
cust_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/customer_dim"
card_transactions = settings[Environment]['GoldMountPath'] + "/source/customer/agg/card_transactions"
household_transactions = settings[Environment]['GoldMountPath'] + "/source/customer/agg/household_transactions"


now = common.get_now_pst()
run_date = now.date()
#Get last saturday's date for the current run date
last_saturday_date = common.get_last_saturday(run_date)
print(f"last saturday's date {last_saturday_date} for the running date {run_date} ")

# Derive last 31 months back date based on last saturday's date.
last_31_months_date = last_saturday_date.replace(day=1) - relativedelta(months=31)
print(f" last 31 months start date: {last_31_months_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold household transactions at hhn and week_end_date level.
# MAGIC * Fetch card_transactions table from gold layer
# MAGIC * Aggregate the data at hhn and week_end_date level
# MAGIC * This table refreshes for every run

# COMMAND ----------

try:
  
  # Identify last 31 months back starting weekend date
  dateDF = spark.read.format("delta").load(date_dim).filter(f.col("date") == last_31_months_date).select("week_end_date")
  agg_start_week_end_dt = dateDF.select("week_end_date").collect()[0]['week_end_date']
  
  # Fetch card_transactions from last 31 months to as of last staturday
  CustTrnxsDF = spark.read.format("delta").load(card_transactions)\
                                          .filter((f.col("week_end_date") >= agg_start_week_end_dt) & (f.col("week_end_date") <= last_saturday_date))

  # Get HHN for all cards except card_number = '0'
  custDF = spark.read.format("delta").load(cust_dim).filter(f.col("card_number") != '0')\
                                                    .withColumn("most_recent_record", f.row_number().over(Window.partitionBy(f.col("card_number")).orderBy(f.col("eff_to_dt").desc())))\
                                                    .filter(f.col("most_recent_record") == 1)\
                                                    .select("card_number", f.col("current_household_id").alias("hhn"))
  
  hhnTxnsDF = CustTrnxsDF.join(custDF, "card_number", "inner")\
                         .select(CustTrnxsDF["*"], custDF.hhn)\
                         .groupBy("hhn", "week_end_date")\
                          .agg(f.max("sof_instore_current_4wk_flag").alias("sof_instore_current_4wk_flag")
                               , f.max("sof_instore_current_13wk_flag").alias("sof_instore_current_13wk_flag")
                               , f.max("sof_instore_prior_13wk_flag").alias("sof_instore_prior_13wk_flag")
                               , f.max("sof_ecomm_current_4wk_flag").alias("sof_ecomm_current_4wk_flag")
                               , f.max("sof_ecomm_current_13wk_flag").alias("sof_ecomm_current_13wk_flag")
                               , f.max("sof_ecomm_prior_13wk_flag").alias("sof_ecomm_prior_13wk_flag")
                               , f.max("blf_instore_current_4wk_flag").alias("blf_instore_current_4wk_flag")
                               , f.max("blf_instore_current_13wk_flag").alias("blf_instore_current_13wk_flag")
                               , f.max("blf_instore_prior_13wk_flag").alias("blf_instore_prior_13wk_flag")
                               , f.max("blf_ecomm_current_4wk_flag").alias("blf_ecomm_current_4wk_flag")
                               , f.max("blf_ecomm_current_13wk_flag").alias("blf_ecomm_current_13wk_flag")
                               , f.max("blf_ecomm_prior_13wk_flag").alias("blf_ecomm_prior_13wk_flag")
                              )\
                          .withColumn("dl_load_dt", f.lit(now))\
                          .select("hhn", "week_end_date", "sof_instore_current_4wk_flag", "sof_instore_current_13wk_flag", "sof_instore_prior_13wk_flag"
                                                        , "sof_ecomm_current_4wk_flag", "sof_ecomm_current_13wk_flag", "sof_ecomm_prior_13wk_flag", "blf_instore_current_4wk_flag"
                                                        , "blf_instore_current_13wk_flag", "blf_instore_prior_13wk_flag", "blf_ecomm_current_4wk_flag", "blf_ecomm_current_13wk_flag"
                                                        , "blf_ecomm_prior_13wk_flag", "dl_load_dt")
  
  # Load data to target table
  hhnTxnsDF.write.format("delta")\
                     .mode("overwrite")\
                     .option("overwriteSchema", "true")\
                     .partitionBy("week_end_date")\
                     .save(household_transactions)
                                      
  
except Exception as ex:
  raise str(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 168 HOURS

# COMMAND ----------

common.vacuum_delta_table(spark, household_transactions)

# COMMAND ----------

# DBTITLE 1,Create hive_metastore table
common.create_hive_metastore_table(spark, database = 'customer', table = 'household_transactions', location = household_transactions)
