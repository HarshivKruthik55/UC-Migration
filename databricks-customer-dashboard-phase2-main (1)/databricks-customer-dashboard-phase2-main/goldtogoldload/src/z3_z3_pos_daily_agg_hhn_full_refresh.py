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

# Unity Catalog three-level namespace
catalog_name = settings[Environment]['catalog_name']
MASTER_DIM_GOLD_SCHEMA = settings['MASTER_DIM_GOLD_SCHEMA']
SALES_AGG_GOLD_SCHEMA = settings['SALES_AGG_GOLD_SCHEMA']

# Source and target table names
SOURCE_TABLE_NAME_1 = "customer_dim"
SOURCE_TABLE_NAME_2 = "pos_daily_agg_card"
TABLE_NAME = "pos_daily_agg_hhn"

# Target table path
pos_daily_agg_hhn_path = settings[Environment]['GoldMountPath'] + f"/source/sales/agg/{TABLE_NAME}"

# Source table UC references
customer_dim = f"{catalog_name}.{MASTER_DIM_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_1}"
pos_daily_agg_card = f"{catalog_name}.{SALES_AGG_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_2}"

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
  posDailyAggCardDF = spark.table(pos_daily_agg_card)\
                            .filter((f.col("fiscal_date") >= start_sunday) & (f.col("fiscal_date") <= last_saturday_date))
                                    
  customerDF = spark.table(customer_dim).select("customer_hk", "current_card_type", "current_household_id")
  
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
  
  # Create UC table with DDL
  pos_daily_agg_hhn_table = f"{catalog_name}.{SALES_AGG_GOLD_SCHEMA}.{TABLE_NAME}"
  spark.sql(f"""
    CREATE OR REPLACE TABLE {pos_daily_agg_hhn_table} (
    fiscal_date DATE COMMENT 'Transaction fiscal date',
    hhn DECIMAL(12,0) COMMENT 'Unique identifier for each household',
    location_hk STRING COMMENT 'Location hash key identifier',
    channel STRING COMMENT 'Sales channel (In Store, Delivery, Pickup, Adjustment)',
    transaction_count INT COMMENT 'Number of distinct transactions',
    sales DECIMAL(18,2) COMMENT 'Total merchandise sales amount',
    scan DECIMAL(18,2) COMMENT 'Total merchandise scan margin',
    units DECIMAL(18,2) COMMENT 'Total unit count',
    items DECIMAL(18,2) COMMENT 'Total item count',
    dl_load_dt TIMESTAMP COMMENT 'Timestamp of when the data was loaded into the table'
    )
    USING delta
    LOCATION '{pos_daily_agg_hhn_path}'
    """)

  # Saving the table
  posDailyAggHHNDF.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(pos_daily_agg_hhn_table)
  
except Exception as ex:
  raise str(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 0 HOURS
# MAGIC * Maintain one snapshot data for this table to support External tables in Azure Synapse.

# COMMAND ----------

DeltaTable = DeltaTable.forPath(spark, pos_daily_agg_hhn_path)
DeltaTable.vacuum(0)
