# Databricks notebook source
# MAGIC %md
# MAGIC ### This Notebook performs full refresh of aggregate table
# MAGIC * Creates pos_daily_agg_card aggregate table

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

retail_sales_fact = settings[Environment]['GoldMountPath'] + "/source/sales/fact/retail_sale_fact"
location_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/location_dim"
item_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/item_dim"
register_dim = settings[Environment]['GoldMountPath'] + "/source/sales/dim/register_dim"
pos_daily_agg_card = settings[Environment]['GoldMountPath'] + "/source/sales/agg/pos_daily_agg_card"
sofBannerList = settings["sofBannerList"]

now = common.get_now_pst()


run_date = now.date()
#Get last saturday's date for the current run date
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
# MAGIC ### Gold POS Daily Aggregate at Card Level Table
# MAGIC * Fetch POS Transactions from gold layer retail_sale
# MAGIC * Consider Financial transactions only
# MAGIC * Consider transactions for 'SOF', 'UF' and 'PSF' banners only
# MAGIC * Aggregate data at Card, location and Daily level
# MAGIC * This table refreshes on every run

# COMMAND ----------

try:

  locationDF = spark.read.format("delta").load(location_dim)\
                                         .filter(f.col("current_banner_short_name").isin(sofBannerList))\
                                         .select("location_hk")
  itemDF = spark.read.format("delta").load(item_dim).filter(f.col("current_financial_sale_flag") == 'Y').select("item_hk") 
  registerDF = spark.read.format("delta").load(register_dim).select("register_hk","id")

  # Fetch retail_sale fact data from gold layer for the current month and back to the last 31 months.
  retailSalesDF = spark.read.format("delta").load(retail_sales_fact)\
                          .filter((f.col("fiscal_date") >= start_sunday) & (f.col("fiscal_date") <= last_saturday_date))\
                          .select("fiscal_date", "location_hk", "customer_hk", "item_hk", "register_hk", "ecomm_flag"
                                  , "transaction_number", "merch_sales", "merch_scan_margin", "unit_count", "item_count")

  # Aggregate data at Card, location and daily level
  posDailyAggCardDF = retailSalesDF.alias("r").join(locationDF.alias("l"), f.col("r.location_hk") == f.col("l.location_hk"), "inner")\
                                             .join(itemDF.alias("i"), f.col("r.item_hk") == f.col("i.item_hk"), "inner")\
                                             .join(registerDF.alias("re"), f.col("r.register_hk") == f.col("re.register_hk"), "left")\
                                             .select("r.fiscal_date", "r.location_hk", "r.customer_hk", "r.merch_sales"
                                                     , "r.merch_scan_margin", "r.unit_count", "r.item_count", "r.transaction_number"
                                                     , f.when(f.col("r.ecomm_flag") == "Non-Ecomm Sale", f.lit("In Store"))
                                                       .when((f.col("r.ecomm_flag") == "Ecomm Sale") & (f.substring("re.ID", -2, 2) == "99")
                                                             , f.lit("Delivery"))
                                                       .when((f.col("r.ecomm_flag") == "Ecomm Sale") & (f.substring("re.ID", -2, 2) == "97")
                                                             , f.lit("Pickup"))
                                                       .otherwise(f.lit("Adjustment"))
                                                       .alias("channel"))\
                                            .groupBy("fiscal_date", "customer_hk", "location_hk", "channel")\
                                            .agg(f.countDistinct(f.col("transaction_number")).alias("transaction_count")
                                                 , f.sum("merch_sales").alias("sales")
                                                 , f.sum(f.coalesce(f.col("merch_scan_margin"), f.lit(0))).alias("scan")
                                                 , f.sum("unit_count").alias("units")
                                                 , f.sum("item_count").alias("items")
                                                 )\
                                            .withColumn("dl_load_dt", f.lit(now))\
                                            .withColumn("dl_update_dt", f.lit(now))\
                                            .select("fiscal_date", "customer_hk", "location_hk", "channel", "transaction_count", "sales"
                                                         , "scan", "units", "items", "dl_load_dt", "dl_update_dt")


  # Load data to target table
  posDailyAggCardDF.write.mode("overwrite")\
                   .format("delta")\
                   .partitionBy("fiscal_date")\
                   .save(pos_daily_agg_card)
  
except Exception as ex:
  raise str(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 168 HOURS

# COMMAND ----------

common.vacuum_delta_table(spark, pos_daily_agg_card)

# COMMAND ----------

# DBTITLE 1,Create hive_metastore table
common.create_hive_metastore_table(spark, database = 'sales', table = 'pos_daily_agg_card', location = pos_daily_agg_card)
