# Databricks notebook source
# MAGIC %md
# MAGIC ### This Notebook performs incremental load of aggregate table
# MAGIC * Creates card_transactions aggregate table

# COMMAND ----------

import pyspark.sql.functions as f
from datetime import datetime, timedelta
from delta.tables import *
from utils import common
import json
from dateutil.relativedelta import relativedelta

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)
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

# Unity Catalog three-level namespace
catalog_name = settings[Environment]['catalog_name']
CUSTOMER_AGG_GOLD_SCHEMA = settings['CUSTOMER_AGG_GOLD_SCHEMA']

# Source table names
SOURCE_TABLE_NAME_1 = "retail_sale_fact"
SOURCE_TABLE_NAME_2 = "location_dim"
SOURCE_TABLE_NAME_3 = "item_dim"
SOURCE_TABLE_NAME_4 = "date_dim"
SOURCE_TABLE_NAME_5 = "customer_dim"
TABLE_NAME = "card_transactions"

# Target table path
card_transactions_path = settings[Environment]['GoldMountPath'] + f"/source/customer/agg/{TABLE_NAME}"

# Source table UC references
retail_sales_fact = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_1}"
location_dim = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_2}"
item_dim = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_3}"
date_dim = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_4}"
cust_dim = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_5}"

sofBannerList = settings["sofBannerList"]
blfBannerList = settings["blfBannerList"]
locationTypeList = settings["locationTypeList"]


now = common.get_now_pst()


run_date = now.date()
#Get the last sunday's date if the run date is weekday
last_sunday_date = common.get_last_sunday(run_date)
print(f"last sunday's date {last_sunday_date} for the running date {run_date} ")

#Get last saturday's date for the current run date
last_saturday_date = common.get_last_saturday(run_date)
print(f"last saturday's date {last_saturday_date} for the running date {run_date} ")

# Derive last 31 months back date based on last saturday's date.
last_31_months_date = last_saturday_date.replace(day=1) - relativedelta(months=31)
print(f" last 31 months start date: {last_31_months_date}")


# COMMAND ----------

# DBTITLE 1,Function to returns the transactions flag values for Save On Foods and Buy Low Foods banners
def get_flag_value(distCardWeekDF, srcDF, banner_list, ecomm_flag, filter_start_date_column, filter_end_dt_column, tgt_column):
  # Derive flag value based on passing parameter values
  flagDF = distCardWeekDF.alias("d").join(srcDF.alias("s"), (f.col("d.card_number") == f.col("s.card_number")) 
                                      & (f.col("s.current_banner_short_name").isin(banner_list))
                                      & (f.col("s.ecomm_flag") == ecomm_flag)
                                      & (f.col("s.fiscal_date") >= f.col(f"d.{filter_start_date_column}")) 
                                      & (f.col("s.fiscal_date") <= f.col(f"d.{filter_end_dt_column}")), "leftsemi")\
                                .select("d.card_number", "d.week_end_date")\
                                .withColumn(tgt_column, f.lit(1))\
                                .groupBy("card_number", "week_end_date")\
                                .agg(f.max(tgt_column).alias(tgt_column))
  return flagDF
  

# COMMAND ----------

# DBTITLE 1,Function to returns the aggregate dataset at Card and week level
def cust_transactions_agg(retailSalesDF, locationDF, itemDF, custDF, dateDF, agg_start_week_end_dt, last_saturday_date):
  # agg_start_week_end_dt uses to identify starting week_end_dt for the aggregations.
  # Perform joins with Dim tables and get the respective dim columns
  # Fetch finanicial transactions only for save on foods and buy low foods banners.
  srcDF = retailSalesDF.alias("r").join(locationDF.alias("l"), f.col("r.location_hk") == f.col("l.location_hk"), "inner")\
                                               .join(itemDF.alias("i"), f.col("r.item_hk") == f.col("i.item_hk"), "inner")\
                                               .join(custDF.alias("c"), f.col("r.customer_hk") == f.col("c.customer_hk"), "inner")\
                                               .join(dateDF.alias("d"), f.col("r.fiscal_date") == f.col("d.date"), "inner")\
                                               .select("r.fiscal_date", "r.ecomm_flag", "c.card_number", "l.current_banner_short_name", "d.date", "d.week_end_date", "d.current_4wk_start_dt"
                                                       , "d.current_13wk_start_dt", "d.prior_13wk_start_dt", "d.prior_13wk_end_dt", "d.future_26wk_end_dt")
  
  # f.col("week_end_date") >= agg_start_week_end_dt condition to identify starting week_end_dt for the aggregations.
  # Get unique records for the combination of card_number and week_end_date
  distCardWeekDF = srcDF.filter(f.col("week_end_date") >= agg_start_week_end_dt)\
                              .select("card_number", "week_end_date", "current_4wk_start_dt", "current_13wk_start_dt"
                                      , "prior_13wk_start_dt", "prior_13wk_end_dt", "future_26wk_end_dt").distinct()
  
  # Get rolling 26 weeks week_end_dates for each processing week_end_date
  dateWkEndDtDF = dateDF.select("week_end_date", "current_4wk_start_dt", "current_13wk_start_dt", "prior_13wk_start_dt", "prior_13wk_end_dt").distinct()
  distCardWeekDF = distCardWeekDF.alias("d").join(dateWkEndDtDF.alias("w"), (f.col("w.week_end_date") >= f.col("d.week_end_date")) 
                                                  & (f.col("w.week_end_date") <= f.col("d.future_26wk_end_dt")), "inner")\
                                            .select("d.card_number", "w.week_end_date", "w.current_4wk_start_dt", "w.current_13wk_start_dt"
                                                    , "w.prior_13wk_start_dt", "w.prior_13wk_end_dt")\
                                            .filter(f.col("week_end_date") <= last_saturday_date)
  distCardWeekDF.persist()
  distCardWeekDF.count()
  
  # Derive save on foods instore current 4 weeks flag.
  sofInstoreCurr4WkFlagDF = get_flag_value(distCardWeekDF, srcDF, sofBannerList, 'Non-Ecomm Sale', 'current_4wk_start_dt', 'week_end_date', 'sof_instore_current_4wk_flag')
  # Derive save on foods ecomm current 4 weeks flag.
  sofEcommCurr4WkFlagDF = get_flag_value(distCardWeekDF, srcDF, sofBannerList, 'Ecomm Sale', 'current_4wk_start_dt', 'week_end_date', 'sof_ecomm_current_4wk_flag')
  # Derive buy low foods instore current 4 weeks flag.
  blfInstoreCurr4WkFlagDF = get_flag_value(distCardWeekDF, srcDF, blfBannerList, 'Non-Ecomm Sale', 'current_4wk_start_dt', 'week_end_date', 'blf_instore_current_4wk_flag')
  # Derive buy low foods ecomm current 4 weeks flag.
  blfEcommCurr4WkFlagDF = get_flag_value(distCardWeekDF, srcDF, blfBannerList, 'Ecomm Sale', 'current_4wk_start_dt', 'week_end_date', 'blf_ecomm_current_4wk_flag')
  # Derive save on foods instore current 13 weeks flag.
  sofInstoreCurr13WkFlagDF = get_flag_value(distCardWeekDF, srcDF, sofBannerList, 'Non-Ecomm Sale', 'current_13Wk_start_dt', 'week_end_date', 'sof_instore_current_13wk_flag')
  # Derive save on foods ecomm current 13 weeks flag.
  sofEcommCurr13WkFlagDF = get_flag_value(distCardWeekDF, srcDF, sofBannerList, 'Ecomm Sale', 'current_13Wk_start_dt', 'week_end_date', 'sof_ecomm_current_13wk_flag')
  # Derive buy low foods instore current 13 weeks flag.
  blfInstoreCurr13WkFlagDF = get_flag_value(distCardWeekDF, srcDF, blfBannerList, 'Non-Ecomm Sale', 'current_13Wk_start_dt', 'week_end_date', 'blf_instore_current_13wk_flag')
  # Derive buy low foods ecomm current 13 weeks flag.
  blfEcommCurr13WkFlagDF = get_flag_value(distCardWeekDF, srcDF, blfBannerList, 'Ecomm Sale', 'current_13Wk_start_dt', 'week_end_date', 'blf_ecomm_current_13wk_flag')
  # Derive save on foods instore prior 13 weeks flag.
  sofInstorePrior13WkFlagDF = get_flag_value(distCardWeekDF, srcDF, sofBannerList, 'Non-Ecomm Sale', 'Prior_13Wk_start_dt', 'Prior_13Wk_end_dt', 'sof_instore_prior_13wk_flag')
  # Derive save on foods ecomm prior 13 weeks flag.
  sofEcommPrior13WkFlagDF = get_flag_value(distCardWeekDF, srcDF, sofBannerList, 'Ecomm Sale', 'Prior_13Wk_start_dt', 'Prior_13Wk_end_dt', 'sof_ecomm_prior_13wk_flag')
  # Derive buy low foods instore prior 13 weeks flag.
  blfInstorePrior13WkFlagDF = get_flag_value(distCardWeekDF, srcDF, blfBannerList, 'Non-Ecomm Sale', 'Prior_13Wk_start_dt', 'Prior_13Wk_end_dt', 'blf_instore_prior_13wk_flag')
  # Derive buy low foods ecomm prior 13 weeks flag.
  blfEcommPrior13WkFlagDF = get_flag_value(distCardWeekDF, srcDF, blfBannerList, 'Ecomm Sale', 'Prior_13Wk_start_dt', 'Prior_13Wk_end_dt', 'blf_ecomm_prior_13wk_flag')
                                            
  # Merge current 4 weeks , 13 weeks and prior 13 weeks flags column at card and week_end_date level.
  mergeDF = distCardWeekDF.join(sofInstoreCurr4WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(sofEcommCurr4WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(blfInstoreCurr4WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(blfEcommCurr4WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(sofInstoreCurr13WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(sofEcommCurr13WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(blfInstoreCurr13WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(blfEcommCurr13WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(sofInstorePrior13WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(sofEcommPrior13WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(blfInstorePrior13WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .join(blfEcommPrior13WkFlagDF, ["card_number", "week_end_date"], "left")\
                          .select(distCardWeekDF.card_number, distCardWeekDF.week_end_date
                                 , sofInstoreCurr4WkFlagDF.sof_instore_current_4wk_flag
                                 , sofEcommCurr4WkFlagDF.sof_ecomm_current_4wk_flag
                                 , blfInstoreCurr4WkFlagDF.blf_instore_current_4wk_flag
                                 , blfEcommCurr4WkFlagDF.blf_ecomm_current_4wk_flag
                                 , sofInstoreCurr13WkFlagDF.sof_instore_current_13wk_flag
                                 , sofEcommCurr13WkFlagDF.sof_ecomm_current_13wk_flag
                                 , blfInstoreCurr13WkFlagDF.blf_instore_current_13wk_flag
                                 , blfEcommCurr13WkFlagDF.blf_ecomm_current_13wk_flag
                                 , sofInstorePrior13WkFlagDF.sof_instore_prior_13wk_flag
                                 , sofEcommPrior13WkFlagDF.sof_ecomm_prior_13wk_flag
                                 , blfInstorePrior13WkFlagDF.blf_instore_prior_13wk_flag
                                 , blfEcommPrior13WkFlagDF.blf_ecomm_prior_13wk_flag)
                                            
  # Default the flag values to zero if any flag column contains null.                                          
  custTxnAggDF = mergeDF.withColumn("sof_instore_current_4wk_flag", f.coalesce(f.col("sof_instore_current_4wk_flag"), f.lit(0)))\
                        .withColumn("sof_instore_current_13wk_flag", f.coalesce(f.col("sof_instore_current_13wk_flag"), f.lit(0)))\
                        .withColumn("sof_instore_prior_13wk_flag", f.coalesce(f.col("sof_instore_prior_13wk_flag"), f.lit(0)))\
                        .withColumn("sof_ecomm_current_4wk_flag", f.coalesce(f.col("sof_ecomm_current_4wk_flag"), f.lit(0)))\
                        .withColumn("sof_ecomm_current_13wk_flag", f.coalesce(f.col("sof_ecomm_current_13wk_flag"), f.lit(0)))\
                        .withColumn("sof_ecomm_prior_13wk_flag", f.coalesce(f.col("sof_ecomm_prior_13wk_flag"), f.lit(0)))\
                        .withColumn("blf_instore_current_4wk_flag", f.coalesce(f.col("blf_instore_current_4wk_flag"), f.lit(0)))\
                        .withColumn("blf_instore_current_13wk_flag", f.coalesce(f.col("blf_instore_current_13wk_flag"), f.lit(0)))\
                        .withColumn("blf_instore_prior_13wk_flag", f.coalesce(f.col("blf_instore_prior_13wk_flag"), f.lit(0)))\
                        .withColumn("blf_ecomm_current_4wk_flag", f.coalesce(f.col("blf_ecomm_current_4wk_flag"), f.lit(0)))\
                        .withColumn("blf_ecomm_current_13wk_flag", f.coalesce(f.col("blf_ecomm_current_13wk_flag"), f.lit(0)))\
                        .withColumn("blf_ecomm_prior_13wk_flag", f.coalesce(f.col("blf_ecomm_prior_13wk_flag"), f.lit(0)))\
                        .groupBy("card_number", "week_end_date")\
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
                        .withColumn("dl_update_dt", f.lit(now))
  
  distCardWeekDF.unpersist()
  return custTxnAggDF

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold customer transactions at Card and week_end_date level.
# MAGIC * Fetch POS finanical transactions from gold layer retail_sale
# MAGIC * Consider transactions for Save On Foods, Buy Low Foods banners only
# MAGIC * Derive 4,13 and 26 weeks transaction flags for each card at banner and rolling future 26 weeks for week_end_date upto current week_end_date load.
# MAGIC * this table refreshes on every run

# COMMAND ----------

try:
  
  # Get only locations for Save On Foods, Buy Low Foods banners only
  locationDF = spark.table(location_dim)\
                                         .filter((f.col("current_banner_short_name").isin(sofBannerList + blfBannerList)) & (f.col("location_type").isin(locationTypeList)))\
                                         .select("location_hk", "current_banner_short_name")
  # Get Financial items only
  itemDF = spark.table(item_dim).filter(f.col("current_financial_sale_flag") == 'Y')\
                                                    .select("item_hk", "current_financial_sale_flag")
  # Get all cards except card_number = 0
  custDF = spark.table(cust_dim).filter(f.col("card_number") != '0')\
                                                    .select("customer_hk","card_number")
  # Derive current 4 week start date ,current 13 week start date and prior 13 week start & end dates for each week_end_date for all the dates
  dateDF = spark.table(date_dim)\
                                     .select("date", "week_end_date")\
                                     .withColumn("current_4wk_start_dt", f.date_sub(f.col("week_end_date"), 27))\
                                     .withColumn("current_13wk_start_dt", f.date_sub(f.col("week_end_date"), 90))\
                                     .withColumn("prior_13wk_start_dt", f.date_sub(f.col("week_end_date"), 181))\
                                     .withColumn("prior_13wk_end_dt", f.date_sub(f.col("week_end_date"), 91))\
                                     .withColumn("future_26wk_end_dt", f.date_add(f.col("week_end_date"), 175))
  # Persist Date Dataframe
  dateDF.persist()
  dateDF.count()

  # Find the prior 26 weeks start date for last 31 months date
  last_31months_plus_26weeks_start_dt  = dateDF.filter(f.col("date") == last_31_months_date).select("prior_13wk_start_dt").collect()[0]['prior_13wk_start_dt']
  print(f"last 31 months + 26 weeks start date: {last_31months_plus_26weeks_start_dt}")
  
  # Find starting week_end_dt for the aggregation.It should 31 months + 26 weeks week_end_date
  agg_start_week_end_dt = dateDF.filter(f.col("date") == last_31months_plus_26weeks_start_dt).select("week_end_date").collect()[0]['week_end_date']
  print(f"Starting weekend date for this load: {agg_start_week_end_dt}")
  
  # Find the prior 26 weeks start date for last 31 months + 26 weeks back date and use it as a least date in filter condition of retail_sale data
  rs_filter_start_date  = dateDF.filter(f.col("date") == agg_start_week_end_dt).select("prior_13wk_start_dt").collect()[0]['prior_13wk_start_dt']
  print(f"retail_sale_fact filter start date: {rs_filter_start_date}")
  
  
  # Fetch retail_sale fact data from gold layer for the current month and back to the last 31 months.
  retailSalesDF = spark.table(retail_sales_fact)\
                          .filter((f.col("fiscal_date") >= rs_filter_start_date) & (f.col("fiscal_date") <= last_saturday_date))\
                          .select("fiscal_date", "location_hk", "customer_hk", "item_hk", "ecomm_flag")



  # Aggregate data at Card and week level
  custTxnAggDF = cust_transactions_agg(retailSalesDF, locationDF, itemDF, custDF, dateDF, agg_start_week_end_dt, last_saturday_date)

  # Create UC table with DDL
  card_transactions_table = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{TABLE_NAME}"
  spark.sql(f"""
    CREATE OR REPLACE TABLE {card_transactions_table} (
    card_number STRING COMMENT 'Unique card identifier for the customer',
    week_end_date DATE COMMENT 'Week ending date for the transaction period',
    sof_instore_current_4wk_flag INT COMMENT 'SOF instore transaction flag for current 4 weeks',
    sof_instore_current_13wk_flag INT COMMENT 'SOF instore transaction flag for current 13 weeks',
    sof_instore_prior_13wk_flag INT COMMENT 'SOF instore transaction flag for prior 13 weeks',
    sof_ecomm_current_4wk_flag INT COMMENT 'SOF ecommerce transaction flag for current 4 weeks',
    sof_ecomm_current_13wk_flag INT COMMENT 'SOF ecommerce transaction flag for current 13 weeks',
    sof_ecomm_prior_13wk_flag INT COMMENT 'SOF ecommerce transaction flag for prior 13 weeks',
    blf_instore_current_4wk_flag INT COMMENT 'BLF instore transaction flag for current 4 weeks',
    blf_instore_current_13wk_flag INT COMMENT 'BLF instore transaction flag for current 13 weeks',
    blf_instore_prior_13wk_flag INT COMMENT 'BLF instore transaction flag for prior 13 weeks',
    blf_ecomm_current_4wk_flag INT COMMENT 'BLF ecommerce transaction flag for current 4 weeks',
    blf_ecomm_current_13wk_flag INT COMMENT 'BLF ecommerce transaction flag for current 13 weeks',
    blf_ecomm_prior_13wk_flag INT COMMENT 'BLF ecommerce transaction flag for prior 13 weeks',
    dl_load_dt TIMESTAMP COMMENT 'Timestamp of when the data was loaded into the table',
    dl_update_dt TIMESTAMP COMMENT 'Timestamp of when the data was last updated'
    )
    USING delta
    PARTITIONED BY (week_end_date)
    LOCATION '{card_transactions_path}'
    """)

  # Saving the table
  custTxnAggDF.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(card_transactions_table)

  # Un persist Date Dataframe
  dateDF.unpersist()
  
except Exception as ex:
  raise str(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 168 HOURS

# COMMAND ----------

# DBTITLE 1,Vacuum Delta Table
common.vacuum_delta_table(spark, card_transactions_path)
