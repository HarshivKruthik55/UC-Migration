# Databricks notebook source
# MAGIC %md
# MAGIC ### This Notebook performs full refresh of table
# MAGIC * Creates household_type table

# COMMAND ----------

import pyspark.sql.functions as f
from datetime import datetime, timedelta
from delta.tables import *
from utils import common
from pyspark.sql.window import Window
import json

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


# Unity Catalog three-level namespace - dynamic per environment
catalog = f"sofdl_{Environment.lower()}"
schema = "customer"


customer_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/customer_dim"
household_type = settings[Environment]['GoldMountPath'] + "/source/customer/agg/household_type"

now = common.get_now_pst()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold household type table
# MAGIC * Fetch all active customers from customer dim to derive household_type for each HHN.
# MAGIC * Populate hh_type = 'Customer Card' if the household distinct card_type are Customer Card and VMore Cards
# MAGIC * Populate hh_type = 'Other' if the household distinct card_type has more than one value Or card_type = ‘Unknown’
# MAGIC * Populate hh_type = card_type if the household distinct card_type has one value 
# MAGIC * This table refreshes on every run

# COMMAND ----------

try:
  # Fetch most recent records for all customers from customer dim to derive household_type for each HHN.
  # Overwrite 'VMore Cards' card_type to 'Customer Card' to consider it is a Customer Card.
  customerDF = spark.read.format("delta").load(customer_dim)\
                          .withColumn("most_recent_record", f.row_number().over(Window.partitionBy("card_number", "current_household_id").orderBy(f.col("eff_to_dt").desc())))\
                          .filter(f.col("most_recent_record") == 1)\
                          .withColumn("current_card_type", f.when(f.col("current_card_type") == 'VMore Cards', 'Customer Card')
                                                           .otherwise(f.col("current_card_type")))\
                          .select("current_card_type", "current_household_id").distinct()
  
  hhTypeDF = customerDF.withColumn("card_type_count", f.count("current_card_type").over(Window.partitionBy("current_household_id")))\
                       .withColumn("hh_type", f.when(((f.col("card_type_count") > 1) | (f.col("current_card_type") == 'Unknown')), 'Other')
                                               .otherwise(f.col("current_card_type")))\
                       .withColumn("hh_type", f.when(f.col("hh_type") == 'Community Cards', 'Community')\
                                               .when(f.col("hh_type") == 'Customer Card', 'Customer')\
                                               .when(f.col("hh_type") == 'Manager Cards', 'Manager')\
                                               .when(f.col("hh_type") == 'Training Cards', 'Training')\
                                               .otherwise(f.col("hh_type")))\
                       .withColumn("dl_load_dt", f.lit(now))\
                       .withColumnRenamed("current_household_id", "hhn")\
                       .select("hhn", "hh_type", "dl_load_dt").distinct()
  
  # Load data to target table
  hhTypeDF.write.mode("overwrite")\
                    .format("delta")\
                    .save(household_type)
  
except Exception as ex:
  raise str(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 0 HOURS
# MAGIC * Maintain one snapshot data for this table to support External tables in Azure Synapse.

# COMMAND ----------

DeltaTable = DeltaTable.forPath(spark, household_type)
DeltaTable.vacuum(0)

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog table

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.household_type USING DELTA LOCATION '{household_type}'")
