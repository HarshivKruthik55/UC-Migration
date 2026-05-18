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

# Unity Catalog three-level namespace
catalog_name = settings[Environment]['catalog_name']
MASTER_DIM_GOLD_SCHEMA = settings['MASTER_DIM_GOLD_SCHEMA']
CUSTOMER_AGG_GOLD_SCHEMA = settings['CUSTOMER_AGG_GOLD_SCHEMA']

# Source and target table names
SOURCE_TABLE_NAME_1 = "customer_dim"
TABLE_NAME = "household_type"

# Target table path
household_type_path = settings[Environment]['GoldMountPath'] + f"/source/customer/agg/{TABLE_NAME}"

# Source table UC references
customer_dim = f"{catalog_name}.{MASTER_DIM_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_1}"

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
  customerDF = spark.table(customer_dim)\
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
  
  # Create UC table with DDL
  household_type_table = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{TABLE_NAME}"
  spark.sql(f"""
    CREATE OR REPLACE TABLE {household_type_table} (
    hhn DECIMAL(12,0) COMMENT 'Unique identifier for each household',
    hh_type STRING COMMENT 'Household type classification (Customer, Community, Manager, Training, Other)',
    dl_load_dt TIMESTAMP COMMENT 'Timestamp of when the data was loaded into the table'
    )
    USING delta
    LOCATION '{household_type_path}'
    """)

  # Saving the table
  hhTypeDF.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(household_type_table)
  
except Exception as ex:
  raise str(ex)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 0 HOURS
# MAGIC * Maintain one snapshot data for this table to support External tables in Azure Synapse.

# COMMAND ----------

DeltaTable = DeltaTable.forPath(spark, household_type_path)
DeltaTable.vacuum(0)
