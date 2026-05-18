# Databricks notebook source
# MAGIC %md
# MAGIC ### This Notebook refreshes household_shop_dates table on every run
# MAGIC * Loads data from Gold to Gold. 
# MAGIC * Overwrites the data in Gold location

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import pyspark.sql.functions as f
from datetime import datetime, timedelta
from delta.tables import *
import json
from utils import common
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting spark configuration properties

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Check
# MAGIC * Reading environment value from corresponding widget text box
# MAGIC * Validating the environment value

# COMMAND ----------

dbutils.widgets.text("Environment", "", "")

Environment = dbutils.widgets.get("Environment").upper()  #Envirnment


if not Environment:
  raise Exception("Environment - Mandatory parameter is not passed")
  
if Environment != 'DEV' and Environment != 'QA' and Environment != 'PROD':
  raise Exception(f"Invalid Environment : {Environment}. Valid values are DEV or QA or PROD")
  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Config File
# MAGIC * Reading table paths from config file

# COMMAND ----------

config = open("../../configs/config.json")
settings = json.load(config)

# Unity Catalog three-level namespace
catalog_name = settings[Environment]['catalog_name']
CUSTOMER_AGG_GOLD_SCHEMA = settings['CUSTOMER_AGG_GOLD_SCHEMA']

# Source and target table names
SOURCE_TABLE_NAME_1 = "customer_dim"
SOURCE_TABLE_NAME_2 = "card_shop_dates"
TABLE_NAME = "household_shop_dates"

# Target table path
household_dates_path = settings[Environment]['GoldMountPath'] + f"/source/customer/agg/{TABLE_NAME}/"

# Source table UC references
customer_dim = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_1}"
card_shop_dates_table = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{SOURCE_TABLE_NAME_2}"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading source tables 
# MAGIC * Reading source tables(customer dim and card_shop_dates) from gold, 

# COMMAND ----------

#dim_customer : Spark.DataFrame
#Customer details
cust = spark.table(customer_dim)\
                 .filter(f.col("card_number") != '0')\
                 .select("card_number","current_household_id").distinct()

#Card Shop Dates
card_shop_dates = spark.table(card_shop_dates_table)\
                      .drop("dl_load_dt","dl_update_dt")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold to Gold Shop Dates Aggregate at Household Level 
# MAGIC * Uses card_shop_date and customer dim tables
# MAGIC * Performs Aggregate at household level
# MAGIC * dl_load_dt is populated using the run date to make sure sure that it reflects correct date for non scheduled runs as well as scheduled runs
# MAGIC * This table refreshes on every run

# COMMAND ----------

now = common.get_now_pst()

household_shop_date = card_shop_dates.join(cust, card_shop_dates.card_number == cust.card_number)\
                                    .groupby(f.col('current_household_id'))\
                                    .agg(
                                          f.min(f.col("sof_instore_first_shop_date")).alias("sof_instore_first_shop_date"),
                                          f.max(f.col("sof_instore_last_shop_date")).alias("sof_instore_last_shop_date"),
                                          f.min(f.col("sof_ecom_first_shop_date")).alias("sof_ecom_first_shop_date"),
                                          f.max(f.col("sof_ecom_last_shop_date")).alias("sof_ecom_last_shop_date"),
                                          f.min(f.col("buylow_instore_first_shop_date")).alias("buylow_instore_first_shop_date"),
                                          f.max(f.col("buylow_instore_last_shop_date")).alias("buylow_instore_last_shop_date"),
                                          f.min(f.col("buylow_ecom_first_shop_date")).alias("buylow_ecom_first_shop_date"),
                                          f.max(f.col("buylow_ecom_last_shop_date")).alias("buylow_ecom_last_shop_date"),
                                          f.min(f.col("sof_only_instore_first_shop_date")).alias("sof_only_instore_first_shop_date"),
                                          f.max(f.col("sof_only_instore_last_shop_date")).alias("sof_only_instore_last_shop_date"),
                                          f.min(f.col("sof_only_ecom_first_shop_date")).alias("sof_only_ecom_first_shop_date"),
                                          f.max(f.col("sof_only_ecom_last_shop_date")).alias("sof_only_ecom_last_shop_date"),
                                          f.min(f.col("uf_only_instore_first_shop_date")).alias("uf_only_instore_first_shop_date"),
                                          f.max(f.col("uf_only_instore_last_shop_date")).alias("uf_only_instore_last_shop_date"),
                                          f.min(f.col("uf_only_ecom_first_shop_date")).alias("uf_only_ecom_first_shop_date"),
                                          f.max(f.col("uf_only_ecom_last_shop_date")).alias("uf_only_ecom_last_shop_date"),
                                          f.min(f.col("psf_only_instore_first_shop_date")).alias("psf_only_instore_first_shop_date"),
                                          f.max(f.col("psf_only_instore_last_shop_date")).alias("psf_only_instore_last_shop_date"),
                                          f.min(f.col("psf_only_ecom_first_shop_date")).alias("psf_only_ecom_first_shop_date"),
                                          f.max(f.col("psf_only_ecom_last_shop_date")).alias("psf_only_ecom_last_shop_date"),
                                          f.min(f.col("blf_only_instore_first_shop_date")).alias("blf_only_instore_first_shop_date"),
                                          f.max(f.col("blf_only_instore_last_shop_date")).alias("blf_only_instore_last_shop_date"),
                                          f.min(f.col("blf_only_ecom_first_shop_date")).alias("blf_only_ecom_first_shop_date"),
                                          f.max(f.col("blf_only_ecom_last_shop_date")).alias("blf_only_ecom_last_shop_date"),
                                          f.min(f.col("nm_only_instore_first_shop_date")).alias("nm_only_instore_first_shop_date"),
                                          f.max(f.col("nm_only_instore_last_shop_date")).alias("nm_only_instore_last_shop_date"),
                                          f.min(f.col("nm_only_ecom_first_shop_date")).alias("nm_only_ecom_first_shop_date"),
                                          f.max(f.col("nm_only_ecom_last_shop_date")).alias("nm_only_ecom_last_shop_date"),
                                          f.min(f.col("mff_only_instore_first_shop_date")).alias("mff_only_instore_first_shop_date"),
                                          f.max(f.col("mff_only_instore_last_shop_date")).alias("mff_only_instore_last_shop_date"),
                                          f.min(f.col("mff_only_ecom_first_shop_date")).alias("mff_only_ecom_first_shop_date"),
                                          f.max(f.col("mff_only_ecom_last_shop_date")).alias("mff_only_ecom_last_shop_date"),
                                          f.min(f.col("qf_only_instore_first_shop_date")).alias("qf_only_instore_first_shop_date"),
                                          f.max(f.col("qf_only_instore_last_shop_date")).alias("qf_only_instore_last_shop_date"),
                                          f.min(f.col("qf_only_ecom_first_shop_date")).alias("qf_only_ecom_first_shop_date"),
                                          f.max(f.col("qf_only_ecom_last_shop_date")).alias("qf_only_ecom_last_shop_date"),
                                          f.min(f.col("pfg_instore_first_shop_date")).alias("pfg_instore_first_shop_date"),
                                          f.max(f.col("pfg_instore_last_shop_date")).alias("pfg_instore_last_shop_date"),
                                          f.min(f.col("pfg_ecom_first_shop_date")).alias("pfg_ecom_first_shop_date"),
                                          f.max(f.col("pfg_ecom_last_shop_date")).alias("pfg_ecom_last_shop_date"),
                                          f.min(f.col("pfg_overall_first_shop_date")).alias("pfg_overall_first_shop_date"),
                                          f.max(f.col("pfg_overall_last_shop_date")).alias("pfg_overall_last_shop_date")
                                    )\
                                    .withColumn("dl_load_dt", f.lit(now))\
                                    .withColumnRenamed("current_household_id","hhn")\
                                    .select("hhn",
                                            "sof_instore_first_shop_date", "sof_instore_last_shop_date",
                                            "sof_ecom_first_shop_date", "sof_ecom_last_shop_date",
                                            "buylow_instore_first_shop_date", "buylow_instore_last_shop_date",
                                            "buylow_ecom_first_shop_date", "buylow_ecom_last_shop_date",
                                            "sof_only_instore_first_shop_date", "sof_only_instore_last_shop_date",
                                            "sof_only_ecom_first_shop_date", "sof_only_ecom_last_shop_date",
                                            "uf_only_instore_first_shop_date", "uf_only_instore_last_shop_date",
                                            "uf_only_ecom_first_shop_date", "uf_only_ecom_last_shop_date",
                                            "psf_only_instore_first_shop_date", "psf_only_instore_last_shop_date",
                                            "psf_only_ecom_first_shop_date", "psf_only_ecom_last_shop_date",
                                            "blf_only_instore_first_shop_date", "blf_only_instore_last_shop_date",
                                            "blf_only_ecom_first_shop_date", "blf_only_ecom_last_shop_date",
                                            "nm_only_instore_first_shop_date", "nm_only_instore_last_shop_date",
                                            "nm_only_ecom_first_shop_date", "nm_only_ecom_last_shop_date",
                                            "mff_only_instore_first_shop_date", "mff_only_instore_last_shop_date",
                                            "mff_only_ecom_first_shop_date", "mff_only_ecom_last_shop_date",
                                            "qf_only_instore_first_shop_date", "qf_only_instore_last_shop_date",
                                            "qf_only_ecom_first_shop_date", "qf_only_ecom_last_shop_date",
                                            "pfg_instore_first_shop_date","pfg_instore_last_shop_date",
                                            "pfg_ecom_first_shop_date","pfg_ecom_last_shop_date",
                                            "pfg_overall_first_shop_date","pfg_overall_last_shop_date",
                                            "dl_load_dt")
  

#Create UC table with DDL
household_shop_dates_table = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{TABLE_NAME}"
spark.sql(f"""
    CREATE OR REPLACE TABLE {household_shop_dates_table} (
    hhn DECIMAL(12,0) COMMENT 'Unique identifier for each household',
    sof_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for SOF',
    sof_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for SOF',
    sof_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for SOF',
    sof_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for SOF',
    buylow_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for BuyLow',
    buylow_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for BuyLow',
    buylow_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for BuyLow',
    buylow_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for BuyLow',
    sof_only_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for SOF only',
    sof_only_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for SOF only',
    sof_only_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for SOF only',
    sof_only_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for SOF only',
    uf_only_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for UF only',
    uf_only_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for UF only',
    uf_only_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for UF only',
    uf_only_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for UF only',
    psf_only_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for PSF only',
    psf_only_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for PSF only',
    psf_only_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for PSF only',
    psf_only_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for PSF only',
    blf_only_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for BLF only',
    blf_only_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for BLF only',
    blf_only_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for BLF only',
    blf_only_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for BLF only',
    nm_only_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for NM only',
    nm_only_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for NM only',
    nm_only_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for NM only',
    nm_only_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for NM only',
    mff_only_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for MFF only',
    mff_only_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for MFF only',
    mff_only_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for MFF only',
    mff_only_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for MFF only',
    qf_only_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for QF only',
    qf_only_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for QF only',
    qf_only_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for QF only',
    qf_only_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for QF only',
    pfg_instore_first_shop_date DATE COMMENT 'Date of the first instore shopping visit for PFG',
    pfg_instore_last_shop_date DATE COMMENT 'Date of the last instore shopping visit for PFG',
    pfg_ecom_first_shop_date DATE COMMENT 'Date of the first ecommerce shopping visit for PFG',
    pfg_ecom_last_shop_date DATE COMMENT 'Date of the last ecommerce shopping visit for PFG',
    pfg_overall_first_shop_date DATE COMMENT 'Overall date of the first shop visit across all formats',
    pfg_overall_last_shop_date DATE COMMENT 'Overall date of the last shop visit across all formats',
    dl_load_dt TIMESTAMP COMMENT 'Timestamp of when the data was loaded into the table'
    )
    USING delta
    LOCATION '{household_dates_path}'
    """)

# Saving the table
household_shop_date.write \
  .mode("overwrite") \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .saveAsTable(household_shop_dates_table)
