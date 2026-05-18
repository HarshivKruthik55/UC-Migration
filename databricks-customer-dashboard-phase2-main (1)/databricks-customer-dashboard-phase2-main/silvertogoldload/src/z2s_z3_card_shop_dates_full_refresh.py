# Databricks notebook source
# MAGIC %md
# MAGIC ### This Notebook refreshes customer_shop_dates table on every run
# MAGIC * Loads data from Silver to Gold. 
# MAGIC * Overwrites the data in Gold location

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# DBTITLE 0,Imports
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
# MAGIC * Reading banner and location lists from config file

# COMMAND ----------

config = open("../../configs/config.json")
settings = json.load(config)

# Unity Catalog three-level namespace
catalog_name = settings[Environment]['catalog_name']
SILVER_LDW_PRODUCT_SCHEMA = settings['SILVER_LDW_PRODUCT_SCHEMA']
SILVER_MERGED_CUSTOMER_SCHEMA = settings['SILVER_MERGED_CUSTOMER_SCHEMA']
CUSTOMER_AGG_GOLD_SCHEMA = settings['CUSTOMER_AGG_GOLD_SCHEMA']

# Source and target table names
SOURCE_TABLE_NAME_1 = "retail_sale"
SOURCE_TABLE_NAME_2 = "customer"
SOURCE_TABLE_NAME_3 = "item"
SOURCE_TABLE_NAME_4 = "location"
TABLE_NAME = "card_shop_dates"

# Target table path
card_shop_dates_path = settings[Environment]['GoldMountPath'] + f"/source/customer/agg/{TABLE_NAME}/"

# Source table UC references
rs_table = f"{catalog_name}.{SILVER_LDW_PRODUCT_SCHEMA}.{SOURCE_TABLE_NAME_1}"
customer_table = f"{catalog_name}.{SILVER_MERGED_CUSTOMER_SCHEMA}.{SOURCE_TABLE_NAME_2}"
item_table = f"{catalog_name}.{SILVER_LDW_PRODUCT_SCHEMA}.{SOURCE_TABLE_NAME_3}"
location_table = f"{catalog_name}.{SILVER_LDW_PRODUCT_SCHEMA}.{SOURCE_TABLE_NAME_4}"

sofBannerList = settings["sofBannerList"]
blfBannerList = settings["blfBannerList"]
locationTypeList = settings["locationTypeList"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading source tables and filtering
# MAGIC * Reading source tables(retail sales, customer dim and item dim) from silver, because gold only has around 3 years of transactions for retail sales
# MAGIC * Filtering retail sales transactions based on item_dim financial flag, location_dim banner_short_name and location_dim location_type.

# COMMAND ----------

#Daily transaction table
rs = spark.table(rs_table).select('fiscal_Date', 'customer_id', 'store_number', 'ecomm_flag', 'item_id')

#dim_customer : Spark.DataFrame
#Customer details
cust = spark.table(customer_table)\
                 .filter(f.col("card_number") != '0')\
                 .withColumn("most_recent_record", f.row_number().over(Window.partitionBy(f.col("card_number")).orderBy(f.col("eff_to_dt").desc())))\
                             .filter((f.col("most_recent_record") == 1))\
                 .select('ldw_customer_id',"card_number").distinct()

#dim_product : Spark.DataFrame
#Product details. Using this table to filter Financial sales.
item = spark.table(item_table)\
            .withColumn("most_recent_record", f.row_number().over(Window.partitionBy(f.col("id")).orderBy(f.col("eff_to_dt").desc())))\
                             .filter((f.col("most_recent_record") == 1))\
            .select('id', 'Financial_Sale_Flag')

#dim_location : Spark.DataFrame
#Location details. Using this table to filter ST(Store Type) locations under the specified banners.
location = spark.table(location_table)\
                .withColumn("most_recent_record", f.row_number().over(Window.partitionBy(f.col("id")).orderBy(f.col("eff_to_dt").desc())))\
                             .filter((f.col("most_recent_record") == 1))\
                .filter(f.col('location_type').isin(locationTypeList))\
                .select('id',"banner_short_name")

#Filtering for financial sales for store transactions for required banners
frs = rs.join(item.filter((f.col('Financial_Sale_Flag')=='Y')).select('id'), (rs.item_id==item.id))\
       .drop(item.id)\
       .join(location, rs.store_number == location.id)\
       .select('customer_id','fiscal_date','ecomm_flag','banner_short_name')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Get shop dates function by aggregating retail sales data 
# MAGIC * Uses filtered retail sales silver table and customer dim silver table
# MAGIC * Aggregated by card number
# MAGIC * Returns a dataframe with required shop dates

# COMMAND ----------

def get_shop_dates(frs,cust):
  cust_shop_date=frs\
  .join(cust,frs.customer_id == cust.ldw_customer_id)\
  .drop("customer_id", "ldw_customer_id")\
  .groupby(f.col('card_number'))\
  .agg(
   f.min(f.when((frs.banner_short_name.isin(sofBannerList))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("sof_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin(sofBannerList))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("sof_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin(sofBannerList))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("sof_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin(sofBannerList))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("sof_ecom_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin(blfBannerList))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("buylow_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin(blfBannerList))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("buylow_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin(blfBannerList))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("buylow_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin(blfBannerList))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("buylow_ecom_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("SOF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("sof_only_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("SOF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("sof_only_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("SOF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("sof_only_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("SOF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("sof_only_ecom_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("UF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("uf_only_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("UF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("uf_only_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("UF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("uf_only_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("UF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("uf_only_ecom_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("PSF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("psf_only_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("PSF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("psf_only_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("PSF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("psf_only_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("PSF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("psf_only_ecom_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("BLF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("blf_only_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("BLF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("blf_only_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("BLF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("blf_only_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("BLF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("blf_only_ecom_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("NM"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("nm_only_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("NM"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("nm_only_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("NM"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("nm_only_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("NM"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("nm_only_ecom_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("MFF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("mff_only_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("MFF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("mff_only_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("MFF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("mff_only_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("MFF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("mff_only_ecom_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("QF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("qf_only_instore_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("QF"))&(frs.ecomm_flag=='Non-Ecomm Sale'),
                 frs.fiscal_date)).alias("qf_only_instore_last_shop_date"),
   f.min(f.when((frs.banner_short_name.isin("QF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("qf_only_ecom_first_shop_date"),
   f.max(f.when((frs.banner_short_name.isin("QF"))&(frs.ecomm_flag=='Ecomm Sale'),    
                 frs.fiscal_date)).alias("qf_only_ecom_last_shop_date"),
   f.min(f.when(frs.ecomm_flag=='Non-Ecomm Sale',frs.fiscal_date)).alias("pfg_instore_first_shop_date"),
   f.max(f.when(frs.ecomm_flag=='Non-Ecomm Sale',frs.fiscal_date)).alias("pfg_instore_last_shop_date"),
   f.min(f.when(frs.ecomm_flag=='Ecomm Sale',    frs.fiscal_date)).alias("pfg_ecom_first_shop_date"),
   f.max(f.when(frs.ecomm_flag=='Ecomm Sale',    frs.fiscal_date)).alias("pfg_ecom_last_shop_date")
  )
  cust_shop_date = cust_shop_date\
    .withColumn("pfg_overall_first_shop_date",  f.least(f.col('pfg_instore_first_shop_date'), 
                                                        f.col('pfg_ecom_first_shop_date')))\
    .withColumn("pfg_overall_last_shop_date", f.greatest(f.col('pfg_instore_last_shop_date'), 
                                                         f.col('pfg_ecom_last_shop_date')))
  
  return cust_shop_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver to Gold Shop Dates Aggregate at Card Level 
# MAGIC * Calls get_shop_dates function to perform aggregation
# MAGIC * dl_load_dt is populated using the run date to make sure sure that it reflects correct date for non scheduled runs as well as scheduled runs
# MAGIC * This table refreshes on every run

# COMMAND ----------

now = common.get_now_pst()
run_date = now.date()
last_saturday_date = common.get_last_saturday(run_date)

cust.persist()
cust.count()

# Getting last shop dates using filtered retail sales table
shop_dates = get_shop_dates(frs.filter(f.col("fiscal_date").cast("date") <= last_saturday_date),cust).alias("shop_dates")

cust_shop_date = shop_dates.join(cust.select("card_number"), cust.card_number == shop_dates.card_number,"right")\
                    .drop(cust.card_number)\
                    .withColumn("dl_load_dt", f.lit(now))\
                    .select("card_number",
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
card_shop_dates_table = f"{catalog_name}.{CUSTOMER_AGG_GOLD_SCHEMA}.{TABLE_NAME}"
spark.sql(f"""
    CREATE OR REPLACE TABLE {card_shop_dates_table} (
    card_number STRING COMMENT 'Unique card identifier for the customer',
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
    LOCATION '{card_shop_dates_path}'
    """)

# Saving the table
cust_shop_date.write \
  .mode("overwrite") \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .saveAsTable(card_shop_dates_table)

cust.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 168 HOURS

# COMMAND ----------

common.vacuum_delta_table(spark, card_shop_dates_path)
