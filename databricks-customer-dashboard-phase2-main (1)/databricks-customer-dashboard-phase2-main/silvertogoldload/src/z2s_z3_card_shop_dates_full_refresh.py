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


# Unity Catalog three-level namespace - dynamic per environment
catalog = f"sofdl_{Environment.lower()}"
schema = "customer"

# table paths
rs_path = settings[Environment]['SilverMountPath'] + "/source/ldw/product/retail_sale/"
customer_path = settings[Environment]['SilverMountPath'] + "/source/merged/customer/customer"
item_path = settings[Environment]['SilverMountPath'] + "/source/ldw/product/item"
location_path = settings[Environment]['SilverMountPath'] + "/source/ldw/product/location"
card_shop_dates_path = settings[Environment]['GoldMountPath'] + "/source/customer/agg/card_shop_dates/"

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
rs = spark.read.format('delta').load(rs_path).select('fiscal_Date', 'customer_id', 'store_number', 'ecomm_flag', 'item_id')

#dim_customer : Spark.DataFrame
#Customer details
cust = spark.read.format('delta').load(customer_path)\
                 .filter(f.col("card_number") != '0')\
                 .withColumn("most_recent_record", f.row_number().over(Window.partitionBy(f.col("card_number")).orderBy(f.col("eff_to_dt").desc())))\
                             .filter((f.col("most_recent_record") == 1))\
                 .select('ldw_customer_id',"card_number").distinct()

#dim_product : Spark.DataFrame
#Product details. Using this table to filter Financial sales.
item = spark.read.format('delta').load(item_path)\
            .withColumn("most_recent_record", f.row_number().over(Window.partitionBy(f.col("id")).orderBy(f.col("eff_to_dt").desc())))\
                             .filter((f.col("most_recent_record") == 1))\
            .select('id', 'Financial_Sale_Flag')

#dim_location : Spark.DataFrame
#Location details. Using this table to filter ST(Store Type) locations under the specified banners.
location = spark.read.format('delta').load(location_path)\
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
  
#Saving the table
cust_shop_date.write.mode("overwrite")\
  .format("delta")\
  .save(card_shop_dates_path) 

cust.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM DELTA TABLE FOR 168 HOURS

# COMMAND ----------

common.vacuum_delta_table(spark, card_shop_dates_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating table in Unity Catalog

# COMMAND ----------


spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.card_shop_dates USING DELTA LOCATION '{card_shop_dates}'")
