# Databricks notebook source
# DBTITLE 1,Imports
import json
import csv
from pyspark.sql import types as T
import pyspark.sql.functions as F
from datetime import datetime
from pytz import timezone
from functools import reduce
from pyspark.sql import DataFrame
from utils import common
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# DBTITLE 1,Load Variables from Widgets
dbutils.widgets.text("Environment", "", "")
Environment = dbutils.widgets.get("Environment").upper()

# COMMAND ----------

# DBTITLE 1,Check if Environment is set
if not Environment:
  raise Exception("Environment - Mandatory parameter is not passed")
  
if Environment != 'DEV' and Environment != 'QA' and Environment != 'PROD':
  raise Exception(f"Invalid Environment : {Environment}. Valid values are DEV or QA or PROD")

# COMMAND ----------

# DBTITLE 1,Set Mount Path
config = open("../../../configs/config.json")
settings = json.load(config)
retail_sales_fact = settings[Environment]['GoldMountPath'] + "/source/sales/fact/retail_sale_fact"
location_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/location_dim"
item_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/item_dim"
date_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/date_dim"
cust_dim = settings[Environment]['GoldMountPath'] + "/source/master/dim/customer_dim"
tgtPath = settings[Environment]['GoldMountPath'] + "/source/sales/agg/pos_daily_agg_card"
goldTestResultsPath = settings[Environment]['GoldMountPath'] + "/source/sales/test_results/unit_testresults/pos_daily_agg_card"
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

# DBTITLE 1,Create views for data in silver and gold layers
#creating views to read and compare data
tgtDF = spark.read.format("delta").load(tgtPath)
tgtDF.createOrReplaceTempView("gold_pos_daily_agg_card")

# COMMAND ----------

# DBTITLE 1,Verify Columns Present in Target table
#verify columns present in the table and ensure all the expected columns should be present
expectedTrgColumns = ["fiscal_date", "customer_hk", "location_hk", "channel", "transaction_count", "sales", "scan", "units", "items", "dl_load_dt", "dl_update_dt"]
print("Source:", expectedTrgColumns)
trgColumns = tgtDF.columns
print("Target:", trgColumns)
if trgColumns == expectedTrgColumns:
  testCaseResult1 = 'Pass'
else:
  testCaseResult1='Fail'
print(testCaseResult1)

tableName = "pos_daily_agg_card"
testResult = testCaseResult1
testCaseName = "Column Def Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCase1DF = spark.createDataFrame(
    [
        (tableName,testResult,testCaseName,executionDate)  
    ],
    T.StructType(
        [
            T.StructField("table_name", T.StringType(), True),
            T.StructField("test_result", T.StringType(), True),
            T.StructField("test_case_name", T.StringType(), True),
            T.StructField("execution_date", T.StringType(), True)
        ]
    ),
)
now = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S')
Year = now[0:4]
Month = now[5:7]
Day = now[8:10]

# COMMAND ----------

# DBTITLE 1,Verify data duplication
#verify duplicate records in pos_daily_agg_card table gold layer
dupCntDf = spark.sql("""Select 'Gold_Pos_Daily_Agg_Card_Dup_Check' As Test_Case_Name, Case When Count(*) = 0 Then 'Pass' Else 'Fail' End As Result 
                         From 
                         (Select fiscal_date, customer_hk, location_hk, channel, Count(*) As Cnt 
                           From gold_pos_daily_agg_card
                          Group By fiscal_date, customer_hk, location_hk, channel Having Count(*) > 1)""")
dupCntDf.show()
testCaseResult2 = dupCntDf.collect()[0]['Result']

tableName = "pos_daily_agg_card"
testResult = testCaseResult2
testCaseName = "Data duplication Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCase2DF = spark.createDataFrame(
    [
        (tableName,testResult,testCaseName,executionDate)  
    ],
    T.StructType(
        [
            T.StructField("table_name", T.StringType(), True),
            T.StructField("test_result", T.StringType(), True),
            T.StructField("test_case_name", T.StringType(), True),
            T.StructField("execution_date", T.StringType(), True)
        ]
    ),
)
now = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S')
Year = now[0:4]
Month = now[5:7]
Day = now[8:10]

# COMMAND ----------

# DBTITLE 1,Check if scan column contains any NULLs
#verify whether scan column contains any NULLS or not
ScanColNullDf = spark.sql("""Select 'Gold_Pos_Daily_Agg_Card_Scan_Null_Check' As Test_Case_Name, Case When Count(*) = 0 Then 'Pass' Else 'Fail' End As Result 
                         From gold_pos_daily_agg_card
                         Where scan is NULL""")
ScanColNullDf.show()
testCaseResult3 = ScanColNullDf.collect()[0]['Result']

tableName = "pos_daily_agg_card"
testResult = testCaseResult3
testCaseName = "Scan Column Null Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCase3DF = spark.createDataFrame(
    [
        (tableName,testResult,testCaseName,executionDate)  
    ],
    T.StructType(
        [
            T.StructField("table_name", T.StringType(), True),
            T.StructField("test_result", T.StringType(), True),
            T.StructField("test_case_name", T.StringType(), True),
            T.StructField("execution_date", T.StringType(), True)
        ]
    ),
)
now = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S')
Year = now[0:4]
Month = now[5:7]
Day = now[8:10]

# COMMAND ----------

# DBTITLE 1,Card numbers count check
# Get only locations for Save On Foods, Buy Low Foods banners only
locationDF = spark.read.format("delta").load(location_dim)\
                                       .filter(F.col("current_banner_short_name").isin(sofBannerList))\
                                       .select("location_hk")
# Get Financial items only
itemDF = spark.read.format("delta").load(item_dim).filter(F.col("current_financial_sale_flag") == 'Y')\
                                                  .select("item_hk")
# Get all cards except card_number = 0
custDF = spark.read.format("delta").load(cust_dim).select("customer_hk","card_number")

# Fetch retail_sale fact data from gold layer for the current month and back to the last 31 months.
retailSalesDF = spark.read.format("delta").load(retail_sales_fact)\
                        .filter((F.col("fiscal_date") >= start_sunday) & (F.col("fiscal_date") <= last_saturday_date))\
                        .select("fiscal_date", "location_hk", "customer_hk", "item_hk")

srcDF = retailSalesDF.alias("r").join(locationDF.alias("l"), F.col("r.location_hk") == F.col("l.location_hk"), "inner")\
                                               .join(itemDF.alias("i"), F.col("r.item_hk") == F.col("i.item_hk"), "inner")\
                                               .join(custDF.alias("c"), F.col("r.customer_hk") == F.col("c.customer_hk"), "inner")\
                                               .select("c.card_number").distinct()
srcCnt = srcDF.count()
print(f"Source distinct cards count: {srcCnt}")

targetDF = tgtDF.join(custDF, tgtDF.customer_hk == custDF.customer_hk, "inner")\
             .select(custDF.card_number).distinct()

tgtCnt = targetDF.count()
print(f"Target distinct cards count: {tgtCnt}")


if srcCnt == tgtCnt:
  testCaseResult4 = 'Pass'
else:
  testCaseResult4 = 'Fail'
  
print(testCaseResult4)

tableName = "pos_daily_agg_card"
testResult = testCaseResult4
testCaseName = "card numbers count Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCase4DF = spark.createDataFrame(
    [
        (tableName,testResult,testCaseName,executionDate)  
    ],
    T.StructType(
        [
            T.StructField("table_name", T.StringType(), True),
            T.StructField("test_result", T.StringType(), True),
            T.StructField("test_case_name", T.StringType(), True),
            T.StructField("execution_date", T.StringType(), True)
        ]
    ),
)

testCase4DF.display()

# COMMAND ----------

# DBTITLE 1,Concatenate results sets
#concatenate results and log results into the file
testCaseDfs = [testCase1DF, testCase2DF, testCase3DF, testCase4DF]
finalResultDf = reduce(DataFrame.unionAll, testCaseDfs)
finalResultDf.display()
finalResultDf.repartition(1).write.mode("append").option("header",True).csv(f"{goldTestResultsPath}/{Year}/{Month}/{Day}/test_results_{Year}{Month}{Day}.csv")

# COMMAND ----------

# DBTITLE 1,Check Automated Test Results
webHookURL = settings[Environment]['WebHookURL']
testCaseFailureTitle="Unit Test validations of Gold Layer pos_daily_agg_card table failed"
notebookName = "test_ut_z3_z3_pos_daily_agg_card_full_refresh"
testCaseFailureContent=""

#job should be failed upon any automated tests fail.
try:
  if testCaseResult1 == 'Pass' and testCaseResult2 == 'Pass' and testCaseResult3 == 'Pass'  and testCaseResult4 == 'Pass':
    print('Unit test validations of gold pos_daily_agg_card table succeeded')
  else:
    
    if testCaseResult1 != 'Pass':
      testCaseFailureContent=testCaseFailureContent+"Test Case 1, "
    if testCaseResult2 != 'Pass':
      testCaseFailureContent=testCaseFailureContent+"Test Case 2, "
    if testCaseResult3 != 'Pass':
      testCaseFailureContent=testCaseFailureContent+"Test Case 3, "
    if testCaseResult4 != 'Pass':
      testCaseFailureContent=testCaseFailureContent+"Test Case 4, "
    testCaseFailureContent="Failed test cases: "+testCaseFailureContent[:-2] # Removes the extra , from the list of failed test cases
    
    common.webhook_call(webHookURL,testCaseFailureContent,testCaseFailureTitle,notebookName)
    raise Exception ('Unit test validations of gold pos_daily_agg_card table failed')
finally:
  print('Notebook execution task for Unit Test Validations is completed')

# COMMAND ----------


