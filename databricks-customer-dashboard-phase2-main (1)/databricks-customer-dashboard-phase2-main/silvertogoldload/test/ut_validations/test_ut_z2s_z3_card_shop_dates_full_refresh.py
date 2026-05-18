# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import json
import csv
from pyspark.sql import types as T
import pyspark.sql.functions as F
from datetime import datetime
from pytz import timezone
from functools import reduce
from pyspark.sql import DataFrame
from utils import common

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Check
# MAGIC * Reading environment value from corresponding widget text box
# MAGIC * Validating the environment value

# COMMAND ----------

dbutils.widgets.text("Environment", "", "")
Environment = dbutils.widgets.get("Environment").upper()

if not Environment:
  raise Exception("Environment - Mandatory parameter is not passed")
  
if Environment != 'DEV' and Environment != 'QA' and Environment != 'PROD':
  raise Exception(f"Invalid Environment : {Environment}. Valid values are DEV or QA or PROD")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Config File
# MAGIC * Reading table paths from config file
# MAGIC * Setting a value for target table name

# COMMAND ----------

config = open("../../../configs/config.json")
settings = json.load(config)
customer_path = settings[Environment]['SilverMountPath'] + "/source/merged/customer/customer"
goldPath = settings[Environment]['GoldMountPath'] + "/source/customer/agg/card_shop_dates/"
goldTestResultsPath = settings[Environment]['GoldMountPath'] + "/source/customer/test_results/unit_testresults/card_shop_dates"
targetTableName = "Gold_Card_Shop_Dates"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading tables and create views
# MAGIC * Reading tables(Card Shop Dates and customer dim)
# MAGIC * Create views for household shop dates table and customer dim silver

# COMMAND ----------

goldDf = spark.read.format("delta").load(goldPath)
customerDF = spark.read.format("delta").load(customer_path)

goldDf.createOrReplaceTempView("gold_card_shop_dates")
customerDF.createOrReplaceTempView("customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Columns Present in Gold Layer for Card Shop Dates Table

# COMMAND ----------

expectedTrgColumns = ["card_number",
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
                            "dl_load_dt"]
trgColumns = goldDf.columns
if trgColumns == expectedTrgColumns:
  resultDf1 = 'Pass'
else:
  resultDf1 = 'Fail'
print(resultDf1)
tableName = targetTableName
testResult = resultDf1
testCaseName = "Column Def Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCaseDf1 = common.create_test_case_df(spark, tableName, testResult, testCaseName, executionDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Card Number Duplication in Gold Layer for Card Shop Dates Table

# COMMAND ----------

dupCntDf = spark.sql("""Select 'Gold_Trans_Types_Dup_Check' As Test_Case_Name, Case When Count(*) = 0 Then 'Pass' Else 'Fail' End As Result 
                          From (Select card_number, Count(*) From gold_card_shop_dates Group By card_number Having Count(*)>1)""")
dupCntDf.show()
resultDf2 = dupCntDf.collect()[0]['Result']
tableName = targetTableName
testResult = resultDf2
testCaseName = "Duplicate Records Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCaseDf2 = common.create_test_case_df(spark, tableName, testResult, testCaseName, executionDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verifying that card_shop_dates table includes all card numbers from client table

# COMMAND ----------

dtRecencyDf = spark.sql("""Select 
                           'Gold_All_Cards_Present_Check' As Test_Case_Name, 
                           Case When Count(*) = 0 Then 'Pass' Else 'Fail' End As Result 
                             From (Select card_number From customer  where card_number != '0'
                                     except 
                                   Select card_number From gold_card_shop_dates) """)
dtRecencyDf.show()
resultDf3 = dtRecencyDf.collect()[0]['Result']
tableName = targetTableName
testResult = resultDf3
testCaseName = "Verifying that card_shop_dates table includes all card numbers from client table"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCaseDf3 = common.create_test_case_df(spark, tableName, testResult, testCaseName, executionDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concatenate Results Sets

# COMMAND ----------

testCaseDfs = [testCaseDf1, testCaseDf2, testCaseDf3]
finalResultDf = reduce(DataFrame.unionAll, testCaseDfs)

now = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S')
year = now[0:4]
month = now[5:7]
day = now[8:10]

finalResultDf.repartition(1).write.mode("append").option("header",True).csv(f"{goldTestResultsPath}/{year}/{month}/{day}/test_results_{year}{month}{day}.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Automated Test Results

# COMMAND ----------

webHookURL = settings[Environment]['WebHookURL']
testCaseFailureTitle = "Unit Test validations of Gold Layer Card Shop Dates Table failed"
notebookName = "test_ut_z2s_z3_card_shop_dates_full_refresh"
testResults = [resultDf1, resultDf2, resultDf3]

testCaseFailureContent = ""
try:
  if all([res == 'Pass' for res in testResults]):
    print('Success')
  else:
    failed_test_cases_messages = [f"Test Case {i + 1}" for i, res in enumerate(testResults) if res == 'Fail']
    testCaseFailureContent = "Failed test cases: " + ', '.join(failed_test_cases_messages)
    common.webhook_call(webHookURL,testCaseFailureContent,testCaseFailureTitle,notebookName)
    raise Exception ('Unit Test validations of Gold Layer Card Shop Dates Table failed',)
finally:
  print('Notebook execution task for Unit Test Validations')
