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
tgtPath = settings[Environment]['GoldMountPath'] + "/source/customer/agg/card_transactions"
goldTestResultsPath = settings[Environment]['GoldMountPath'] + "/source/customer/test_results/unit_testresults/card_transactions"

# COMMAND ----------

# DBTITLE 1,Create views for data in gold layers
#creating views for gold layer tables to read and compare data
tgtDf = spark.read.format("delta").load(tgtPath)
tgtDf.createOrReplaceTempView("gold_card_transactions")

# COMMAND ----------

# DBTITLE 1,Verify Columns Present in Target table
#verify columns present in the table and ensure all the expected columns should be present
expectedTrgColumns = ["card_number", "week_end_date", "sof_instore_current_4wk_flag", "sof_instore_current_13wk_flag", "sof_instore_prior_13wk_flag", "sof_ecomm_current_4wk_flag", "sof_ecomm_current_13wk_flag", "sof_ecomm_prior_13wk_flag", "blf_instore_current_4wk_flag", "blf_instore_current_13wk_flag", "blf_instore_prior_13wk_flag", "blf_ecomm_current_4wk_flag", "blf_ecomm_current_13wk_flag", "blf_ecomm_prior_13wk_flag", "dl_load_dt", "dl_update_dt"]
print("Source:", expectedTrgColumns)
trgColumns = tgtDf.columns
print("Target:", trgColumns)
if trgColumns == expectedTrgColumns:
  testCaseResult1 = 'Pass'
else:
  testCaseResult1='Fail'
print(testCaseResult1)

tableName = "card_transactions"
testResult = testCaseResult1
testCaseName = "Column Def Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCase1Df = spark.createDataFrame(
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
#verify duplicate records in card_transactions table gold layer
dupCntDf = spark.sql("""Select 'Gold_Card_Transactions_Dup_Check' As Test_Case_Name, Case When Count(*) = 0 Then 'Pass' Else 'Fail' End As Result 
                         From 
                         (Select card_number, week_end_date, Count(*) As Cnt 
                           From gold_card_transactions
                          Group By card_number, week_end_date Having Count(*) > 1)""")
dupCntDf.show()
testCaseResult2 = dupCntDf.collect()[0]['Result']

tableName = "card_transactions"
testResult = testCaseResult2
testCaseName = "Data duplication Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCase2Df = spark.createDataFrame(
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

# DBTITLE 1,Check if flag column contains NULL
#verify if any flag column contains NULL
ColNullDf = spark.sql("""Select 'Gold_Card_Transactions_Null_Check' As Test_Case_Name, Case When Count(*) = 0 Then 'Pass' Else 'Fail' End As Result 
                         From gold_card_transactions
                         Where sof_instore_current_4wk_flag is NULL
                         Or sof_instore_current_13wk_flag is NULL
                         Or sof_instore_prior_13wk_flag is NULL
                         Or sof_ecomm_current_4wk_flag is NULL
                         Or sof_ecomm_current_13wk_flag is NULL
                         Or sof_ecomm_prior_13wk_flag is NULL
                         Or blf_instore_current_4wk_flag is NULL
                         Or blf_instore_current_13wk_flag is NULL
                         Or blf_instore_prior_13wk_flag is NULL
                         Or blf_ecomm_current_4wk_flag is NULL
                         Or blf_ecomm_current_13wk_flag is NULL
                         Or blf_ecomm_prior_13wk_flag is NULL
                         """)
ColNullDf.show()
testCaseResult3 = ColNullDf.collect()[0]['Result']

tableName = "card_transactions"
testResult = testCaseResult3
testCaseName = "Columns Null Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCase3Df = spark.createDataFrame(
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

# DBTITLE 1,Compare Current 4 week and 13 week flag values
#Current 4 week and 13 week flag values should match when 4 week flag =1
FlagsValuesCheckDf = spark.sql("""Select 'Gold_Card_Transactions_4wkFlag_13wkFlag_Check' As Test_Case_Name, Case When Count(*) = 0 Then 'Pass' Else 'Fail' End As Result 
                         From
                         (
                         Select card_number, week_end_date
                         From gold_card_transactions
                         Where sof_instore_current_4wk_flag = 1 And sof_instore_current_4wk_flag <> sof_instore_current_13wk_flag
                         Union
                         Select card_number, week_end_date
                         From gold_card_transactions
                         Where sof_ecomm_current_4wk_flag = 1 And sof_ecomm_current_4wk_flag <> sof_ecomm_current_13wk_flag
                         Union
                         Select card_number, week_end_date
                         From gold_card_transactions
                         Where blf_instore_current_4wk_flag = 1 And blf_instore_current_4wk_flag <> blf_instore_current_13wk_flag
                         Union
                         Select card_number, week_end_date
                         From gold_card_transactions
                         Where blf_ecomm_current_4wk_flag = 1 And blf_ecomm_current_4wk_flag <> blf_ecomm_current_13wk_flag
                         )
                         """)
FlagsValuesCheckDf.show()
testCaseResult4 = FlagsValuesCheckDf.collect()[0]['Result']

tableName = "card_transactions"
testResult = testCaseResult4
testCaseName = "Flag values Check"
executionDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S') 

testCase4Df = spark.createDataFrame(
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

# DBTITLE 1,Concatenate results sets
#concatenate results and log results into the file
testCaseDfs = [testCase1Df, testCase2Df, testCase3Df, testCase4Df]
finalResultDf = reduce(DataFrame.unionAll, testCaseDfs)
finalResultDf.display()
finalResultDf.repartition(1).write.mode("append").option("header",True).csv(f"{goldTestResultsPath}/{Year}/{Month}/{Day}/test_results_{Year}{Month}{Day}.csv")

# COMMAND ----------

# DBTITLE 1,Check Automated Test Results
webHookURL = settings[Environment]['WebHookURL']
testCaseFailureTitle="Unit Test validations of Gold Layer card_transactions table failed"
notebookName = "test_ut_z3_z3_card_transactions_full_refresh"
testCaseFailureContent=""

#job should be failed upon any automated tests fail.
try:
  if testCaseResult1 == 'Pass' and testCaseResult2 == 'Pass' and testCaseResult3 == 'Pass' and testCaseResult4 == 'Pass':
    print('Unit test validations of gold card_transactions table succeeded')
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
    raise Exception ('Unit test validations of gold card_transactions table failed')
finally:
  print('Notebook execution task for Unit Test Validations is completed')

# COMMAND ----------


