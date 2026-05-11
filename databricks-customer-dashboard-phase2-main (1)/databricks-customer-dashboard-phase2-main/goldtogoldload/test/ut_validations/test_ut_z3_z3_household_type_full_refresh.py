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
from pyspark.sql.window import Window

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
tgtPath = settings[Environment]['GoldMountPath'] + "/source/customer/agg/household_type"
custDimPath = settings[Environment]['GoldMountPath'] + "/source/master/dim/customer_dim"
dateDimPath = settings[Environment]['GoldMountPath'] + "/source/master/dim/date_dim"
goldTestResultsPath = settings[Environment]['GoldMountPath'] + "/source/customer/test_results/unit_testresults/household_type"

# COMMAND ----------

# DBTITLE 1,Create views for data in silver and gold layers
#creating views for gold layers to read and compare data
tgtDF = spark.read.format("delta").load(tgtPath)
tgtDF.createOrReplaceTempView("gold_householdtype")

# COMMAND ----------

# DBTITLE 1,Verify Columns Present in Target table
#verify columns present in the table and ensure all the expected columns should be present
expectedTrgColumns = ["hhn", "hh_type", "dl_load_dt"]
print("Source:", expectedTrgColumns)
trgColumns = tgtDF.columns
print("Target:", trgColumns)
if trgColumns == expectedTrgColumns:
  testCaseResult1 = 'Pass'
else:
  testCaseResult1='Fail'
print(testCaseResult1)

tableName = "Gold_Household_Type"
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
#verify duplicate records in Household_Type table gold layer
dupCntDf = spark.sql("""Select 'Gold_Household_Type_Dup_Check' As Test_Case_Name, Case When Count(*) = 0 Then 'Pass' Else 'Fail' End As Result 
                         From 
                         (Select hhn, Count(*) As Cnt 
                           From gold_householdtype
                          Group By hhn Having Count(*) > 1)""")
dupCntDf.show()
testCaseResult2 = dupCntDf.collect()[0]['Result']

tableName = "Gold_Household_Type"
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

# DBTITLE 1,Household type check
# Fetch most recent records for all customers from customer dim to derive household_type for each HHN.
# Overwrite 'VMore Cards' card_type to 'Customer Card' to consider it is a Customer Card.
customerDF = spark.read.format("delta").load(custDimPath)\
                        .withColumn("most_recent_record", F.row_number().over(Window.partitionBy("card_number", "current_household_id").orderBy(F.col("eff_to_dt").desc())))\
                        .filter(F.col("most_recent_record") == 1)\
                        .withColumn("current_card_type", F.when(F.col("current_card_type") == 'VMore Cards', 'Customer Card')
                                                         .otherwise(F.col("current_card_type")))\
                        .select("current_card_type", "current_household_id").distinct()

srcHHTypeDF = customerDF.withColumn("card_type_count", F.count("current_card_type").over(Window.partitionBy("current_household_id")))\
                        .withColumn("hh_type", F.when(((F.col("card_type_count") > 1) | (F.col("current_card_type") == 'Unknown')), 'Other')
                                .otherwise(F.col("current_card_type")))\
                        .withColumn("hh_type", F.when(F.col("hh_type") == 'Community Cards', 'Community')\
                                                    .when(F.col("hh_type") == 'Customer Card', 'Customer')\
                                                    .when(F.col("hh_type") == 'Manager Cards', 'Manager')\
                                                    .when(F.col("hh_type") == 'Training Cards', 'Training')\
                                                    .otherwise(F.col("hh_type")))\
                        .withColumnRenamed("current_household_id", "hhn")\
                        .select("hhn", "hh_type").distinct()

targetDF = tgtDF.select("hhn", "hh_type")
misMatchHHTypeDF = targetDF.subtract(srcHHTypeDF)

misMatchHHTypeCnt = misMatchHHTypeDF.count()
print(f"Household Type mismatch count: {misMatchHHTypeCnt}")

if misMatchHHTypeCnt == 0:
  testCaseResult3 = 'Pass'
else:
  testCaseResult3 = 'Fail'
print(testCaseResult3)

tableName = "Gold_Household_Type"
testResult = testCaseResult3
testCaseName = "Household Type Check"
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

# COMMAND ----------

# DBTITLE 1,Concatenate results sets
#concatenate results and log results into the file
testCaseDfs = [testCase1DF, testCase2DF, testCase3DF]
finalResultDf = reduce(DataFrame.unionAll, testCaseDfs)
finalResultDf.repartition(1).write.mode("append").option("header",True).csv(f"{goldTestResultsPath}/{Year}/{Month}/{Day}/test_results_{Year}{Month}{Day}.csv")

# COMMAND ----------

# DBTITLE 1,Check Automated Test Results
webHookURL = settings[Environment]['WebHookURL']
testCaseFailureTitle="Unit Test validations of Gold Layer household_type table failed"
notebookName = "test_ut_z3_z3_household_type_full_refresh"
testCaseFailureContent=""

#job should be failed upon any automated tests fail.
try:
  if testCaseResult1 == 'Pass' and testCaseResult2 == 'Pass' and testCaseResult3 == 'Pass':
    print('Unit test validations of gold household_type table succeeded')
  else:
    
    if testCaseResult1 != 'Pass':
      testCaseFailureContent=testCaseFailureContent+"Test Case 1, "
    if testCaseResult2 != 'Pass':
      testCaseFailureContent=testCaseFailureContent+"Test Case 2, "
    if testCaseResult3 != 'Pass':
      testCaseFailureContent=testCaseFailureContent+"Test Case 3, "
    testCaseFailureContent="Failed test cases: "+testCaseFailureContent[:-2] # Removes the extra , from the list of failed test cases
    
    common.webhook_call(webHookURL,testCaseFailureContent,testCaseFailureTitle,notebookName)
    raise Exception ('Unit test validations of gold household_type table failed')
finally:
  print('Notebook execution task for Unit Test Validations is completed')
