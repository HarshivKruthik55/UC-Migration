import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from delta.tables import *
import pytz
from datetime import date, timedelta, datetime as dt
import requests

## CONVERT STRING TYPE TIMESTAMP COLUMNS TO TIMESTAMP TYPE 
def to_timestamp(srcDf, col_names):
  for colmn in col_names:
    srcDf = srcDf.withColumn(colmn, f.col(colmn).cast("timestamp"))
  return srcDf

#### VACUUM DELTA TABLE FOR 168 HOURS

def vacuum_delta_table(spark, DeltaTablePath):
  deltaTable = DeltaTable.forPath(spark, DeltaTablePath)
  deltaTable.vacuum(168)
  

## RETURNS CURRENT PST TIMESTAMP
  
def get_now_pst():
  tz = pytz.timezone('US/Pacific')
  current_time = dt.now(tz).replace(microsecond=0).replace(tzinfo=None) 
  return current_time
    
#### Function definition to check if File Exists in the Unprocessed Path

def file_exists(contentList):
  for content in contentList:
    if ".parquet" in content.name:
      return True
  return False 

## RETURNS THE LAST SATURDAY'S DATE

def get_last_saturday(Date):
  # last week date for the current day
  last_week_date = Date + timedelta(-7)
  make_it_saturday_dict = {1: 5, 2:4,3:3,4:2,5:1,6:0,7:6}
  # return last saturday's date
  return(last_week_date + timedelta(make_it_saturday_dict[Date.isoweekday()]))


## RETURNS THE LAST SUNDAY'S DATE

def get_last_sunday(Date):
  # last week date for the current day
  last_week_date = Date + timedelta(-7)
  make_it_sunday_dict = {1: 6, 2:5,3:4,4:3,5:2,6:1,7:7}
  # return last saturday's date
  return(last_week_date + timedelta(make_it_sunday_dict[last_week_date.isoweekday()]))


## Drop and re-create the table is exists in Databrixks hive_metastore
def create_hive_metastore_table(spark, database, table, location):
  # Drop the table if exists
  spark.sql("DROP TABLE IF EXISTS {db}.{table}".format(db=database,table=table))
  # Create table
  spark.sql("CREATE TABLE {db}.{table} USING DELTA LOCATION '{location}'"
                           .format(location=location, db=database,table=table))

## Create UT test result DataFrame
def create_test_case_df(spark, table_name:str, test_result:str, test_case_name:str, execution_date_str:str) -> DataFrame:
  test_case_df = spark.createDataFrame(
    [
        (table_name,test_result,test_case_name,execution_date_str)  
    ],
    StructType(
        [
            StructField("table_name", StringType(), True),
            StructField("test_result", StringType(), True),
            StructField("test_case_name", StringType(), True),
            StructField("execution_date", StringType(), True)
        ]
    )
  )
  return test_case_df


## Function to send a notification to the microsoft teams using Webhook
def webhook_call(webhook_url:str, content:str, title:str , notebookName:str , color:str="Ff0000") -> int:
    """
      - Send a teams notification to the desired webhook_url
      - Returns the status code of the HTTP request
        - webhook_url : the url you got from the teams webhook configuration
        - content : your formatted notification content
        - title : the message that'll be displayed as title, and on phone notifications
        - color (optional) : hexadecimal code of the notification's top line color, default corresponds to black
    """
    response = requests.post(
        url=webhook_url,
        headers={"Content-Type": "application/json"},
        json={
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": title,
            "sections": [{
                "activityTitle": title,
                "facts":[{
                    "name": "Project name",
                    "value": "Customer Dashboard Phase 2"
                },{
                    "name":"Notebook Name",
                    "value": notebookName
                },{
                    "name":"Message",
                    "value": content
                },{
                    "name":"Notification Time:",
                    "value": str(get_now_pst())
                }]
            }]
        }
    )
    return response.status_code # Should be 200
  
  
