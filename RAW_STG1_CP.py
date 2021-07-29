# Databricks notebook source
#widget definition
dbutils.widgets.text("BatchId","102")
dbutils.widgets.text("JobId","1")
dbutils.widgets.text("Zone_Name","RAW_STG1")
dbutils.widgets.text("container","gdpdata")

# COMMAND ----------

#widget parameters
batchid = dbutils.widgets.get("BatchId")
jobId = dbutils.widgets.get("JobId")
Zone_Name = dbutils.widgets.get("Zone_Name")
container = dbutils.widgets.get("container")

# COMMAND ----------

#Accessing storage account 
accesskey = dbutils.secrets.get(scope="casestudykv",key="storageAccount2015AccessKey")
spark.conf.set(  "fs.azure.account.key.storageaccount2015.dfs.core.windows.net",accesskey)

# COMMAND ----------

#Get data from RAW Layer
Raw_Data = spark.read.parquet("abfss://" + container + "@storageaccount2015.dfs.core.windows.net/" + container + "_RAW/y=2021/m=07/d=20/")
Raw_Data.createOrReplaceTempView("Raw_Table");

# COMMAND ----------

#retriving transformation query for STG1
from pyspark.sql import *
import pandas
jdbc_url = "jdbc:sqlserver://sql-db-demo.database.windows.net:1433;database=sql-demo1"
Password = dbutils.secrets.get(scope="casestudykv",key="sqldbPassword")
connection_properties = {
  "user" : "kanagadurga",
  "password" : Password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

control_table = spark.read.jdbc(jdbc_url, table = "metaData.controlTable", properties = connection_properties)
control_table.createOrReplaceTempView("controlTable");
tfm_query = spark.sql("select query from controlTable where batchid=" + batchid + " and jobId = " + jobId + " and Zone_name ='" + Zone_Name + "' ").collect();  

# COMMAND ----------

# Create STG1 Delta Table
if tfm_query!=[] :
  rawData_STG1 = spark.sql(tfm_query[0][0])
  print(tfm_query[0][0])
  deltapath= "abfss://" + container + "@storageaccount2015.dfs.core.windows.net/" + container + "_STG1/y=2021/m=07/d=20/stg1_"+ container
  rawData_STG1.write.mode("overwrite").format("delta").save(deltapath)
  spark.sql("CREATE TABLE IF NOT Exists default.STG1_" + container + " USING DELTA LOCATION '" + deltapath + "'" )
