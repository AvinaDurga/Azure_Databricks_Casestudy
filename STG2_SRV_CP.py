# Databricks notebook source
#widget definition
dbutils.widgets.text("BatchId","102")
dbutils.widgets.text("JobId","5")
dbutils.widgets.text("Zone_Name","STG2_SRV")

# COMMAND ----------

#widget parameters
batchid = dbutils.widgets.get("BatchId")
jobId = dbutils.widgets.get("JobId")
Zone_Name = dbutils.widgets.get("Zone_Name")

# COMMAND ----------

#Accessing storage account for USERDATA pipeline
accesskey = dbutils.secrets.get(scope="casestudykv",key="storageAccount2015AccessKey")
spark.conf.set(  "fs.azure.account.key.storageaccount2015.dfs.core.windows.net",accesskey)

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

# load data into sql database
if tfm_query!=[] :
  STG2Data_SRV = spark.sql(tfm_query[0][0])
  print(tfm_query[0][0])
  STG2Data_SRV = DataFrameWriter(STG2Data_SRV)
  STG2Data_SRV.jdbc(url=jdbc_url, table= "serving.SRV_contry_progress_report", mode ="overwrite", properties=connection_properties)

# COMMAND ----------


