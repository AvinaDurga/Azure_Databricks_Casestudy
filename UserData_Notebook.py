# Databricks notebook source
#widget definition
dbutils.widgets.text("BatchId","101")
dbutils.widgets.text("JobId","1")
dbutils.widgets.text("Zone_Name","STG2_SER")

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

#Get data from RAW Layer
userData_Raw = spark.read.parquet("abfss://userdata@storageaccount2015.dfs.core.windows.net/userdata_RAW/y=2021/m=07/d=20/")

# COMMAND ----------

#Creating delta table in ADLS
userData_Raw.write.mode("overwrite").format("delta").save("abfss://userdata@storageaccount2015.dfs.core.windows.net/userdata_STG1/y=2021/m=07/d=20/stg1_userdata")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT Exists default.STG1_USER_DATA USING DELTA LOCATION 'abfss://userdata@storageaccount2015.dfs.core.windows.net/userdata_STG1/y=2021/m=07/d=20/stg1_userdata'

# COMMAND ----------

#retriving transformation query
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

tfm_query = spark.sql("select query from controlTable where batchid=" + batchid + " and jobId = " + jobId + " and Zone_name ='" + Zone_Name + "' ").collect()[0][0];
print(tfm_query)

# COMMAND ----------

# load data into sql database
userData_SER = spark.sql(tfm_query)
userData_SER = DataFrameWriter(userData_SER)
userData_SER.jdbc(url=jdbc_url, table= "serving.SRV_userdata", mode ="overwrite", properties=connection_properties)
