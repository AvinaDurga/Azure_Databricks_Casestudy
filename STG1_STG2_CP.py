# Databricks notebook source
#widget definition
dbutils.widgets.text("BatchId","102")
dbutils.widgets.text("JobId","4")
dbutils.widgets.text("Zone_Name","STG1_STG2")

# COMMAND ----------

#widget parameters
batchid = dbutils.widgets.get("BatchId")
jobId = dbutils.widgets.get("JobId")
Zone_Name = dbutils.widgets.get("Zone_Name")

# COMMAND ----------

#Accessing storage account 
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
print(tfm_query[0][0])

# COMMAND ----------

# Create STG1 Delta Table
#spark.sql("CREATE TABLE IF NOT Exists default.STG2_gdpdata USING DELTA LOCATION 'abfss://gdpdata@storageaccount2015.dfs.core.windows.net/gdpdata_STG1/y=2021/m=07/d=20/stg1_gdpdata'" )
container = "countryprogress"
if tfm_query!=[] :
  rawData_STG1 = spark.sql(tfm_query[0][0])
  print(tfm_query[0][0])
  deltapath= "abfss://" + container + "@storageaccount2015.dfs.core.windows.net/country_progress_report_STG2/y=2021/m=07/d=20/stg2_" +container
  rawData_STG1.write.mode("overwrite").format("delta").save(deltapath)
  spark.sql("CREATE TABLE IF NOT Exists default.STG2_country_progress_report USING DELTA LOCATION '"+deltapath+"'" )

# COMMAND ----------

dbutils.notebook.run('./STG2_SRV_CP',2000,{'BatchId': '102', 'JobId': '5', 'Zone_Name': 'STG2_SRV'})

# COMMAND ----------


