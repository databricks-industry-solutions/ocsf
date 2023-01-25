# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load sample Zeek data sets for dev/debug
# MAGIC 
# MAGIC The L7 log data set is extracted from MACCDC pcaps using zeek.

# COMMAND ----------

# MAGIC %run ../config/notebook_config

# COMMAND ----------

sql_list=[
  f"DROP SCHEMA IF EXISTS {getParam('db')} CASCADE",
  f"CREATE DATABASE IF NOT EXISTS {getParam('db')} LOCATION '{getParam('data_path')}'",
  f"USE SCHEMA {getParam('db')}"
]

for s in sql_list:
  print(s)
  spark.sql(s)

# COMMAND ----------

# DBTITLE 1,Download http data
# MAGIC %sh
# MAGIC 
# MAGIC mkdir /dbfs/tmp/ocsf
# MAGIC mkdir /dbfs/tmp/ocsf/maccdc2012
# MAGIC cd /dbfs/tmp/ocsf/maccdc2012
# MAGIC pwd
# MAGIC echo "Removing all files"
# MAGIC rm -rf *
# MAGIC 
# MAGIC for idx in 00;
# MAGIC do
# MAGIC   mkdir $idx
# MAGIC   cd $idx
# MAGIC   pwd 
# MAGIC   for fname in http.log.gz dns.log.gz;
# MAGIC   do
# MAGIC     dlpath="https://raw.githubusercontent.com/lipyeow-lim/security-datasets01/main/maccdc-2012/$idx/$fname"
# MAGIC     wget $dlpath
# MAGIC     gzip -d $fname
# MAGIC   done
# MAGIC   cd ..
# MAGIC done
# MAGIC 
# MAGIC ls -lR

# COMMAND ----------

# DBTITLE 1,Load dns & http data
from pyspark.sql.functions import col
from pyspark.sql.types import *
download_path=f"{getParam('download_path')}/maccdc2012"
tables=["http", "dns"]
folders=["00"]
# Load the zeek logs extracted from pcaps
for t in tables:
  tb = f"{getParam('db')}.{t}"
  for f in folders:
    jsonfile=f"{download_path}/{f}/{t}.log"
    print(f"Loading {jsonfile} into {tb} ...")
    df = spark.read.format("json").load(jsonfile).withColumn("eventDate", col("ts").cast("Timestamp").cast("Date"))
    df.write.option("mergeSchema", "true").partitionBy("eventDate").mode("append").saveAsTable(tb)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 'http' as table_name, count(*), min(eventDate), max(eventDate)
# MAGIC from http
# MAGIC union all
# MAGIC select 'dns' as table_name, count(*), min(eventDate), max(eventDate)
# MAGIC from dns

# COMMAND ----------


