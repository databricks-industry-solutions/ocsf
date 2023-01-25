# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # How to convert Zeek logs to OCSF
# MAGIC 
# MAGIC * Assume zeek logs are written as newline-delimited JSON into cloud storage (eg. AWS S3) and ingested in its raw format/schema into Delta lake. 
# MAGIC * This notebook is a developer environment for developing the mapping logic as SQL queries that are then converted to DLT or SQL views.

# COMMAND ----------

# MAGIC %run ../config/notebook_config

# COMMAND ----------

sqlstr=f"use schema {getParam('db')}"
spark.sql(sqlstr)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Dev queries for http logs
# MAGIC 
# MAGIC ## Mapping Zeek HTTP schema to OCSF `http_activity` Schema
# MAGIC 
# MAGIC For **schema-on-write**, convert the query into a Delta Live Table (DLT) job.
# MAGIC 
# MAGIC For **schema-on-read**, convert the query into a `create view` statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from http;

# COMMAND ----------

# DBTITLE 1,Basic SQL query that maps the zeek HTTP to OCSF http activity schema
# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   99 as activity_id,
# MAGIC   4 as category_uid,
# MAGIC   4002 as class_uid,
# MAGIC   ts::timestamp as time,
# MAGIC   99 as severity_id,
# MAGIC   400299 as type_uid,
# MAGIC   named_struct(
# MAGIC     'hostname', host,
# MAGIC     'ip', `id.resp_h`,
# MAGIC     'port', `id.resp_p`
# MAGIC   ) as dst_endpoint,
# MAGIC   named_struct(
# MAGIC     'http_method', method,
# MAGIC     'user_agent', user_agent,
# MAGIC     'version', `version`,
# MAGIC     'url', uri
# MAGIC   ) as http_request,
# MAGIC   named_struct(
# MAGIC     'code', status_code
# MAGIC   ) as http_response,
# MAGIC   named_struct(
# MAGIC     'product', 'zeek',
# MAGIC     'version', '1.0.0'
# MAGIC   ) as metadata,
# MAGIC   named_struct (
# MAGIC     'ip', `id.orig_h`,
# MAGIC     'port', `id.orig_p`
# MAGIC   ) as src_endpoint
# MAGIC from http;

# COMMAND ----------

# DBTITLE 1,Schema-on-read view for OCSF http_activity
# MAGIC %sql
# MAGIC 
# MAGIC create view if not exists v_http_activity
# MAGIC as
# MAGIC select 
# MAGIC   99 as activity_id,
# MAGIC   4 as category_uid,
# MAGIC   4002 as class_uid,
# MAGIC   ts::timestamp as time,
# MAGIC   99 as severity_id,
# MAGIC   400299 as type_uid,
# MAGIC   named_struct(
# MAGIC     'hostname', host,
# MAGIC     'ip', `id.resp_h`,
# MAGIC     'port', `id.resp_p`
# MAGIC   ) as dst_endpoint,
# MAGIC   named_struct(
# MAGIC     'http_method', method,
# MAGIC     'user_agent', user_agent,
# MAGIC     'version', `version`,
# MAGIC     'url', uri
# MAGIC   ) as http_request,
# MAGIC   named_struct(
# MAGIC     'code', status_code
# MAGIC   ) as http_response,
# MAGIC   named_struct(
# MAGIC     'product', 'zeek',
# MAGIC     'version', '1.0.0'
# MAGIC   ) as metadata,
# MAGIC   named_struct (
# MAGIC     'ip', `id.orig_h`,
# MAGIC     'port', `id.orig_p`
# MAGIC   ) as src_endpoint
# MAGIC from http;

# COMMAND ----------

# DBTITLE 1,Querying the OCSF view for http logs
# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from v_http_activity
# MAGIC where dst_endpoint.ip = '192.168.229.251' 
# MAGIC   and http_response.code = '200' 
# MAGIC   and http_request.http_method = 'GET'

# COMMAND ----------

# DBTITLE 1,Querying the OCSF table for http logs
# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from http_activity
# MAGIC where dst_endpoint.ip = '192.168.229.251' 
# MAGIC   and http_response.code = '200' 
# MAGIC   and http_request.http_method = 'GET'

# COMMAND ----------


