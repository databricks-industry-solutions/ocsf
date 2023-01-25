-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # DLT query for mapping zeek http logs to OCSF http_activity
-- MAGIC 
-- MAGIC Please change the database/schema name of the input table in the DLT query below

-- COMMAND ----------

create streaming live table http_activity
as
select 
  99 as activity_id,
  4 as category_uid,
  4002 as class_uid,
  ts::timestamp as time,
  99 as severity_id,
  400299 as type_uid,
  named_struct(
    'hostname', host,
    'ip', `id.resp_h`,
    'port', `id.resp_p`
  ) as dst_endpoint,
  named_struct(
    'http_method', method,
    'user_agent', user_agent,
    'version', `version`,
    'url', uri
  ) as http_request,
  named_struct(
    'code', status_code
  ) as http_response,
  named_struct(
    'product', 'zeek',
    'version', '1.0.0'
  ) as metadata,
  named_struct (
    'ip', `id.orig_h`,
    'port', `id.orig_p`
  ) as src_endpoint
from stream(ocsf_lipyeow_lim.http);
