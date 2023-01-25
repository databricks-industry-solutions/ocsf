# Databricks notebook source
# MAGIC %run ./config/notebook_config

# COMMAND ----------

sql_list=[
  f"DROP SCHEMA IF EXISTS {getParam('db')} CASCADE",
  f"CREATE DATABASE IF NOT EXISTS {getParam('db')} LOCATION '{getParam('data_path')}'",
]

for s in sql_list:
  print(s)
  spark.sql(s)
