# Databricks notebook source
import os
import json
import re

cfg={}
cfg["useremail"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
cfg["username"] = cfg["useremail"].split('@')[0]
cfg["username_sql_compatible"] = re.sub('\W', '_', cfg["username"])
cfg["id"] = "ocsf"
cfg["db"] = f"{cfg['id']}_{cfg['username_sql_compatible']}"
cfg["data_path"] = f"/tmp/{cfg['username_sql_compatible']}/{cfg['id']}/"
cfg["download_path"] = f"/tmp/{cfg['id']}"

if "getParam" not in vars():
  def getParam(param):
    assert param in cfg
    return cfg[param]

print(json.dumps(cfg, indent=2))


# COMMAND ----------


