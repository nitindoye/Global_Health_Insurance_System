# Databricks notebook source
# MAGIC %run /Workspace/Users/borkarshreya08@gmail.com/globalbupa/00_commons/01_NB_Helper_Global

# COMMAND ----------

# imports
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import *
import re

# COMMAND ----------

def get_bronze_path(container:str="globalbupa"):
  # workspace URLs
  dev_workspaceUrl = "adb-7405610477490834.14.azuredatabricks.net"
  qa_workspaceUrl = "adb-7405614438398437.18.azuredatabricks.net"
  prod_workspaceUrl = "adb-7405614438398437.19.azuredatabricks.net"

  # storage accounts
  dev_storageAccount = "adlsglobalbupa"
  qa_storageAccount = "adlsglobalbupastorageqa"
  prod_storageAccount = "adlsglobalbupastorageprod"

  # Detect current workspace
  cur_workspaceUrl = spark.conf.get("spark.databricks.workspaceUrl")

  # Resolve environment-specific setting
  if cur_workspaceUrl == dev_workspaceUrl:
    storageAccount = dev_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "bronze"
    adls_root_path = f"abfss://globalbupa@{storageAccount}.dfs.core.windows.net/"

  elif cur_workspaceUrl == qa_workspaceUrl:
    storageAccount = qa_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "bronze"
    adls_root_path = f"abfss://globalbupa@{storageAccount}.dfs.core.windows.net/"
  elif cur_workspaceUrl == prod_workspaceUrl:
    storageAccount = prod_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "bronze"
    adls_root_path = f"abfss://globalbupa@{storageAccount}.dfs.core.windows.net/"
  else:
    raise ValueError(f"Unknown workspace: {cur_workspaceUrl}")

  return {
    "catalog_name": catalog_name,
    "adls_root_path": adls_root_path,
    "container":container,
    "schema_name":schema_name
  }
   
# env Variables 
env_config = get_bronze_path()
catalog_name = env_config["catalog_name"]
print(catalog_name)
adls_root_path = env_config["adls_root_path"]
container = env_config["container"]
schema_name = env_config["schema_name"]
print(schema_name)
# bronze_table = f"`{catalog_name}`.{schema_name}.claim"

# COMMAND ----------

file_path = f"{adls_root_path}/copy_files/"
files = dbutils.fs.ls(file_path)
for file in files:
    f = file.name


# COMMAND ----------



# COMMAND ----------

def getting_table_name():
    table = []
    for file in files:
        f = file.name

        t = re.sub(r'([._-]?\d{2}[_-]\d{2}[_-]\d{4})$', '',
                re.sub(r'\.[^.]+$', '', f'globbupa_{f}'))
        table.append(t)

        
    return {"table_name":table}

tables = getting_table_name()
tbl_names = tables["table_name"]

# COMMAND ----------

tgt_tables = [f"`{catalog_name}`.bronze.{t}" for t in tbl_names]
print(tgt_tables)

# COMMAND ----------

# create external table 
for f in tgt_tables:
    if not spark.catalog.tableExists(f):
        spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {f}
                USING DELTA 
                LOCATION "{adls_root_path}/global_customers/Bronze/{f.split('.')[-1]}"
                """
                )
    else:
        print(f"Table {f} already exists.")

# COMMAND ----------

# copy into

