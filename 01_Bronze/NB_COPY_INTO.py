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

# Step 1: Read files
file_path = f"{adls_root_path}copy_files/"
files = [f for f in dbutils.fs.ls(file_path) if f.isFile()]

# Step 2: Helpers
def get_table_name(file_name):
    name = re.sub(r'\.[^.]+$', '', file_name)
    name = re.sub(r'([._-]?\d{2}[._-]\d{2}[._-]\d{4})$', '', name)
    name = f"globalbupa_{name}".lower()
    name = re.sub(r'[_-]+$', '', name)
    return name

def get_file_format(file_name):
    if file_name.lower().endswith(".csv"):
        return "CSV"
    elif file_name.lower().endswith(".json"):
        return "JSON"
    elif file_name.lower().endswith(".parquet"):
        return "PARQUET"
    else:
        return None

# Step 3: Process each file
for file in files:
    file_name = file.name
    file_full_path = file.path
    
    table = get_table_name(file_name)
    full_table_name = f"`{catalog_name}`.{schema_name}.{table}"
    table_path = f"{adls_root_path}global_customers/Bronze/{table}"

    file_format = get_file_format(file_name)

    if not file_format:
        print(f"Skipping unsupported file: {file_name}")
        continue

    print(f"\n🚀 Processing file: {file_name}")

    try:
        # ✅ Step 3A: CREATE TABLE (MOST IMPORTANT)
        if not spark.catalog.tableExists(full_table_name):
            print(f"Creating table: {full_table_name}")

            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_table_name}
                USING DELTA
                LOCATION '{table_path}'
            """)
        else:
            print(f"Table already exists: {full_table_name}")

        # ✅ Step 3B: COPY INTO
        if file_format == "CSV":
            copy_sql = f"""
            COPY INTO {full_table_name}
            FROM '{file_full_path}'
            FILEFORMAT = CSV
            FORMAT_OPTIONS (
                'header' = 'true',
                'inferSchema' = 'true'
            )
            COPY_OPTIONS ('mergeSchema' = 'true')
            """

        elif file_format == "JSON":
            copy_sql = f"""
            COPY INTO {full_table_name}
            FROM '{file_full_path}'
            FILEFORMAT = JSON
            COPY_OPTIONS ('mergeSchema' = 'true')
            """

        elif file_format == "PARQUET":
            copy_sql = f"""
            COPY INTO {full_table_name}
            FROM '{file_full_path}'
            FILEFORMAT = PARQUET
            COPY_OPTIONS ('mergeSchema' = 'true')
            """

        result_df = spark.sql(copy_sql)

        #  Step 3C: Metrics
        result_df.select(
            "num_affected_rows",
            "num_inserted_rows",
            "num_skipped_corrupt_files"
        ).show(truncate=False)

    except Exception as e:
        print(f"❌ Error processing file {file_name}: {str(e)}")
