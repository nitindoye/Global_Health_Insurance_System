# Databricks notebook source
# MAGIC %md
# MAGIC **This Notebook will create a metadata table for dynamic file ingestion process 
# MAGIC into bronze layer**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# create external volume for each country
# workspace URLs
dev_workspaceUrl = "dbc-fc59c034-3c4b.cloud.databricks.com"
qa_workspaceUrl = "adb-7405614438398437.18.azuredatabricks.net"
prod_workspaceUrl = "adb-7405614438398437.19.azuredatabricks.net"

# storage accounts
dev_storageAccount = "globalbupa-health-696645005482-ap-south-1-an"
qa_storageAccount = "adlsglobalbupaqa"
prod_storageAccount = "adlsglobalbupaprod"

# Detect current workspace
cur_workspaceUrl = spark.conf.get("spark.databricks.workspaceUrl")

# Resolve environment-specific setting
if cur_workspaceUrl == dev_workspaceUrl:
  storageAccount = dev_storageAccount
  catalog_name = "global-pds-dev"
  schema_name = "globalbupa"
  s3_root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"

elif cur_workspaceUrl == qa_workspaceUrl:
  storageAccount = qa_storageAccount
  catalog_name = "global-pds-qa"
  schema_name = "globalbupa"
  s3_root_path = f"abfss://globalbupa@{storageAccount}.dfs.core.windows.net"
elif cur_workspaceUrl == prod_workspaceUrl:
  storageAccount = prod_storageAccount
  catalog_name = "globalbupa_prod"
  schema_name = "globalbupa"
  s3_root_path = f"abfss://globalbupa@{storageAccount}.dfs.core.windows.net"
else:
  raise ValueError(f"Unknown workspace: {cur_workspaceUrl}")

print(f"Current workspace: {cur_workspaceUrl}")
print(f"Storage account: {storageAccount}")
print(f"Catalog name: {catalog_name}")
print(f"s3 root path: {s3_root_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `global-pds-dev`.bronze.file_ingestion_metadata (
# MAGIC     file_pattern STRING,
# MAGIC     table_name STRING,
# MAGIC     sensitive_cols STRING,
# MAGIC     active_flag STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://globalbupa-health-696645005482-ap-south-1-an/glob_health/globalbupa_customers/Bronze/file_ingestion_metadata'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into `global-pds-dev`.bronze.file_ingestion_metadata values (
# MAGIC     'insurance_customer','insurance_customer', 'FirstName,LastName,Email,Phone','Y');
# MAGIC
# MAGIC insert into `global-pds-dev`.bronze.file_ingestion_metadata values (
# MAGIC     'policies_master','policies_master', '','Y');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `global-pds-dev`.bronze.file_ingestion_metadata

# COMMAND ----------


