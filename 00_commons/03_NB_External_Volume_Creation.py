# Databricks notebook source
# MAGIC %md
# MAGIC #### This NOTEBOOK will create external volume for multiple countries

# COMMAND ----------

# DBTITLE 1,IMPORTS
#IMPORTS
from pyspark.sql import functions as F
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,setting up envronment variables
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
  root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"

elif cur_workspaceUrl == qa_workspaceUrl:
  storageAccount = qa_storageAccount
  catalog_name = "global-pds-qa"
  schema_name = "globalbupa"
  root_path = f"abfss://globalbupa@{storageAccount}.dfs.core.windows.net"
elif cur_workspaceUrl == prod_workspaceUrl:
  storageAccount = prod_storageAccount
  catalog_name = "globalbupa_prod"
  schema_name = "globalbupa"
  root_path = f"abfss://globalbupa@{storageAccount}.dfs.core.windows.net"
else:
  raise ValueError(f"Unknown workspace: {cur_workspaceUrl}")

print(f"Current workspace: {cur_workspaceUrl}")
print(f"Storage account: {storageAccount}")
print(f"Catalog name: {catalog_name}")
print(f"root path: {root_path}")

# COMMAND ----------

# creating Volume for Each Country
countries = [
    "Austria",
    "Denmark",
    "Netherlands",
    "France",
    "Germany",
    "Ireland",
    "Italy",
    "Spain",
    "Sweden",
    "UnitedKingdom",
    "Estonia",
    "Finland",
    "Norway",
    "Poland",
    "Belgium",
    "Greece",
    "CzechRepublic",
    "Latvia"
             
]


for country in countries:
    volume_name = f"globbupa_{country.lower()}"
    external_path = f"{root_path}/globalcountries/{country.lower()}"

    spark.sql(f"""
              CREATE EXTERNAL VOLUME IF NOT EXISTS `{catalog_name}`.{schema_name}.{volume_name}
              LOCATION '{external_path}'
              COMMENT 'External Volume for {country}'
            """)
    print(f"External volume for {country} is created")

# COMMAND ----------

# creating landingzone volume
# workspace URLs
dev_workspaceUrl = "adb-7405614438398437.17.azuredatabricks.net"
qa_workspaceUrl = "adb-7405614438398437.18.azuredatabricks.net"
prod_workspaceUrl = "adb-7405614438398437.19.azuredatabricks.net"

# storage accounts
dev_storageAccount = "adlsglobalbupa"
qa_storageAccount = "adlsglobalbupa"
prod_storageAccount = "adlsglobalbupaprod"

# Detect current workspace
cur_workspaceUrl = spark.conf.get("spark.databricks.workspaceUrl")

# Resolve environment-specific setting
if cur_workspaceUrl == dev_workspaceUrl:
  storageAccount = dev_storageAccount
  catalog_name = "globalbupa_dev"
  landing_path = f"abfss://landingzone@{storageAccount}.dfs.core.windows.net/"

elif cur_workspaceUrl == qa_workspaceUrl:
  storageAccount = qa_storageAccount
  catalog_name = "globalbupa_qa"
  landing_path = f"abfss://landingzone@{storageAccount}.dfs.core.windows.net/"
elif cur_workspaceUrl == prod_workspaceUrl:
  storageAccount = prod_storageAccount
  catalog_name = "globalbupa_prod"
  landing_path = f"abfss://landingzone@{storageAccount}.dfs.core.windows.net/"
else:
  raise ValueError(f"Unknown workspace: {cur_workspaceUrl}")

vol_name = "globbupa_landing_vol"
external_path = f"{landing_path}"
spark.sql("""
          CREATE EXTERNAL VOLUME IF NOT EXISIS {catalog_name}.{vol_name}
          LOCATION '{external_path}'
          COMMENT 'External Volume for landingzone'""")
print("Landing zone volume created")

