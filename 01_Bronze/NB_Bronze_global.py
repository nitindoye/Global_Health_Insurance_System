# Databricks notebook source
# DBTITLE 1,Run helper notebook
# MAGIC %run /Workspace/Users/borkarshreya08@gmail.com/globalbupa/00_commons/01_NB_Helper_Global

# COMMAND ----------

# DBTITLE 1,Import
# imports
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Widgets
# widgets
dbutils.widgets.text("country","")
# get country from widget (passed from ADF Pipeline)
country = dbutils.widgets.get("country").lower()

# COMMAND ----------

# DBTITLE 1,get env variables
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
bronze_table = f"`{catalog_name}`.{schema_name}.insurance_customers"

# COMMAND ----------

# date setup
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
current_date = now.strftime("%Y-%m-%d")

# COMMAND ----------

# construct volume path
file_path = dbutils.fs.ls(f"/Volumes/{catalog_name}/globalbupa/globbupa_{country}/")[0].path
file_path = file_path.replace("dbfs:/", "/")
print(f"Path: {file_path}")

file_name = file_path.split("/")[-1]
print(f"file_name: {file_name}")


# COMMAND ----------

# DBTITLE 1,Read Files dynamically
# read different format files 
def read_file(file_path:str):
    lower_path = file_path.lower()
    if lower_path.endswith(".csv"):
        df = (spark.read
              .format("csv").
              option("header","true")
              .option("inferSchema","true")
              .load(file_path)
            )
    elif lower_path.endswith("xlsx") or lower_path.endswith("xls"):
        # if file_path.startswith("dbfs:/Volumes/"):
        #     file_paths = file_path.replace("dbfs:/", "/")
        df = (
        spark.read
            .format("excel")
            .option("headerRows", 1)
            .option("inferSchema", "true")
            .load(file_path)
        )

    elif lower_path.endswith(".json"):
        df = (spark.read
              .format("json")
              .option("header","true")
              .option("inferSchema","true")
              .option("multiline","true")
              .load(file_path)
            )
    
    elif lower_path.endswith(".parquet"):
        df = (spark.read
              .format("parquet")
              .option("header","true")
              .option("inferSchema","true")
              .load(file_path))
    else:
        raise ValueError(f"Unknown file type: {file_path}")
    return df

# create dataframe    
df = read_file(file_path)


# COMMAND ----------

# DBTITLE 1,Encryption
# encrypt columns
sensitive_cols = [
    "FirstName",
    "LastName",
    "Email",
    "Phone"
]
custdf = build_column_exprs(df,sensitive_cols,salt_key)
cust_df = df.select(*custdf)

#display(df)

cust_df = (cust_df.withColumn("country",lit(country.capitalize()))
           .withColumn("SnapshotDate",lit(current_date))
           .withColumn("FileName",lit(file_name))
               
           )
# add metadata columns to last           
meta_cols = ["country", "SnapshotDate","FileName"]
data_cols = []
for c in cust_df.columns:
    if c not in meta_cols:
        data_cols.append(c)

cust_df = cust_df.select(data_cols + meta_cols)


# COMMAND ----------

# DBTITLE 1,write to delta table
# write table to bronze delta table
(cust_df.write
 .format("delta")
 .mode("append")
 .option("mergeSchema", "true")
 .option("path", f"{adls_root_path}/globalbupa_customers/Bronze/insurance_customers/")
 .saveAsTable(bronze_table)
)

# COMMAND ----------

# move file from adls location to archieve folder
src_path = file_path
archive_path = f"{adls_root_path}/glob_archieve_files/yyyy={year}/mm={month}/dd={day}/"
print(f"Archive path: {archive_path}")

def move_file(src_path: str, archive_path: str):
    print("Starting file archieval process")
    try:
        dbutils.fs.mkdirs(archive_path)
        print("Archive Directory Ensured")
        # list Source files
        src_items = dbutils.fs.ls(src_path)
        print(f"Files found in source: {len(src_items)} ")

        moved_files = 0

        for item in src_items:
            if item.isFile():
                print(f"Moving File: {item.path}")
                dbutils.fs.mv(item.path, archive_path)
                moved_files += 1
        print(f"Files Archival Completed. Total files moved: {moved_files}")
    except Exception as e:
        print(f"Error While moving File: {str(e)}")

move_file(src_path, archive_path)


# COMMAND ----------

##############   END ----################################
