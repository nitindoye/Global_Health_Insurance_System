# Databricks notebook source
# MAGIC %run /Workspace/Users/nitindoye470@gmail.com/Global_Health_Insurance_System/00_commons/01_NB_Helper_Global

# COMMAND ----------

# imports
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import *

# COMMAND ----------

# widgets
dbutils.widgets.text("country","")
# get country from widget (passed from ADF Pipeline)
country = dbutils.widgets.get("country").lower()

# COMMAND ----------

def get_bronze_path(container:str="globalbupa"):
  # workspace URLs
  dev_workspaceUrl = "dbc-fc59c034-3c4b.cloud.databricks.com"
  qa_workspaceUrl = "adb-7405614438398437.18.azuredatabricks.net"
  prod_workspaceUrl = "adb-7405614438398437.19.azuredatabricks.net"

  # storage accounts
  dev_storageAccount = "globalbupa-health-696645005482-ap-south-1-an"
  qa_storageAccount = "adlsglobalbupastorageqa"
  prod_storageAccount = "adlsglobalbupastorageprod"

  # Detect current workspace
  cur_workspaceUrl = spark.conf.get("spark.databricks.workspaceUrl")

  # Resolve environment-specific setting
  if cur_workspaceUrl == dev_workspaceUrl:
    storageAccount = dev_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "bronze"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"

  elif cur_workspaceUrl == qa_workspaceUrl:
    storageAccount = qa_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "bronze"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"
  elif cur_workspaceUrl == prod_workspaceUrl:
    storageAccount = prod_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "bronze"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"
  else:
    raise ValueError(f"Unknown workspace: {cur_workspaceUrl}")

  return {
    "catalog_name": catalog_name,
    "s3_root_path": s3_root_path,
    "container":container,
    "schema_name":schema_name
  }
   
# env Variables 
env_config = get_bronze_path()
catalog_name = env_config["catalog_name"]
print(catalog_name)
root_path = env_config["root_path"]
container = env_config["container"]
schema_name = env_config["schema_name"]
print(schema_name)

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

config_df = spark.table("`global-pds-dev`.bronze.file_ingestion_metadata").filter("active_flag = 'Y'")
config = config_df.filter(
        F.lit(file_name).contains(F.col("file_pattern"))
    ).collect()

if not config:
        print(f"No config found for {file_name}")
config_row = config[0]

table_name = config_row["table_name"]
sensitive_cols = config_row["sensitive_cols"]

bronze_table = f"`{catalog_name}`.{schema_name}.{table_name}"
print(bronze_table)


# COMMAND ----------

# read different format files 
def read_file(file_path:str):
    lower_path = file_path.lower()
    if lower_path.endswith(".csv"):
        df = (spark.read
              .format("csv")
              .option("header","true")
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

if sensitive_cols is None or sensitive_cols == "":
    print("No sensitive columns → Skipping encryption")
    final_df = df
else:
    sensitive_cols_list = [c.strip() for c in sensitive_cols.split(",") if c.strip()]

    if not sensitive_cols_list:
        print("Empty sensitive column list → Skipping encryption")
        final_df = df
    else:
        print(f"Applying encryption on: {sensitive_cols_list}")
        column_exprs = build_column_exprs(df, sensitive_cols_list, salt_key)
        final_df = df.select(*column_exprs)

# COMMAND ----------

final_df = (final_df.withColumn("country",lit(country.capitalize()))
           .withColumn("SnapshotDate",lit(current_date))
           .withColumn("FileName",lit(file_name))
               
           )
# add metadata columns to last           
meta_cols = ["country", "SnapshotDate","FileName"]
data_cols = []
for c in final_df.columns:
    if c not in meta_cols:
        data_cols.append(c)

final_df = final_df.select(data_cols + meta_cols)

# COMMAND ----------

# write table to bronze delta table
(final_df.write
 .format("delta")
 .mode("append")
 .option("mergeSchema", "true")
 .option("path", f"{s3_root_path}/globalbupa_customers/Bronze/{table_name}/")
 .saveAsTable(bronze_table)
)

# COMMAND ----------

# move file from adls location to archieve folder
src_path = file_path
archive_path = f"{s3_root_path}/glob_archieve_files/yyyy={year}/mm={month}/dd={day}/"
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



# COMMAND ----------


