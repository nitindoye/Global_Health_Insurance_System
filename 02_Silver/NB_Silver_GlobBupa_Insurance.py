# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

def get_path(container:str="globalbupa"):
  # workspace URLs
  dev_workspaceUrl = "dbc-fc59c034-3c4b.cloud.databricks.com"
  qa_workspaceUrl = "adb-7405614438398437.18.azuredatabricks.net"
  prod_workspaceUrl = "adb-7405614438398437.19.azuredatabricks.net"

  # storage accounts
  dev_storageAccount = "globalbupa-health-696645005482-ap-south-1-an"
  qa_storageAccount = "globalbupastorageqa"
  prod_storageAccount = "globalbupastorageprod"

  # Detect current workspace
  cur_workspaceUrl = spark.conf.get("spark.databricks.workspaceUrl")

  # Resolve environment-specific setting
  if cur_workspaceUrl == dev_workspaceUrl:
    storageAccount = dev_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "silver"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"

  elif cur_workspaceUrl == qa_workspaceUrl:
    storageAccount = qa_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "silver"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"
  elif cur_workspaceUrl == prod_workspaceUrl:
    storageAccount = prod_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "silver"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"
  else:
    raise ValueError(f"Unknown workspace: {cur_workspaceUrl}")

  return {
    "catalog_name": catalog_name,
    "root_path": root_path,
    "container":container,
    "schema_name":schema_name
  }
   



# COMMAND ----------

# env Variables 
env_config = get_path()
catalog_name = env_config["catalog_name"]
print(f"catalog_name: {catalog_name}")
root_path = env_config["root_path"]
container = env_config["container"]
schema_name = env_config["schema_name"]
print(f"schema name:{schema_name}")

# COMMAND ----------

# DBTITLE 1,Customers
catalog_name = 'global-pds-dev'
customers_bronze = spark.read.table(f"`{catalog_name}`.bronze.insurance_customers")
silver_customers = (
    customers_bronze
      .withColumn("DOB", to_date("DOB"))
      .withColumn("Age", col("Age").cast("int"))
      .withColumn("Email", lower(col("Email")))
      .withColumn("Phone", regexp_replace("Phone", "\\D", ""))
)

(silver_customers.write
 .mode("overwrite")
 .format("delta")
 .option("path",f"{adls_root_path}/globalbupa_customers/Silver/customers")
 .saveAsTable(f"`{catalog_name}`.silver.customers")
 )


# COMMAND ----------

# DBTITLE 1,Policy Deduplication and Standardization
# null value handeling
policies_bronze = spark.read.table(f"`{catalog_name}`.bronze.policies_master")
df_policies = policies_bronze.fillna({
    "coverage_amount": 0,
    "annual_premium": 0,
    "policy_type": "UNKNOWN"
})

# duplicate value handeling
w = Window.partitionBy("PolicyNumber").orderBy(col("SnapshotDate").desc())

pol_silver = (
    policies_bronze.withColumn("rn", row_number().over(w))
       .filter("rn = 1")
       .drop("rn")
       .withColumn("SnapshotDate", to_date("SnapshotDate"))
       .withColumn("PolicyStartDate", to_date("PolicyStartDate"))
       .withColumn("PolicyEndDate", to_date("PolicyEndDate"))
       .withColumn("CoverageAmount_INR", col("CoverageAmount_INR").cast("double"))
       .withColumn("AnnualPremium_INR", col("AnnualPremium_INR").cast("double"))
)

(pol_silver.write
 .mode("overwrite")
 .format("delta")
 .option("path",f"{adls_root_path}/globalbupa_customers/Silver/Policies")
 .saveAsTable(f"`{catalog_name}`.silver.policies")
 )


# COMMAND ----------

# DBTITLE 1,Claim clean and Normalize
claims_bronze = spark.read.table(f"`{catalog_name}`.bronze.policy_claims")
claims_silver = (
    claims_bronze
      .withColumn("ClaimDate", to_date("ClaimDate"))
      .withColumn("SettlementDate", to_date("SettlementDate"))
      .withColumn("ClaimAmount_INR", col("ClaimAmount_INR").cast("double"))
      .withColumn("SettledAmount_INR", col("SettledAmount_INR").cast("double"))
)

(claims_silver.write
 .mode("overwrite")
 .format("delta")
 .option("path",f"{adls_root_path}/globalbupa_customers/Silver/Claims")
 .saveAsTable(f"`{catalog_name}`.silver.claims")
 )

# COMMAND ----------

#claims_silver.display()

# COMMAND ----------

# DBTITLE 1,Payments Normalise + Late Flag
payments_bronze = spark.read.table(f"`{catalog_name}`.bronze.policy_payments")

payments_silver = (
    payments_bronze
      .withColumn("ScheduledDate", to_date("ScheduledDate"))
      .withColumn("PaidDate", to_date("PaidDate"))
      .withColumn("Amount_INR", col("Amount_INR").cast("double"))
      .withColumn("IsLate",
          when(col("PaidDate") > col("ScheduledDate"), 1).otherwise(0)
      )
)
# write to delta
(payments_silver.write
 .mode("overwrite")
 .format("delta")
 .option("path",f"{adls_root_path}/globalbupa_customers/Silver/payments")
 .saveAsTable(f"`{catalog_name}`.silver.payments")
 )
