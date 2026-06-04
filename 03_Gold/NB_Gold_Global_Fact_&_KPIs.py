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
    schema_name = "gold"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"


  elif cur_workspaceUrl == qa_workspaceUrl:
    storageAccount = qa_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "gold"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"

  elif cur_workspaceUrl == prod_workspaceUrl:
    storageAccount = prod_storageAccount
    catalog_name = "global-pds-dev"
    schema_name = "gold"
    root_path = f"s3://globalbupa-health-696645005482-ap-south-1-an/glob_health/"

  else:
    raise ValueError(f"Unknown workspace: {cur_workspaceUrl}")

  return {
    "catalog_name": catalog_name,
    "adls_root_path": adls_root_path,
    "container":container,
    "schema_name":schema_name
  }
   



# COMMAND ----------

# env Variables 
env_config = get_path()
catalog_name = env_config["catalog_name"]
print(f"catalog_name: {catalog_name}")
adls_root_path = env_config["adls_root_path"]
print(f"root_path: {root_path}")
container = env_config["container"]
schema_name = env_config["schema_name"]
print(f"schema name:{schema_name}")

# COMMAND ----------

# DBTITLE 1,FACT_POLICY
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.fact_policy
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://globalbupa@adlsglobalbupa.dfs.core.windows.net/globalbupa_customers/Gold/fact_policy'
# MAGIC  AS
# MAGIC SELECT
# MAGIC   dc.CustomerKey,
# MAGIC   dv.VehicleKey,
# MAGIC   sp.PolicyNumber,
# MAGIC   sp.PolicyType,
# MAGIC   sp.AnnualPremium_INR,
# MAGIC   sp.CoverageAmount_INR,
# MAGIC   sp.PolicyStartDate,
# MAGIC   sp.PolicyEndDate
# MAGIC FROM `global-pds-dev`.silver.policies sp
# MAGIC JOIN `global-pds-dev`.gold.dim_customer dc
# MAGIC   ON sp.CustomerID = dc.CustomerID AND dc.IsCurrent = true
# MAGIC LEFT JOIN `global-pds-dev`.gold.dim_vehicle dv
# MAGIC   ON sp.PolicyNumber = dv.PolicyNumber AND dv.IsCurrent = true;

# COMMAND ----------

# DBTITLE 1,Fact_Claims
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.fact_claims
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://globalbupa@adlsglobalbupa.dfs.core.windows.net/globalbupa_customers/Gold/fact_claims'
# MAGIC AS
# MAGIC SELECT
# MAGIC   c.ClaimID,
# MAGIC   dc.CustomerKey,
# MAGIC   dv.VehicleKey,
# MAGIC   c.ClaimDate,
# MAGIC   c.ClaimAmount_INR,
# MAGIC   c.SettledAmount_INR
# MAGIC FROM `global-pds-dev`.silver.claims c
# MAGIC JOIN `global-pds-dev`.gold.dim_customer dc
# MAGIC   ON c.CustomerID = dc.CustomerID AND dc.IsCurrent = true
# MAGIC LEFT JOIN `global-pds-dev`.gold.dim_vehicle dv
# MAGIC   ON c.PolicyNumber = dv.PolicyNumber AND dv.IsCurrent = true;

# COMMAND ----------

# DBTITLE 1,Fact_Payments
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.fact_payments
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://globalbupa@adlsglobalbupa.dfs.core.windows.net/globalbupa_customers/Gold/fact_payments' 
# MAGIC AS
# MAGIC SELECT
# MAGIC   p.TransactionID,
# MAGIC   dc.CustomerKey,
# MAGIC   p.Amount_INR,
# MAGIC   p.PaidDate
# MAGIC FROM `global-pds-dev`.silver.payments p
# MAGIC JOIN `global-pds-dev`.gold.dim_customer dc
# MAGIC   ON p.CustomerID = dc.CustomerID AND dc.IsCurrent = true;

# COMMAND ----------

# MAGIC %md
# MAGIC ### BUSINESS KPIs

# COMMAND ----------

# DBTITLE 1,TOTAL PORTFOLIO SUMMARY
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_portfolio_overview AS
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT PolicyNumber) AS TotalPolicies,
# MAGIC   SUM(AnnualPremium_INR) AS TotalPremium,
# MAGIC   SUM(CoverageAmount_INR) AS TotalCoverage
# MAGIC FROM `global-pds-dev`.gold.fact_policy;
# MAGIC

# COMMAND ----------

# DBTITLE 1,PREMIUM BY POLICY TYPE
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_premium_by_policy_type AS
# MAGIC SELECT
# MAGIC   PolicyType,
# MAGIC   COUNT(*) AS PolicyCount,
# MAGIC   SUM(AnnualPremium_INR) AS TotalPremium
# MAGIC FROM `global-pds-dev`.gold.fact_policy
# MAGIC GROUP BY PolicyType;
# MAGIC

# COMMAND ----------

# DBTITLE 1,CLAIMS SUMMARY
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_claims_summary AS
# MAGIC SELECT
# MAGIC   COUNT(ClaimID) AS TotalClaims,
# MAGIC   SUM(ClaimAmount_INR) AS ClaimedAmount,
# MAGIC   SUM(SettledAmount_INR) AS SettledAmount
# MAGIC FROM `global-pds-dev`.gold.fact_claims;
# MAGIC

# COMMAND ----------

# DBTITLE 1,LOSS RATIO
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_loss_ratio_overall AS
# MAGIC SELECT
# MAGIC   SUM(fc.SettledAmount_INR) / SUM(fp.AnnualPremium_INR) AS LossRatio
# MAGIC FROM `global-pds-dev`.gold.fact_claims fc
# MAGIC JOIN `global-pds-dev`.gold.fact_policy fp
# MAGIC   ON fc.CustomerKey = fp.CustomerKey;
# MAGIC

# COMMAND ----------

# DBTITLE 1,LOSS RATIO BY POLICY TYPE
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_claim_frequency AS
# MAGIC SELECT
# MAGIC   fp.PolicyType,
# MAGIC   try_divide(COUNT(fc.ClaimID), COUNT(DISTINCT fp.VehicleKey)) AS ClaimFrequency
# MAGIC FROM `global-pds-dev`.gold.fact_policy fp
# MAGIC LEFT JOIN `global-pds-dev`.gold.fact_claims fc
# MAGIC   ON fp.VehicleKey = fc.VehicleKey
# MAGIC GROUP BY fp.PolicyType;

# COMMAND ----------

# DBTITLE 1,CUSTOMER DEMOGRAPHICS
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_customer_distribution AS
# MAGIC SELECT
# MAGIC   dc.City,
# MAGIC   dc.State,
# MAGIC   dc.Country,
# MAGIC   COUNT(DISTINCT dc.CustomerKey) AS CustomerCount
# MAGIC FROM `global-pds-dev`.gold.dim_customer dc
# MAGIC WHERE dc.IsCurrent = true
# MAGIC GROUP BY dc.City, dc.State, dc.Country;
# MAGIC

# COMMAND ----------

# DBTITLE 1,CUSTOMER CLAIM BEHAVIOR
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_customer_claim_behavior AS
# MAGIC SELECT
# MAGIC   dc.CustomerKey,
# MAGIC   COUNT(fc.ClaimID) AS ClaimCount,
# MAGIC   SUM(fc.SettledAmount_INR) AS TotalClaims
# MAGIC FROM `global-pds-dev`.gold.fact_claims fc
# MAGIC JOIN `global-pds-dev`.gold.dim_customer dc
# MAGIC   ON fc.CustomerKey = dc.CustomerKey
# MAGIC GROUP BY dc.CustomerKey;
# MAGIC

# COMMAND ----------

# DBTITLE 1,POLICY PAYMENT FREQUENCY MIX
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_payment_frequency_mix AS
# MAGIC SELECT
# MAGIC   PaymentFrequency,
# MAGIC   COUNT(*) AS PolicyCount
# MAGIC FROM `global-pds-dev`.silver.policies
# MAGIC GROUP BY PaymentFrequency;
# MAGIC

# COMMAND ----------

# DBTITLE 1,POLICY LIFECYCLE (NEW vs EXPIRING)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_policy_lifecycle AS
# MAGIC SELECT
# MAGIC   year(PolicyStartDate) AS Year,
# MAGIC   COUNT(*) AS NewPolicies
# MAGIC FROM `global-pds-dev`.gold.fact_policy
# MAGIC GROUP BY year(PolicyStartDate);
# MAGIC

# COMMAND ----------

# DBTITLE 1,AGENT PERFORMANCE
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_agent_performance AS
# MAGIC SELECT
# MAGIC   AgentID,
# MAGIC   AgentName,
# MAGIC   COUNT(*) AS PoliciesSold,
# MAGIC   SUM(AnnualPremium_INR) AS TotalPremium
# MAGIC FROM `global-pds-dev`.silver.policies
# MAGIC GROUP BY AgentID, AgentName;
# MAGIC

# COMMAND ----------

# DBTITLE 1,AVERAGE PREMIUM PER CUSTOMER
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_avg_premium_per_customer AS
# MAGIC SELECT
# MAGIC   dc.CustomerKey,
# MAGIC   AVG(fp.AnnualPremium_INR) AS AvgPremium
# MAGIC FROM `global-pds-dev`.gold.fact_policy fp
# MAGIC JOIN `global-pds-dev`.gold.dim_customer dc
# MAGIC   ON fp.CustomerKey = dc.CustomerKey
# MAGIC GROUP BY dc.CustomerKey;
# MAGIC

# COMMAND ----------

# DBTITLE 1,PREMIUM COLLECTION PERFORMANCE
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_payment_collection AS
# MAGIC SELECT
# MAGIC   COUNT(TransactionID) AS TotalPayments,
# MAGIC   SUM(Amount_INR) AS TotalCollected,
# MAGIC   AVG(Amount_INR) AS AvgPayment
# MAGIC FROM `global-pds-dev`.gold.fact_payments;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Claim Loss Ratio
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `global-pds-dev`.gold.kpi_loss_ratio
# MAGIC LOCATION 'abfss://globalbupa@adlsglobalbupa.dfs.core.windows.net/globalbupa_customers/Gold/kpi_loss_ratio'
# MAGIC  AS
# MAGIC SELECT
# MAGIC   dv.VehicleMake,
# MAGIC   SUM(fc.SettledAmount_INR) / SUM(fp.AnnualPremium_INR) AS LossRatio
# MAGIC FROM `global-pds-dev`.gold.fact_claims fc
# MAGIC JOIN `global-pds-dev`.gold.fact_policy fp ON fc.VehicleKey = fp.VehicleKey
# MAGIC JOIN `global-pds-dev`.gold.dim_vehicle dv ON fp.VehicleKey = dv.VehicleKey
# MAGIC GROUP BY dv.VehicleMake;
