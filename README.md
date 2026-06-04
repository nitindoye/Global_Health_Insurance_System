# 🌍 Global Health Insurance Data Pipeline (Databricks)

## 📌 Project Overview
This project implements an **end-to-end data engineering pipeline** for a global health insurance company using **Azure Databricks** and **Medallion Architecture (Bronze, Silver, Gold)**.

The pipeline ingests **multi-country insurance data** in various formats, processes it securely, and makes it ready for **analytics, reporting, and compliance**.

---

## 🎯 Business Problem

Global insurance providers receive daily data from multiple countries with challenges such as:

- 📂 Multiple file formats (CSV, Excel, JSON, Parquet)
- 🌐 Country-specific schema differences
- 🔐 Sensitive data (PII & PHI)
- ❌ Poor data quality (duplicates, nulls, inconsistencies)
- 📊 No centralized reporting or governance

---

## ✅ Solution

We built a **scalable and secure data platform** using:

- Azure Databricks
- Unity Catalog
- ADLS Gen2 (Volumes)
- Delta Lake
- PySpark

The solution ensures:

✔ Automated ingestion  
✔ Data standardization  
✔ Encryption of sensitive fields  
✔ Data quality checks  
✔ Audit & governance  
