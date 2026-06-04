# Databricks notebook source
# IMPORTS
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# COMMAND ----------

# getting salt key for encryption
def get_aes_salt():
    return dbutils.secrets.get(scope = "globalscope", key = "aes-salt")

salt_key = get_aes_salt()

# COMMAND ----------

# ENCRYPTION FUNCTION
def encrypt_column(col_name:str, alias:str, aes_salt:str, mode:str = 'ECB'):
    return F.base64(
        F.aes_encrypt(F.col(col_name).cast("string"), F.lit(salt_key), F.lit(mode))
    ).alias(alias)



# COMMAND ----------

def build_column_exprs(df,sensitive_cols, aes_salt):
    column_exprs = []
    for col in df.columns:
        if col in sensitive_cols:
            column_exprs.append(encrypt_column(col, col, salt_key ))
        else:
            column_exprs.append(F.col(col))
    return column_exprs


# COMMAND ----------

# DECRYPTION FUNCTION
def decrypt_column(col_name: str, alias: str, aes_salt: str, mode: str = "ECB"):
    return (
        F.aes_decrypt(
            F.unbase64(F.col(col_name)),
            F.lit(aes_salt),
            F.lit(mode)
        )
        .cast("string")
        .alias(alias)
    )

# COMMAND ----------

def build_decryption_column_exprs(df, sensitive_cols, aes_salt):
    column_exprs = []
    for col in df.columns:
        if col in sensitive_cols:
            column_exprs.append(decrypt_column(col, col, aes_salt))
        else:
            column_exprs.append(F.col(col))
    return column_exprs
