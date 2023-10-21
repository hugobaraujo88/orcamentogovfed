# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the following containers (storage accounts)
# MAGIC 1. zipfiles
# MAGIC 2. extracted
# MAGIC 3. processed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set-up the configs
# MAGIC #### Please update the following 
# MAGIC - application-id
# MAGIC - service-credential
# MAGIC - directory-id

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<application-id>",
           "fs.azure.account.oauth2.client.secret": "<service-credential>", 
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the zipfiles container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://zipfiles@adltransparenciaproject.dfs.core.windows.net/",
  mount_point = "/mnt/adltransparenciaproject/zipfiles",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the extracted container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://extracted@adltransparenciaproject.dfs.core.windows.net/",
  mount_point = "/mnt/adltransparenciaproject/extracted",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the processed container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@adltransparenciaproject.dfs.core.windows.net/",
  mount_point = "/mnt/adltransparenciaproject/processed",
  extra_configs = configs)
