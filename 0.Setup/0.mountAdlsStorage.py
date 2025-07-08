# Databricks notebook source
# MAGIC %md
# MAGIC #### ENTER THE BELOW DETAILS
# MAGIC 1. client_id
# MAGIC 2. secret_id
# MAGIC 3. tenant_id

# COMMAND ----------

storage_account_name = "formula1dbdevadls"
client_id = "b79d6627-f5da-4923-91f7-fb30d5f2f1d1"
client_secret = "mxT8Q~oGWqGI1.iTaafjtnjF_zpBn3yQ2crbZaPJ"
tenant_id = "9cd5292d-d337-4834-b68a-15f1ebfcf00c"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a UDF to mount the container in adls

# COMMAND ----------

def mount_adls(container_name):
  storage_name = "formula1dbdevadls"
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Raw Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dbdevadls/raw")
mount_adls("raw")
dbutils.fs.ls("/mnt/formula1dbdevadls/raw")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Processed Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dbdevadls/processed")
mount_adls("processed")
dbutils.fs.ls("/mnt/formula1dbdevadls/processed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Retail Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dbdevadls/retail")
# mount_adls("retail")
# dbutils.fs.ls("/mnt/formula1dbdevadls/retail")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Presentation Container
# MAGIC

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dbdevadls/presentation")
mount_adls("presentation")
dbutils.fs.ls("/mnt/formula1dbdevadls/presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Incremental Container

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dbdevadls/incremental")
mount_adls("incremental")
dbutils.fs.ls("/mnt/formula1dbdevadls/incremental")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Mount DeltaLake Container
# MAGIC

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dbdevadls/deltalake")
mount_adls("deltalake")
dbutils.fs.ls("/mnt/formula1dbdevadls/deltalake")

# COMMAND ----------

dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
