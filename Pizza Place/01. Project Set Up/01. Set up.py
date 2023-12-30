# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    application_id = dbutils.secrets.get(scope='pizza-place-secret-scope', key='application-id')
    tenant_id = dbutils.secrets.get(scope='pizza-place-secret-scope', key='tenant-id')
    client_secret = dbutils.secrets.get(scope='pizza-place-secret-scope', key='client-secret')

    # Set Spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Check to see if the mount already exists and if so, unmount it
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount to ADLS
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    # Display the mount points
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('dbpizzaplacesa', 'silver')

# COMMAND ----------

mount_adls('dbpizzaplacesa', 'gold')

# COMMAND ----------

dbutils.fs.unmount('')
