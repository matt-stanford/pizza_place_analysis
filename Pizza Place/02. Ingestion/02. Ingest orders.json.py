# Databricks notebook source
application_id = dbutils.secrets.get(scope='pizza-place-secret-scope', key='application-id')
tenant_id = dbutils.secrets.get(scope='pizza-place-secret-scope', key='tenant-id')
client_secret = dbutils.secrets.get(scope='pizza-place-secret-scope', key='client-secret')
 
spark.conf.set("fs.azure.account.auth.type.dbpizzaplacesa.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dbpizzaplacesa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dbpizzaplacesa.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dbpizzaplacesa.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dbpizzaplacesa.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

file_path = 'abfss://bronze@dbpizzaplacesa.dfs.core.windows.net/orders'
table_name = 'orders_bronze'
checkpoint_path = f'{file_path}/_checkpoint'

# COMMAND ----------

spark.conf.set('spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles', 10)

# COMMAND ----------

# Set up cloud files config
cloud_files_conf = {
    'cloudFiles.format': 'json',
    'cloudFiles.inferColumnTypes': 'true',
    "cloudFiles.schemaHints":"order_id BIGINT, date DATE, time TIMESTAMP",
    'cloudFiles.schemaLocation': checkpoint_path,
    'cloudFiles.schemaEvolutionMode': 'rescue',
    'rescuedDataColumn': '_rescued_data'
}

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df = (spark.readStream
      .format('cloudFiles')
      .options(**cloud_files_conf)
      .load(file_path)
      .withColumn('created_at', current_timestamp())
      .withColumn('source_file', input_file_name())
      )

# COMMAND ----------

(df.writeStream
    .format('delta')
    .outputMode('append')
    .queryName('Orders merge')
    .option('checkpointLocation', checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pizza_place_bronze.orders_bronze
