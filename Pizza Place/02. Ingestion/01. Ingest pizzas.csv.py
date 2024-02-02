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

file_path = 'abfss://bronze@dbpizzaplacesa.dfs.core.windows.net/pizzas'
table_name = 'pizzas_bronze'
checkpoint_path = f'{file_path}/_checkpoint'

# COMMAND ----------

spark.conf.set('spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles', 10)

# COMMAND ----------

cloud_files_conf = {
    'cloudFiles.format': 'csv',
    'cloudFiles.inferColumnTypes': 'true',
    'cloudFiles.schemaEvolutionMode': 'rescue',
    'rescuedDataColumn': '_rescued_data',
    'cloudFiles.schemaLocation': checkpoint_path
}

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df = (spark.readStream.format('cloudFiles')
      .options(**cloud_files_conf)
      .option('Header', True)
      .load(file_path)
      .withColumn('created_at', current_timestamp())
      .withColumn('source_file', input_file_name())
)

# COMMAND ----------

(df.writeStream
    .format('delta')
    .outputMode('append')
    .queryName('Pizzas merge')
    .option('checkpointLocation', checkpoint_path)
    .trigger(availableNow=True)
    .toTable(f'pizza_place_bronze.{table_name}')
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT COUNT(*) FROM pizza_place_bronze.pizzas_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pizza_place_bronze.pizzas_bronze
