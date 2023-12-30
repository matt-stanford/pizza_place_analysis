# Databricks notebook source
application_id = dbutils.secrets.get(scope='pizza-place-secret-scope', key='application-id')
tenant_id = dbutils.secrets.get(scope='pizza-place-secret-scope', key='tenant-id')
client_secret = dbutils.secrets.get(scope='pizza-place-secret-scope', key='client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dbpizzaplacesa.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dbpizzaplacesa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dbpizzaplacesa.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dbpizzaplacesa.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dbpizzaplacesa.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

file_path = 'abfss://bronze@dbpizzaplacesa.dfs.core.windows.net/pizza_types'
table_name = 'pizza_types_bronze'
checkpoint_path = f'{file_path}/_checkpoint'

# COMMAND ----------

cloud_files_conf = {
    'cloudFiles.format': 'csv',
    'cloudFiles.inferColumnTypes': 'true',
    'cloudFiles.schemaLocation': checkpoint_path,
    'cloudFiles.schemaEvolutionMode': 'rescue',
    'rescuedDataColumn': '_rescued_data'
}

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df = (spark.readStream
      .format('cloudFiles')
      .options(**cloud_files_conf)
      .option('header', 'true')
      .load(file_path)
      .withColumn('created_at', current_timestamp())
      .withColumn('source_file', input_file_name())
      )

# COMMAND ----------

from delta.tables import *

def upsert_to_delta(input_df, batch_id):
    if spark._jsparkSession.catalog().tableExists(f'pizza_place_bronze.{table_name}'):
        delta_df = DeltaTable.forName(spark, f'pizza_place_bronze.{table_name}')

        (delta_df.alias('t')
            .merge(input_df.alias('s'), 't.pizza_type_id = s.pizza_type_id')
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        input_df.write.format('delta').mode('overwrite').saveAsTable(f'pizza_place_bronze.{table_name}')

# COMMAND ----------

(df.writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', checkpoint_path)
    .foreachBatch(upsert_to_delta)
    .queryName('Pizza types merge')
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pizza_place_bronze.pizza_types_bronze
