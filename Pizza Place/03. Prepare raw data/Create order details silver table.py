# Databricks notebook source
# MAGIC %run "../01. Project Set Up/02. Config"

# COMMAND ----------

silver_file_path = f'{silver_folder_path}/order_details'
silver_table_name = 'pizza_place_silver.order_details_silver'
checkpoint_path = f'{silver_file_path}/_checkpoint'
bronze_table_name = 'pizza_place_bronze.order_details_bronze'

# COMMAND ----------

spark.sql(
    f'''
    CREATE TABLE IF NOT EXISTS {silver_table_name} (
        order_details_id LONG,
        order_id LONG,
        pizza_id STRING,
        quantity LONG
    )
    USING DELTA
    '''
)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = (spark.readStream
      .table(bronze_table_name)
      .drop(col('_rescued_data'))
      .drop(col('created_at'))
      .drop(col('source_file'))
      )

# COMMAND ----------

(df.writeStream
 .format('delta')
 .outputMode('append')
 .option('checkpointLocation', checkpoint_path)
 .queryName('Silver order details merge')
 .trigger(availableNow=True)
 .table(silver_table_name)
 )

# COMMAND ----------

spark.sql(f'SELECT * FROM {silver_table_name}').display()
