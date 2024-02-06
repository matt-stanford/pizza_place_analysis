# Databricks notebook source
# MAGIC %run "../01. Project Set Up/02. Config"

# COMMAND ----------

file_path = f'{silver_folder_path}/pizza_types'
silver_table_name = 'pizza_place_silver.pizza_types_silver'
checkpoint_path = f'{file_path}/_checkpoint'
bronze_table_name = 'pizza_place_bronze.pizza_types_bronze'

# COMMAND ----------

spark.sql(
    f'''
    CREATE TABLE IF NOT EXISTS {silver_table_name} (
        pizza_type_id STRING,
        name STRING,
        category STRING,
        ingredients STRING,
        start_date DATE,
        end_date DATE,
        status STRING
    )
    USING DELTA
    '''
)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = (spark.readStream
 .table(bronze_table_name)
 .withColumn('start_date', to_date(col('created_at')))
 .withColumn('end_date', lit(None))
 .withColumn('status', lit('a'))
 .drop(col('created_at'))
 .drop(col('_rescued_data'))
 .drop(col('source_file'))
)

# COMMAND ----------

from delta.tables import DeltaTable

def upsert_to_delta(input_df, batch_id):
    updates_df = (input_df
        .withColumn('merge_id', col('pizza_type_id'))
        .union(input_df.withColumn('merge_id', lit(None)))
    )
    
    delta_table = DeltaTable.forName(spark, silver_table_name)
    (delta_table
        .alias('original')
        .merge(updates_df.alias('updates'), 'original.pizza_type_id = updates.merge_id AND original.status = "a"')
        .whenMatchedUpdate(
            set={'original.end_date': current_date(), 'original.status': lit('i')}
        )
        .whenNotMatchedInsert(
            condition='updates.merge_id IS NULL',
            values = {
                'original.pizza_type_id': 'updates.pizza_type_id',
                'original.name': 'updates.name',
                'original.category': 'updates.category',
                'original.ingredients': 'updates.ingredients',
                'original.start_date': 'updates.start_date',
                'original.end_date': 'updates.end_date',
                'original.status': 'updates.status'
            }
        )
        .execute()
    )

# COMMAND ----------

(df.writeStream
    .format('delta')
    .outputMode('append')
    .foreachBatch(upsert_to_delta)
    .option('checkpointLocation', checkpoint_path)
    .queryName('Silver pizza types merge')
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

spark.sql(f'SELECT * FROM {silver_table_name}').display()
