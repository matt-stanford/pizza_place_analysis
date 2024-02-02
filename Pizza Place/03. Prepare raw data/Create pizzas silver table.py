# Databricks notebook source
# MAGIC %run "../01. Project Set Up/02. Config"

# COMMAND ----------

file_path = f'{silver_folder_path}/pizzas'
silver_table_name = 'pizza_place_silver.pizzas_silver'
checkpoint_path = f'{file_path}/_checkpoint'
bronze_table_name = 'pizza_place_bronze.pizzas_bronze'

# COMMAND ----------

spark.sql(
    f'''
    CREATE TABLE IF NOT EXISTS {silver_table_name} (
        pizza_id STRING,
        pizza_type_id STRING,
        size STRING,
        price DECIMAL(4,2),
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
 .withColumn('price', regexp_replace(col('price'), '\$', '').cast('decimal(4,2)'))
 .withColumn('start_date', to_date(col('created_at')))
 .withColumn('end_date', lit(None))
 .withColumn('status', lit('a'))
 .drop(col('created_at'))
 .drop(col('_rescued_data'))
 .drop(col('source_file'))
)

# COMMAND ----------

df.display()

# COMMAND ----------

from delta.tables import DeltaTable

def upsert_to_delta(input_df, batch_id):
    updates_df = (input_df
        .withColumn('merge_id', col('pizza_id'))
        .union(input_df.withColumn('merge_id', lit(None)))
    )
    
    delta_table = DeltaTable.forName(spark, silver_table_name)
    (delta_table
        .alias('original')
        .merge(updates_df.alias('updates'), 'original.pizza_id = updates.merge_id AND original.status = "a"')
        .whenMatchedUpdate(
            set={'original.end_date': current_date(), 'original.status': lit('i')}
        )
        .whenNotMatchedInsert(condition='updates.merge_id IS NULL')
        .execute()
    )

# COMMAND ----------

updates_df.where('merge_id IS NULL').display()

# COMMAND ----------

# .whenNotMatchedInsertAll(condition='merge_id IS NULL')

# COMMAND ----------

(df.writeStream
    .format('delta')
    .outputMode('append')
    .foreachBatch(upsert_to_delta)
    .option('checkpointLocation', checkpoint_path)
    .queryName('Silver pizzas merge')
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

spark.sql(f'SELECT * FROM {silver_table_name}').display()
