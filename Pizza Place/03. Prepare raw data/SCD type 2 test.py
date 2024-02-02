# Databricks notebook source
# MAGIC %run "../01. Project Set Up/02. Config"

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

file_path = f'{silver_folder_path}/pizza_types'
silver_table_name = 'pizza_place_silver.pizza_types_silver'
checkpoint_path = f'{file_path}/_checkpoint'
bronze_table_name = 'pizza_place_bronze.pizza_types_bronze'

# COMMAND ----------

# MAGIC %md
# MAGIC ### testing merge with test df

# COMMAND ----------

test_df = spark.read.table(bronze_table_name)
test_df.display()

# COMMAND ----------

test_df \
    .withColumn('merge_id', col('pizza_type_id')) \
    .union(test_df.withColumn('merge_id', lit(None))) \
    .display()

# COMMAND ----------

test_df = (test_df
    .withColumn('start_date', to_date(col('created_at')))
    .withColumn('end_date', lit(''))
    .withColumn('status', lit('a'))
    .drop(col('created_at'))
    .drop(col('_rescued_data'))
    .drop(col('source_file')))

# COMMAND ----------

test_update_df = test_df.filter(col('pizza_type_id') == 'bbq_ckn')

# COMMAND ----------

test_update_df = test_update_df.withColumn('category', lit('Not Chicken!'))

# COMMAND ----------

updates_df = (test_update_df
    .withColumn('merge_id', col('pizza_type_id'))
    .union(test_update_df.withColumn('merge_id', lit(None)))
)

# COMMAND ----------

updates_df.display()

# COMMAND ----------

test_df.write.format('delta').mode('overwrite').saveAsTable('test_delta_table')

# COMMAND ----------

# MAGIC %sql SELECT * FROM test_delta_table

# COMMAND ----------

from delta.tables import *

delta_table = DeltaTable.forPath(spark, '/user/hive/warehouse/test_delta_table')

# COMMAND ----------

(delta_table
    .alias('original')
    .merge(updates_df.alias('updates'), 'original.pizza_type_id = updates.merge_id AND original.status = "a"')
    .whenMatchedUpdate(
        set={'original.end_date': current_date()}
    )
    .whenNotMatchedInsertAll()
    .execute()
    )

# COMMAND ----------

# MAGIC %sql SELECT * FROM test_delta_table

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### end of test

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
        .whenNotMatchedInsertAll()
        .execute()
     )

# COMMAND ----------

 (df.writeStream
    .format('delta')
    .outputMode('append')
    .foreachBatch(upsert_to_delta)
    .option('checkpointLocation', checkpoint_path)
    .option('mergeSchema', 'true')
    .queryName('Silver pizza types merge')
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# %sql
# SELECT * FROM pizza_place_bronze.pizza_types_bronze
