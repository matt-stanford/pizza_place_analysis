# Databricks notebook source
# MAGIC %run "./02. Config"

# COMMAND ----------

spark.sql(
    f'''
        CREATE SCHEMA IF NOT EXISTS pizza_place_bronze
        LOCATION '{bronze_folder_path}'
    '''
)

# COMMAND ----------

spark.sql(
    f'''
        CREATE SCHEMA IF NOT EXISTS pizza_place_silver
        LOCATION '{silver_folder_path}'
    '''
)
