# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE pizza_place_gold.pizza_orders_gold
# MAGIC AS
# MAGIC SELECT
# MAGIC     od.order_details_id,
# MAGIC     od.order_id,
# MAGIC     o.date AS order_date,
# MAGIC     o.time AS order_time,
# MAGIC     pt.name AS pizza_name,
# MAGIC     pt.category AS pizza_category,
# MAGIC     p.size,
# MAGIC     od.quantity,
# MAGIC     p.price * od.quantity AS total_price
# MAGIC FROM pizza_place_silver.order_details_silver od
# MAGIC LEFT JOIN pizza_place_silver.orders_silver o ON o.order_id = od.order_id
# MAGIC JOIN pizza_place_silver.pizzas_silver p ON od.pizza_id = p.pizza_id
# MAGIC JOIN pizza_place_silver.pizza_types_silver pt ON p.pizza_type_id = pt.pizza_type_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pizza_place_gold.pizza_orders_gold
