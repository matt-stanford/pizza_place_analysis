-- Databricks notebook source
CREATE OR REPLACE TABLE pizza_place_silver.pizza_orders_silver
AS
SELECT
    od.order_details_id,
    od.order_id,
    o.date AS order_date,
    o.time AS order_time,
    pt.name AS pizza_name,
    pt.category AS pizza_category,
    p.size,
    od.quantity,
    p.price * od.quantity AS total_price
FROM pizza_place_bronze.order_details_bronze od
LEFT JOIN pizza_place_bronze.orders_bronze o ON o.order_id = od.order_id
JOIN pizza_place_bronze.pizzas_bronze p ON od.pizza_id = p.pizza_id
JOIN pizza_place_bronze.pizza_types_bronze pt ON p.pizza_type_id = pt.pizza_type_id

-- COMMAND ----------

SELECT * FROM pizza_place_silver.pizza_orders_silver
