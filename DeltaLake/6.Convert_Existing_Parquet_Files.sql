-- Databricks notebook source
-- MAGIC %md
-- MAGIC ######Convert Existing Table
-- MAGIC

-- COMMAND ----------

CONVERT TO DELTA f1_demo.results_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###########Convert Existing File Location

-- COMMAND ----------

CONVERT TO DELTA parquet.`/mnt/formula1/results_managed`
