# Databricks notebook source
# MAGIC %md
# MAGIC ### Check Databricks datasets
# MAGIC
# MAGIC #### dbutils.fs.head("/databricks-datasets/retail-org/company_employees/company_employees.csv")

# COMMAND ----------

dbutils.fs.head("/databricks-datasets/retail-org/company_employees/company_employees.csv")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/")
