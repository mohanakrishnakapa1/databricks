# Databricks notebook source
# MAGIC %md
# MAGIC ### Static Allocation will have fixed Executors and Memory which will not be used until job gets finshed.
# MAGIC
# MAGIC ###Dynamic allocation we can set the time to kill the executors if the executor been ideal or once afte the executor fisnishes tasks.
# MAGIC
# MAGIC #### set the below properties Memory,Enable the Dynamic alloacation Max Executors,Timeout Options
# MAGIC

# COMMAND ----------

# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Dynamic Allocation")
    .master("spark://197e20b418a6:7077")
    .config("spark.executor.cores", 2)
    .config("spark.executor.memory", "512M")
    .config("spark.dynamicAllocation.enabled", True)
    .config("spark.dynamicAllocation.minExecutors", 0)
    .config("spark.dynamicAllocation.maxExecutors", 5)
    .config("spark.dynamicAllocation.initialExecutors", 1)
    .config("spark.dynamicAllocation.shuffleTracking.enabled", True)
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "60s")
    .getOrCreate()
)

