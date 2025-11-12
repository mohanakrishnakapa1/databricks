# Databricks notebook source
data = [
    ("john", "Batmenton,Tennis"),
    ("Alice", "cricket,Tennis"),
    ("Bob", "cricket,Carroms")
   
]
schema = "name string,hobbie string"
df = spark.createDataFrame(data, schema)

# COMMAND ----------

from pyspark.sql.functions import split,explode
df_updates = df.withColumn("SplitNames",split("hobbie",",")).withColumn("Hobbie",explode("SplitNames")).select("name","hobbie")
df_updates.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.tpch.orders

# COMMAND ----------


