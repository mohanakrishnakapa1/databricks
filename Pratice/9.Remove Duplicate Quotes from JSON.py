# Databricks notebook source
df=spark.createDataFrame(data=[('{"name":"Sagar"Prajapath"}',),],schema=['data'])
display(df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
display(df.withColumn("data",regexp_replace("data",'\"{1}',' ')))

# COMMAND ----------


