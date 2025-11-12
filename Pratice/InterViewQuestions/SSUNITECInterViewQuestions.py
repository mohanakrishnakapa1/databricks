# Databricks notebook source
# MAGIC %md
# MAGIC ### Q1.Get First Non Null value from 3 columns

# COMMAND ----------

from pyspark.sql.functions import coalesce,col,when
data=[('Goa', ' ', 'AP'),('', 'AP', None), (None, ' ', 'Bglr')]
columns=["city1", "city2", "city3"]
df=spark.createDataFrame(data, columns)
df_ext = df.withColumn("RequestedCol",coalesce(when(col('city1')=='',None).otherwise(col('city1')),
                                               when(col('city2')==' ',None).otherwise(col('city2')),
                                               when(col('city3')==' ',None).otherwise(col('city3'))
                                              )).select(col("RequestedCol"))
display(df_ext)

# COMMAND ----------


