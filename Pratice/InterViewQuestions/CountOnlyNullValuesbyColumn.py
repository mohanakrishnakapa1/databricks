# Databricks notebook source
data = [
    (1, "A", 23),
    (2, "B", None),
    (3, "C", 56),
    (4, None, None),
    (5, None, None)
]

data_schema=['ID','Name','Age']

df=spark.createDataFrame(data,data_schema)

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import count,count_if,col
df.select([count(col(i)) for i in df.columns]).show()

# COMMAND ----------

df.select([count_if(col(i)) for i in df.columns]).show()
