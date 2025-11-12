# Databricks notebook source
spark

# COMMAND ----------

df = spark.createDataFrame([
    {'A': 1, 'B': 1},
    {'A': 2, 'B': 1},
    {'A': 3, 'B': 1},
    {'A': 4, 'B': 2},
    {'A': 5, 'B': 1},
    {'A': 6, 'B': 2},
    {'A': 7, 'B': 1},
    {'A': 8, 'B': 3},
    {'A': 9, 'B': 3},
    {'A': 10, 'B': 3}
])


# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import lag,lead,row_number,rank,dense_rank



# COMMAND ----------

df.createOrReplaceGlobalTempView("DataDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DataDF

# COMMAND ----------


