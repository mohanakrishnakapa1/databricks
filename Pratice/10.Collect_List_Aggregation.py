# Databricks notebook source
data = [
    ("john", "tomato", 2),
    ("𝚋𝚒𝚕𝚕", "𝚊𝚙𝚙𝚕𝚎", 2),
    ("john", "𝚋𝚊𝚗𝚊𝚗𝚊", 2),
    ("john", "tomato", 3),
    ("𝚋𝚒𝚕𝚕", "𝚝𝚊𝚌𝚘", 2),
    ("𝚋𝚒𝚕𝚕", "𝚊𝚙𝚙𝚕𝚎", 2),
]
schema = "name string,item string,weight int"
df = spark.createDataFrame(data, schema)


# COMMAND ----------

from pyspark.sql.functions import *
df1=df.groupBy("name","item").agg(sum("weight").alias("sum_weight"))

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws

# Assuming 'column1' and 'column2' are the two columns for which you want to generate a hash key
df_new = df_new.withColumn("key_combined", sha2(concat_ws("-", "column1", "column2"), 256))

df_new.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect_list only accept only one argument so we have passed them to struct Type below

# COMMAND ----------

df_final=df1.groupBy("name").agg(collect_list(struct("item","sum_weight")).alias("items")).orderBy(col("name").desc())
display(df_final)

# COMMAND ----------


