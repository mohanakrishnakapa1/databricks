# Databricks notebook source
from pyspark.sql.functions import *
df=spark.read.text("/FileStore/tables/word_count.txt")
df_trimmed=df.select(trim(lower(regexp_replace(col("value"),'([^\s\w_]|_)+',''))).alias('value'))
df_split=df_trimmed.select(explode(split(df_trimmed.value,'\s+')).alias('value')).where(expr("value is not null and value<>''"))
df_split.groupby("value").count().orderBy(col("count").desc()).show()

# COMMAND ----------

class word_count():
    def __init__(self):
        
    def word_tot_count(self,df):
        


# COMMAND ----------


