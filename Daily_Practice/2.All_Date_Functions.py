# Databricks notebook source
list_data=[["2022-03-31 01:55 AM"],["2022-02-15 02:55 PM"],["2023-08-18 10:55 AM"],["2021-11-28 4:55 PM"],["2022-03-22 02:15 PM"]]
lis_col=["inp_col"]

# COMMAND ----------

df=spark.createDataFrame(list_data,lis_col)

# COMMAND ----------

df.show()

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from pyspark.sql.functions import to_date,date_format,to_timestamp
df1=df.withColumn("date_new",to_date("inp_col","yyyy-MM-dd"))\
      .withColumn("timestam",to_timestamp("inp_col","yyyy-MM-dd hh:mm a"))
df1.show()

# COMMAND ----------

from pyspark.sql.functions import *
df1.select(year(col("date_new")),month(col("date_new")),dayofmonth(col("date_new"))).show()

# COMMAND ----------

df1.select(dayofweek(col("date_new")),date_format(col("date_new"),'EEEE'),date_format(col("date_new"),'LLL')).show()

# COMMAND ----------

df1.withColumn("curr_date",current_date())\
    .withColumn("curr_timestamp",current_timestamp())\
.show()

# COMMAND ----------

df1.select(date_add("date_new",5),date_sub("date_new",5),datediff("date_new",current_date()),date_trunc("mon","date_new")).show()

# COMMAND ----------

help(date_trunc)

# COMMAND ----------


