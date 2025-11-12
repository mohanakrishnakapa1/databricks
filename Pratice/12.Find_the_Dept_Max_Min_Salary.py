# Databricks notebook source
data=[('Genece' , 2 , 75000),
('𝗝𝗮𝗶𝗺𝗶𝗻' , 2 , 80000 ),
('𝗣𝗮𝗻𝗸𝗮𝗷' , 2 , 80000 ),
('Tarvares' , 2 , 70000),
('Marlania' , 4 , 70000),
('Briana' , 4 , 85000),
('𝗞𝗶𝗺𝗯𝗲𝗿𝗹𝗶' , 4 , 55000),
('𝗚𝗮𝗯𝗿𝗶𝗲𝗹𝗹𝗮' , 4 , 55000),  
('Lakken', 5, 60000),
('Latoynia' , 5 , 65000) ]
schema="emp_name string,dept_id int,salary int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.groupBy("dept_id").agg(max("salary").alias("MaxSalary"),min("salary").alias("MinSalary"))

# COMMAND ----------

df2=df1.join(df,df1["dept_id"]==df1["dept_id"],"inner")


# COMMAND ----------

df2.filter((col("salary")==col("MaxSalary"))| (col("salary")==col("MinSalary"))).show()

# COMMAND ----------


