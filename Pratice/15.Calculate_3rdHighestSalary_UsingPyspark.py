# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/emp.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

windowspec=Window.orderBy(col("salary").desc())
df.select('*',row_number().over(windowspec).alias("rowNum")).filter(col("rowNum")==3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### If you want to find department wise 3 rd Highest Salary Use this below window Spec

# COMMAND ----------

# Define a window specification partitioned by department and ordering by salary in descending order
windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())

# COMMAND ----------


