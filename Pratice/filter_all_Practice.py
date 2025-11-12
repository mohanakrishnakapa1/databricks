# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructType,DoubleType,StructField
circutite_schema = StructType(fields=[StructField("CircuiteID",IntegerType(),False),
                                      StructField("CircutieRef",StringType(),True),
                                      StructField("name",StringType(),True),
                                      StructField("location",StringType(),True),
                                      StructField("Country",StringType(),True),
                                      StructField("lat",DoubleType(),True),
                                      StructField("lng",DoubleType(),True),
                                      StructField("alt",IntegerType(),True),
                                      StructField("url",StringType(),True),])

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/circuits-1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
#df = spark.read.format(file_type) \
 # .option("inferSchema", True) \
  #.option("header", first_row_is_header) \
  #.option("sep", delimiter) \
  #.load(file_location)

#Using Struct Type
# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .schema(circutite_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(f"{raw_folder_path}/circuits-1.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####filter like SQL

# COMMAND ----------

df.filter("country = 'Australia'").show()

# COMMAND ----------

#Passing Dual Filter
df.filter("country = 'Australia' and location like 'Ad%' ").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Filter like Pythonic way

# COMMAND ----------

df.filter(df.Country == "Australia").show()

# COMMAND ----------

##WE can pass like this as well
df.filter(df["Country"] == "Australia").show()


# COMMAND ----------

#PAssing another condition
from pyspark.sql.functions import col
df.filter((df.Country == "Australia") & (col("location").like ("Ad%"))).show()


# COMMAND ----------

##OR we can pass without using col function
from pyspark.sql.functions import col
df.filter((df.Country == "Australia") & (df.location.like ("Ad%"))).show()

# COMMAND ----------


