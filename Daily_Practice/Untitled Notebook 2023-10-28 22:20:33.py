# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/streaming/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm /FileStore/tables/streaming/text_data_3-1.txt

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType,FloatType

# COMMAND ----------

schema=StructType(fields=[StructField("CircuiteID",IntegerType(),False),\
                    StructField("CircuiteRef",StringType(),True),\
                    StructField("Name",StringType(),True),\
                    StructField("Location",StringType(),True),\
                    StructField("Country",StringType(),True),\
                    StructField("lat",FloatType(),True),\
                    StructField("lng",FloatType(),True),\
                    StructField("alt",IntegerType(),True),\
                    StructField("url",StringType(),True),])

# COMMAND ----------

df1 = spark.read.format("csv") \
  .schema(schema) \
  .option("delimiter",',') \
  .option("header",False)\
  .load(f"{raw_folder_path}/2021-03-21/circuits.csv",header=False)

display(df1)

# COMMAND ----------

infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

df = spark.read.format("csv") \
  .schema(schema) \
  .option("sep", delimiter) \
  .option("header", "true") \
  .load(f"{raw_folder_path}/2021-03-21/circuits.csv")

# Display the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql.functions import col
df_aus=df.filter((col("country")=="Australia") & (col("location")=="Melbourne"))

# COMMAND ----------

df_aus.show(2)

# COMMAND ----------

df_aus=df.filter("country='Australia' and location ='Melbourne'")

# COMMAND ----------

df_aus.show()

# COMMAND ----------

df_usa=df.filter("country='USA'")

# COMMAND ----------

df_usa.show()

# COMMAND ----------

df_pivot=df_usa.groupBy("Country").pivot("Location").sum("alt")

# COMMAND ----------

df_pivot.show()

# COMMAND ----------

df_pivot=df_usa.groupBy("Country").pivot("Location").sum("alt")

# COMMAND ----------

df_pivot.show()

# COMMAND ----------

df_usa.show()

# COMMAND ----------

display(df_usa)
df_usa.printSchema()


# COMMAND ----------

df.printSchema()

# COMMAND ----------


from pyspark.sql.functions import collect_list,col,sum,max
df_max1 = df.groupBy("Country").agg(collect_list("Location").alias("Location"),sum(col("alt")).alias("sum_alt"),max(col("lat")))
df_max1.show()

# COMMAND ----------

df_aus.printSchema()

# COMMAND ----------

schemaJson="driverId INT,driverRef STRING,number INT,Code STRING,name STRING,dob DATE,nationality STRING,url STRING"

# COMMAND ----------

#infer_schema = "false"
#first_row_is_header = "false"
#delimiter = ","

df_json = spark.read.format("json") \
                .option("inferSchema","True")\
                .option("header","false")\
                .load(f"{raw_folder_path}/2021-03-21/drivers.json")

# Display the DataFrame
df_json.show()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

df_json.select(col("name.forename").alias("forename")).show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/2021-03-21/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables

# COMMAND ----------


