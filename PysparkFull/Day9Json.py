# Databricks notebook source
Multilinepath = "/FileStore/tables/day9/order_multiline.json"
Singlelinepath = "/FileStore/tables/day9/order_singleline.json"

# COMMAND ----------

df_singlelineJson = spark.read.format("json").load(Singlelinepath)

# COMMAND ----------

df_singlelineJson.show()

# COMMAND ----------

df_singlelineJson.printSchema()

# COMMAND ----------

#Read Multiline json with Multiline True else spark throws error as spart doesn't support multiline by default
df_Multilinejson = spark.read.format("json").option("multiline",True).load(Multilinepath)

# COMMAND ----------

df_Multilinejson.show()

# COMMAND ----------

#With Schema
_schema = "customer_id string,order_id string,contact array<long>"
df_schema_json = spark.read.format("json").schema(_schema).load(Singlelinepath)

# COMMAND ----------

df_schema_json.show()

# COMMAND ----------

#Write Complex schema
_schema = "contact array<string>,customer_id string,order_id string,order_line_items array<struct<amount double,item_id string,qty long>>"
df_complexschemaread = spark.read.format("json").schema(_schema).load(Singlelinepath)

# COMMAND ----------

df_complexschemaread.show()

# COMMAND ----------

df

# COMMAND ----------

#from_json function to read a column json
_schema = "contact array<string>,customer_id string,order_id string,order_line_items array<struct<amount double,item_id string,qty long>>"
from pyspark.sql.functions import from_json
df_singlelinetextformat = spark.read.format("text").load(Singlelinepath)

df_from_json = df_singlelinetextformat.withColumn("fromjsonpase",from_json(df_singlelinetextformat.value,_schema))
df_from_json.show()

# COMMAND ----------

df_singlelinetextformat.show(truncate=False)

# COMMAND ----------

#convert parsed JSON data to string JSON text
from pyspark.sql.functions import to_json
df_unparsed = df_from_json.withColumn("unparsed",to_json(df_from_json.fromjsonpase))

# COMMAND ----------

df_unparsed.printSchema()

# COMMAND ----------

df_unparsed.select("unparsed").show(truncate=False)

# COMMAND ----------

#Flatten the Data
df_flatten = df_from_json.select("fromjsonpase.*")

# COMMAND ----------

df_flatten.show()

# COMMAND ----------

from pyspark.sql.functions import explode
df_flatten1= df_flatten.withColumn("expanded_line_items",explode("order_line_items"))

# COMMAND ----------

df_flatten1.show()

# COMMAND ----------

df_flatten2 = df_flatten1.select("contact","customer_id","order_id","expanded_line_items.*")
df_flatten2.show()

# COMMAND ----------

#Explode array
df_final = df_flatten2.withColumn("contact_expanded",explode("contact")).drop("contact")
df_final.show()

# COMMAND ----------


