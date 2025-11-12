# Databricks notebook source
# MAGIC %fs
# MAGIC ls /FileStore

# COMMAND ----------


from pyspark.sql.types import IntegerType,StringType,StructType,DoubleType,StructField,DateType
name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                      StructField("surname",StringType(),True)
                                      ])

# COMMAND ----------

									  
drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                      StructField("driverRef",StringType(),True),
                                      StructField("number",IntegerType(),True),
                                      StructField("code",StringType(),True),
                                      StructField("name",name_schema),
                                      StructField("dob",DateType(),True),
                                      StructField("nationality",StringType(),True),
                                      StructField("url",StringType(),True)
                                      ])

# COMMAND ----------

df_json=spark.read.option("header","true").schema(drivers_schema).option("multiLine","true").format("json").json("/FileStore/tables/drivers.json")

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

#select all struct Columns at a time
df_json.select("code","dob","name.*").show()

# COMMAND ----------

df_json.count()

# COMMAND ----------

df_json.show()

# COMMAND ----------

df_con=spark.read.option("header","true").format("json").load("/FileStore/tables/constructors.json")

# COMMAND ----------

df_con.count()

# COMMAND ----------

df_con.
