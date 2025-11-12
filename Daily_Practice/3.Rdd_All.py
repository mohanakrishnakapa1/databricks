# Databricks notebook source
# MAGIC %md
# MAGIC ########Create RDD using Text File

# COMMAND ----------

file_location = "/FileStore/tables/input_real_estate.txt"
file_type = "txt"
rdd=sc.textFile(file_location)

# COMMAND ----------

rdd.count()


# COMMAND ----------

rdd.first()

# COMMAND ----------

rdd.take(5)

# COMMAND ----------

rdd.first()

# COMMAND ----------

# MAGIC %md
# MAGIC #########Split RDD

# COMMAND ----------

header = rdd.first()  # Extract the first row as column names
rdd1=rdd.map(lambda x:x.split("|")).filter(lambda x: x != header) 
rdd1.take(3)

# COMMAND ----------

ser = rdd1.map(lambda x:(x[1],x[0])).sortByKey()

# COMMAND ----------

ser.take(19)

# COMMAND ----------

# MAGIC %md
# MAGIC ######### To Display the schema

# COMMAND ----------

rdd1.first()

# COMMAND ----------

location_rdd=rdd1.map(lambda x: x[3])
location_rdd.take(260)

# COMMAND ----------

rdd1.take(10)

# COMMAND ----------

rdd2=rdd1.filter(lambda x: 'Arr' in x[1])
print(rdd2.collect())

# COMMAND ----------

for f in rdd2.collect():
    print(f)

# COMMAND ----------

grouped = rdd1.groupBy(lambda x: x[1]).reduce(lambda x, y: x[0] + y[0])
grouped

# COMMAND ----------

# MAGIC %scala
# MAGIC val data = Seq(("A", 1, "B", 2), ("C", 3, "D", 4))
# MAGIC val rdd = sc.parallelize(data)

# COMMAND ----------

# MAGIC %scala
# MAGIC rdd.take(5)

# COMMAND ----------

# MAGIC %scala
# MAGIC val rdd1=rdd.map(x => x.
# MAGIC rdd1.take(7)

# COMMAND ----------

file_location = "/FileStore/tables/input_real_estate.txt"
file_type = "txt"
rdd=sc.textFile(file_location)

# COMMAND ----------

rdd1=rdd.map(lambda x:x.split('|'))
rdd1.take(10)

# COMMAND ----------

first_two_columns = rdd1.map(lambda columns: (columns[0], columns[1]))
first_two_columns.take(20)

# COMMAND ----------

# MAGIC %scala
# MAGIC val firstTwoColumns = rdd1.map(columns => (columns(0), columns(1)))
# MAGIC firstTwoColumns.take(5)

# COMMAND ----------

first_two_columns = rdd1.map(lambda columns: (_1, _2))
first_two_columns.take(20)

# COMMAND ----------



# COMMAND ----------

spark.readStream.
