# Databricks notebook source
parpath="/FileStore/tables/day8/sales_data.parquet"
orcpath="/FileStore/tables/day8/sales_data.orc"

# COMMAND ----------

#Read Parquet
df_parquet = spark.read.format("parquet").load(parpath)
df_parquet.show()

# COMMAND ----------

#Read ORc
df_orc = spark.read.format("orc").load(orcpath)
df_orc.show()

# COMMAND ----------

#Read Multiple Files at a time same for parquet as well,Read all orc files
df_orc = spark.read.format("orc").load("/FileStore/tables/day8/*.orc")

# COMMAND ----------

#Benifits of Columnar format
#add decorator to get the exectuion time
import time
def get_time(func):
    def execution_time():
        start_time = time.time()
        func()
        end_time = time.time()
        return (f"Execution time:{(end_time - start_time)*1000} ms")
    print(execution_time())

# COMMAND ----------

@get_time
def x():
    df_parquet = spark.read.format("parquet").load(parpath)
    df_parquet.count()


# COMMAND ----------

@get_time
def x():
    df_parquet = spark.read.format("parquet").load(parpath)
    df_parquet.select("trx_id").count()

# COMMAND ----------

#recursive files read: as the files exists different folders like under the there is one more folder

df_parque_recursive = spark.read.format("parquet").option("recursiveFileLookup",True).load(parpath)


# COMMAND ----------

 
