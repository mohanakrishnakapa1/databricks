# Databricks notebook source
# MAGIC %md
# MAGIC ### DLT
# MAGIC DLT is a framework for creating batch and streaming data pipelines in SQL and Python. 
# MAGIC
# MAGIC Common use cases for DLT include data ingestion from sources such as cloud storage (such as Amazon S3, Azure ADLS Gen2, and Google Cloud Storage) and message buses (such as Apache Kafka, Amazon Kinesis, Google Pub/Sub, Azure EventHub, and Apache Pulsar), and incremental batch and streaming transformations.
# MAGIC
# MAGIC  Note
# MAGIC
# MAGIC DLT requires the Premium plan. Contact your Databricks account team for more information.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Works with 3 Types of Datasets
# MAGIC
# MAGIC Streaming Table(Permenanat/Temporary) - Used as append datasources/Increamental data
# MAGIC
# MAGIC Materiliazed view - Used for Transofrmations, aggreagation or Computations
# MAGIC
# MAGIC Views: Used for Intermediate Transformations. Not storead in Target Scehma

# COMMAND ----------


Feature	Streaming Table (STREAMING LIVE TABLE)	                        Materialized View (LIVE TABLE)
Processing Semantics	Incremental (stream processing)	                Batch (correctness-driven, often incremental where possible)
Data Freshness	Low-latency, near real-time	                            Higher latency (seconds/minutes to hours/days), ensures correctness
Input Sources	Primarily append-only data (files, Kafka, logs)	        Any Delta table (handles INSERT, UPDATE, DELETE, MERGE from sources)
Schema Evolution	Handles new columns for new data; existing          Tracks and propagates schema changes from upstream, recomputes as needed.
data not updated without full refresh.	
Primary Goal	Ingestion, low-latency processing of continuously 	    Data transformation, aggregation, ensuring query correctness and performance
arriving data  
Use Cases	Bronze layer, high-volume append-only data pipelines	    Silver/Gold layers, BI dashboards, feature engineering, complex transformations
Updates to Data	Processes new incoming records; does not reflect        Recomputes to reflect all changes (inserts, updates, deletes) in source data.
changes to existing records in source.	
Syntax (Python)	dlt.read_stream("source_table")	                        dlt.read("source_table")

# COMMAND ----------

# Import dlt module
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create table
# MAGIC
# MAGIC #### We can Specify Table properties and Comments but these are optional.
# MAGIC
# MAGIC #### Also Table Name we can define along with properties if not default function name would be table name.

# COMMAND ----------

# Create table and properties
# Tge below one decorator

@dlt.table(

  table_properties = {"quality":"bronze"},
  comment = "This is Bronze Orders Table"
)

def orders_bronze():
  # the below one called as transformation
  df = spark.readStream.table("dev.bronze.orders_raw")
  return df

# COMMAND ----------

# Create Materilized view and properties
# Defined table as well along with properties
# Tge below one decorator

@dlt.table(
  table_properties = {"quality":"bronze"},
  comment = "This is Bronze Cusomer Table"
  name = "customer_bronze"
)

def cust_bronze():
  # the below one called as transformation
  df = spark.read.table("dev.bronze.customer_raw")
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crate view
# MAGIC
# MAGIC Views are Temporary and will not get stored in Target schema
# MAGIC
# MAGIC ### We will use **LIVE keyword to call tables created in same pipeline.

# COMMAND ----------

# Create view and Join tables created above.

@dlt.table(
    comment = "Joined View"
  
)

def Joined_vw():
  
  df_c = spark.read.table("LIVE.customer_bronoze")
  df_o =spark.read.table("LIVE.orders_bronoze")
  df_join = df_o.join(df_c, how ="left_outer",on df_c.custket = df_o.custkey)
  return df_join

# COMMAND ----------

# Create Materilized view and add new column

from pyspark.sql.functions import current_timestamp

@dlt.table(
  table_properties = {"quality":"silver"},
  comment = "Silver Materilized View"
  name = "Joined silver"
)

def joined_sliver():
  # Calling join view and creating new column
  df = spark.read.table("LIVE.Joined_vw").withColumn("__insert_Date",current_timestamp)
  return df

# COMMAND ----------

# aggreagate gold layer


@dlt.table(
  table_properties = {"quality":"gold"},
  comment = "Orders aggregated table"
)
def orders_agg_gold():
  df = spark.read.table("LIVE.joined_sliver")
  df_final = df.groupBy("c_mksegment").agg(count("c_orderkey").alas("sum_ofOrders"))
  return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now if you change the Gold layer Table and add new column then databricks will automatically take care, we don't need to do anything related to schema ** and if do rename existing table..etc. dlt will automatically take care

# COMMAND ----------

# aggreagate gold layer, Now I added one more column and rename the table. we can do same chagnes in silver or bronze layer as well


@dlt.table(
  table_properties = {"quality":"gold"},
  comment = "Orders aggregated table"
)
def orders_sum_gold():
  df = spark.read.table("LIVE.joined_sliver")
  # rename sum_OfOrders to Count_OfOrders -- column name changed. 
  # added one more column
  df_final = df.groupBy("c_mksegment").agg(count("c_orderkey").alas("count_ofOrders"),sum("Order_amount").alias("Sale_amt"))
  return df_final
