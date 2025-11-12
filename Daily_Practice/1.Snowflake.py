# Databricks notebook source
df_snow=spark.read.format("snowflake")\
                .option("sfURL", "https://pc77020.central-us.azure.snowflakecomputing.com")\
                .option("sfWarehouse", "COMPUTE_WH")\
                .option("sfDatabase", "TEST_DB")\
                .option("sfSchema", "PUBLIC")\
                .option("sfRole", "ACCOUNTADMIN")\
                .option("sfUser","mohanaaws") \
                .option("sfPassword","Tanvi*9873")\
                .option("sfAccount","pc77020")\
                .option("dbtable","emp")\
                .load()

# COMMAND ----------

df_snow1=spark.read.format("snowflake")\
                .option("sfURL", "https://pc77020.central-us.azure.snowflakecomputing.com")\
                .option("sfWarehouse", "COMPUTE_WH")\
                .option("sfDatabase", "SNOWFLAKE_SAMPLE_DATA")\
                .option("sfSchema", "TPCH_SF10")\
                .option("sfRole", "ACCOUNTADMIN")\
                .option("sfUser","mohanaaws") \
                .option("sfPassword","Tanvi*9873")\
                .option("sfAccount","pc77020")\
                .option("dbtable","orders")\
                .load()

# COMMAND ----------

df_snow1.show()

# COMMAND ----------

df_snow1 = df_snow1.select("o_orderkey","O_CUSTKEY","O_ORDERSTATUS","O_TOTALPRICE","O_ORDERDATE","O_ORDERPRIORITY","O_SHIPPRIORITY")

# COMMAND ----------

df_snow1.show()

# COMMAND ----------

from pyspark.sql.functions import col
df_snow1= df_snow1.filter((col("O_ORDERSTATUS")=='F') & (col("O_ORDERDATE")=='1994-01-21'))

# COMMAND ----------

df_snow1.show()

# COMMAND ----------

df_snow1.count()

# COMMAND ----------

from pyspark.sql.fu
totalpriority = df_snow1.groupby("O_ORDERPRIORITY").agg(sum(col("O_TOTALPRICE")).alias("Total_Price"),count(col("O_CUSTKEY")).alias("cnt_orderley"))

# COMMAND ----------

totalpriority.show()

# COMMAND ----------


