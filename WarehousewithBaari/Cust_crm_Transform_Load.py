# Databricks notebook source
# MAGIC %sql
# MAGIC select cst_id,cst_key,count(cst_id) from bronze.crm_cust_info group by cst_id,cst_key having count(cst_id)>1 order by cst_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select  cst_id,cst_key,trim(cst_firstname),trim(cst_lastname),
# MAGIC case when upper(cst_marital_status)= 'S' then 'Single'
# MAGIC      when upper(cst_marital_status) = 'M' then 'Married'
# MAGIC      else 'n/a' 
# MAGIC END cst_martial_status,
# MAGIC case when upper(trim(cst_gndr)) = 'F' then 'Female'
# MAGIC    when upper(trim(cst_gndr)) = 'M' then 'Male'
# MAGIC    else 'n/a'
# MAGIC end cst_gndr,
# MAGIC cst_create_date
# MAGIC  from 
# MAGIC (select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag from bronze.crm_cust_info where cst_id is not null) a 
# MAGIC where flag=1

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/FileStore/tables/warehouse/source_crm/deltacust")

# COMMAND ----------

from pyspark.sql.functions import count,col

df_count_nullandextracst_id = df.groupBy("cst_id","cst_key").agg(count("cst_id").alias("CntofCstID")).filter(col("CntofCstID")>1)
display(df_count_nullandextracst_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations using Spark SQL 

# COMMAND ----------

# MAGIC %sql
# MAGIC select  cst_id,cst_key,trim(cst_firstname),trim(cst_lastname),
# MAGIC case when upper(cst_marital_status)= 'S' then 'Single'
# MAGIC      when upper(cst_marital_status) = 'M' then 'Married'
# MAGIC      else 'n/a' 
# MAGIC END cst_martial_status,
# MAGIC case when upper(trim(cst_gndr)) = 'F' then 'Female'
# MAGIC    when upper(trim(cst_gndr)) = 'M' then 'Male'
# MAGIC    else 'n/a'
# MAGIC end cst_gndr,
# MAGIC cst_create_date
# MAGIC  from 
# MAGIC (select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag from bronze.crm_cust_info where cst_id is not null) a 
# MAGIC where flag=1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations using PySpark 

# COMMAND ----------

from pyspark.sql.functions import count, col, row_number, trim, upper, when
from pyspark.sql.window import Window

part = Window.partitionBy(col("cst_id")).orderBy(col("cst_create_date").desc())

df_new = df.withColumn("Flag", row_number().over(part)) \
 .withColumn(
    "cst_marital_status",
    when(upper(col("cst_marital_status")) == 'S','Single').when(upper(col("cst_marital_status")) == 'M','Married').otherwise("NA")
).withColumn("cst_gndr",when(upper(trim(col("cst_gndr")))=='F','Female').when(upper(trim(col("cst_gndr")))=='M','Male').otherwise("NA")).filter((col("Flag")==1) & (col("cst_id").isNotNull()))
 
display(df_new)

# COMMAND ----------

part = Window.partitionBy(col("cst_id")).orderBy(col("cst_create_date").desc())

df_new = df.withColumn("Flag", row_number().over(part)) \
 .withColumn(
    "cst_marital_status",
    when(upper(col("cst_marital_status")) == 'S','Single').when(upper(col("cst_marital_status")) == 'M','Married').otherwise("NA")
).withColumn("cst_gndr",when(upper(trim(col("cst_gndr")))=='F','Female').when(upper(trim(col("cst_gndr")))=='M','Male').otherwise("NA")).filter((col("Flag")==1) & (col("cst_id").isNotNull()))
 
display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Transformations using SelecExpr (Like Spark SQL with in SelectExpr)

# COMMAND ----------

from pyspark.sql.functions import count, col, row_number, trim, upper, when, current_timestamp
from pyspark.sql.window import Window

df_selectExpr = df.selectExpr("cst_id","cst_key","trim(cst_firstname) as cst_firstname",
    "trim(cst_lastname) as cst_lastname","case when upper(cst_marital_status)='S' then 'Single' when upper(cst_marital_status)='M' then 'Married' else 'NA' end as cst_marital_status",
    "case when upper(trim(cst_gndr))='F' then 'Female' when upper(trim(cst_gndr))='M' then 'Male' else 'NA' end as cst_gndr",
    "current_timestamp as dwh_create_date ",
    "row_number() over(partition by cst_id order by cst_create_date desc) as flag").where((col("flag") == 1) & (col("cst_id").isNotNull())).select(["cst_id","cst_key","cst_firstname","cst_lastname","cst_marital_status","cst_gndr","dwh_create_date"])

display(df_selectExpr.where((col("cst_id") == 29483)))


# COMMAND ----------

# MAGIC %md
# MAGIC ### using selctExpr and col and Expr
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count, col, row_number, trim, upper, when, expr
from pyspark.sql.window import Window

window_spec = Window.partitionBy(col("cst_id")).orderBy(col("cst_create_date").desc())

df_select = df.select(
    col("cst_id"),
    col("cst_key"),
    trim(col("cst_firstname")).alias("cst_firstname"),
    trim(col("cst_lastname")).alias("cst_lastname"),
    expr("case when upper(cst_marital_status)='S' then 'Single' when upper(cst_marital_status)='M' then 'Married' else 'NA' end").alias("cst_marital_status"),
    expr("case when upper(trim(cst_gndr))='F' then 'Female' when upper(trim(cst_gndr))='M' then 'Male' else 'NA' end").alias("cst_gndr"),current_timestamp().alias("dwh_crate_date"),
    row_number().over(window_spec).alias("flag")
).where((col("flag") == 1) & (col("cst_id").isNotNull()))

display(df_select)

# COMMAND ----------

df_selectExpr.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("dbfs:/FileStore/tables/warehouse/source_crm/silvercust")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.crm_cust_info
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/FileStore/tables/warehouse/source_crm/silvercust";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.crm_cust_info limit 2

# COMMAND ----------


