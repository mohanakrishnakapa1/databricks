# Databricks notebook source
raw_folder_path='/FileStore/tables'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls f{raw_folder_path}

# COMMAND ----------

def rearrange_cols(inputdf,partiton_column):
    col_lst=[]
    for i in inputdf.columns:
        if i!="race_id":
            col_lst.append(i)
    col_lst.append(partiton_column)
    outputdf=inputdf.select(lst)
    return outputdf

# COMMAND ----------

def overwrite_partitions(inputdf,partition_column,database_name,table_name):
    outputdf=rearrange_cols(inputdf,partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if(spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
        outputdf.write.mode("overwrite").insertInto(f"{database_name}.{table_name}")
    else:
        outputdf.write.mode("overwrite").format("parquet").partitionBy("partition_column").saveAsTable(f"{database_name}.{table_name}")



# COMMAND ----------

/includes/configuration
