# Databricks notebook source
# MAGIC %md
# MAGIC ### Best Technique to optimize performance on low Cardinality columns like orderID or EmployessID 
# MAGIC ### OPTIMIZE <Table NAme> ZORDERBY(ColumnNAme)
# MAGIC ### If we used the Zorder with Combination of Parttion which will be very effient Then we can increase the performance 

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/definitive-guide/"))

# COMMAND ----------

# Reading with Schema
_schema = "first_name string, last_name string, job_title string, dob string, email string,phone string, salary double, department_id int"
df_emp = spark.read.format("csv").option("header",True).schema(_schema).load("/FileStore/tables/day7/employee_records.txt")
df_emp.repartition(16).write.format("delta").option("path","/FileStore/tables/Zorder").mode("overwrite").saveAsTable("emp_records")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Zorder"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(department_id),max(department_id),_metadata.file_name from emp_records
# MAGIC group by _metadata.file_name
# MAGIC order by min(department_id)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 64*1024*8)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE emp_records ZORDER BY (department_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(department_id),max(department_id),_metadata.file_name from emp_records
# MAGIC group by _metadata.file_name
# MAGIC order by min(department_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from emp_records where department_id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_records

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE emp_records ZORDER BY (job_title,department_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select job_title,min(department_id),max(department_id),_metadata.file_name from emp_records
# MAGIC group by job_title,_metadata.file_name
# MAGIC order by min(department_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### the Below query now reads more no of files as we have zorderby (job_title,deepartment_id).

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from emp_records where department_id =1 

# COMMAND ----------

# MAGIC %md
# MAGIC ### If we use the Job_title as partition and department_id as Zorder Key then we will get more Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct job_title from emp_records

# COMMAND ----------

df_emp.write.format("delta").option("path","/FileStore/tables/ZorderParttion").partitionBy("job_title").mode("overwrite").saveAsTable("emp_recordsPart")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/ZorderParttion/job_title=Tree surgeon/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/ZorderParttion/job_title=Tree surgeon/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from emp_recordsPart where department_id=1 and job_title="Tree surgeon"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trying only one Job_title Zorder we can Zorder hole table if we want

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE emp_recordsPart where job_title="Tree surgeon" ZORDER BY (department_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from emp_recordsPart where department_id=1 and job_title="Tree surgeon"

# COMMAND ----------


