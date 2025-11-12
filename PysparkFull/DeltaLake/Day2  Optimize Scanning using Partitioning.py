# Databricks notebook source
# MAGIC %md
# MAGIC ## So Data scanning will be less if we use the partitioning. So it will gives us lot of performance.
# MAGIC ### Be sure about Partition column, Can lead small file issue if we use high cardinality columns like OrderId ..etc
# MAGIC ### Use only Higher Order columns like(low cardinality) Country,State..etc

# COMMAND ----------

# Reading with Schema
_schema = "first_name string, last_name string, job_title string, dob string, email string,phone string, salary double, department_id int"
df_emp = spark.read.format("csv").option("header",True).schema(_schema).load("/FileStore/tables/day7/employee_records.txt")

# COMMAND ----------

df_emp.write.format("csv").partitionBy("department_id").option("path","/FileStore/tables/emppartition").saveAsTable("emp_part_tab")

# COMMAND ----------

emp_part_table = spark.read.table("emp_part_tab")

# COMMAND ----------

#This will scan all the table
emp_part_table.count()

# COMMAND ----------

#if we query based on partition column then scanning will much less
emp_part_table.where("department_id=3 and salary>50000").count()

# COMMAND ----------

#If we remove the department_id from filter then query will scan all the rows.Go and check the DAG
emp_part_table.where(" salary>50000").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can apply this on even SQL 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from emp_part_tab where department_id=3 and salary>50000

# COMMAND ----------


