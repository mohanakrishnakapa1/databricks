# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## By Deafult AQE will decide the join here as we haven't disabled the AQE.
# MAGIC ### Given all examples related TempViews and how to create columns using existing columns.etc
# MAGIC ## Hints: We can decide Join startegy and repartitioning and coalesce and rebalance etc. It can improve the performance 
# MAGIC ### Given examples below
# MAGIC
# MAGIC ### Metastore in realtime scenarios can be Hive or Database ..etc
# MAGIC
# MAGIC

# COMMAND ----------

# Reading with Schema
_schema = "first_name string, last_name string, job_title string, dob string, email string,phone string, salary double, department_id int"
df_emp = spark.read.format("csv").option("header",True).schema(_schema).load("/FileStore/tables/day7/employee_records.txt")
 
# Read DEPT CSV data
_dept_schema = "department_id int, department_name string, description string, city string, state string, country string"
df_dept = spark.read.format("csv").option("header",True).schema(_dept_schema).load("/FileStore/tables/day7/department_data.txt")

# COMMAND ----------

#Spark Catalog (Metadata)  - in-memory/Hive. In Databricks default set to Hive/ Standalone Spark Environment it will be set to in-memory
spark.conf.get("spark.sql.catalogImplementation")

# COMMAND ----------

#Show Database
db = spark.sql("show databases")
db.show()

# COMMAND ----------

#Show Tables
spark.sql("show tables in default").show()

# COMMAND ----------

#Create Tables or TempView

df_emp.createOrReplaceTempView("emp_view")
df_dept.createOrReplaceTempView("dept_view")


# COMMAND ----------

#View Data from table
spark.sql("""select * from emp_view""")

# COMMAND ----------

# apply Filters
spark.sql("""select * from emp_view where department_id=4""").show()


# COMMAND ----------

# add column
emp_new = spark.sql("""select e.*,date_format(dob,'yyyy') as dob_year from emp_view e where department_id=4""")


# COMMAND ----------

## Create temp view based on updated dataframe using Spark Sql
emp_new.createOrReplaceTempView("emp_newtemp") 


# COMMAND ----------

#Join emp and detp -- Hints

spark.sql(""" 
          select e.*,d.department_name from emp_view e left outer join dept_view d on e.department_id=d.department_id        
          """).show()

# COMMAND ----------

#change join to Shuffle Merge from Broadcast
spark.sql(""" 
          select /*+SHUFFLE_MERGE(e) */ e.*,d.department_name from emp_view e left outer join dept_view d on e.department_id=d.department_id        
          """).show()

# COMMAND ----------


## Change Join to BroadCast for department
spark.sql(""" 
          select /*+BROADCAST(d) */ e.*,d.department_name from emp_view e left outer join dept_view d on e.department_id=d.department_id        
          """).show()

# COMMAND ----------

emp_final = spark.sql(""" 
          select /*+BROADCAST(d) */ e.*,d.department_name from emp_view e left outer join dept_view d on e.department_id=d.department_id        
          """)
# Write the data as Table

emp_final.write.format("parquet").saveAsTable("emp_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the tables Once again and see the difference as we have see some Temp Tables and the table saved permeanant Table ***

# COMMAND ----------

## Reas the tables Once again and see the difference as we have see some Temp Tables and the table saved permeanant Table

spark.sql("show tables in default").show()

# COMMAND ----------

# Read table 

emp_tab = spark.sql("select * from emp_final")
emp_tab.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist the metadata in Hive.. This is Default in Databricks. Use this configuration in Spark session .enableHiveSupport()

# COMMAND ----------

# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Spark SQL")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "/data/output/spark-warehouse")
    .getOrCreate()
)

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show details of Metadata

# COMMAND ----------

spark.sql("describe extended emp_final").show()

# COMMAND ----------


