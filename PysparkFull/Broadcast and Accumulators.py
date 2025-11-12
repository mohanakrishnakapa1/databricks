# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### If we crate a variable and pass to executors then variable will passed to each task after serialization and deserilization happen row by row. If we have simple variable fine, If we have table values or Machinelearning Model then we will have performance issues
# MAGIC
# MAGIC
# MAGIC ## So the solution is Brodcast variable, Once we create a broadcast variable then it will use all executors and return the result set and they don't need to shuffle data.
# MAGIC
# MAGIC *** this why Braodcast variable is also called as distributed shared variable***#Broadcast the variable
# MAGIC
# MAGIC sc = spark.sparkContext
# MAGIC broadcast_dept_names = sc.broadcast(dept_names)
# MAGIC
# MAGIC Check the value of BC Variable
# MAGIC
# MAGIC broadcast_dept_names.value
# MAGIC
# MAGIC broadcast_dept_names.value.get(1)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Variable (Lookup)
dept_names = {1 : 'Department 1', 
              2 : 'Department 2', 
              3 : 'Department 3', 
              4 : 'Department 4',
              5 : 'Department 5', 
              6 : 'Department 6', 
              7 : 'Department 7', 
              8 : 'Department 8', 
              9 : 'Department 9', 
              10 : 'Department 10'}

# COMMAND ----------

_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id int"

emp = spark.read.format("csv").schema(_schema).option("header", True).load("/FileStore/tables/pysparkfull/day19/employee_records.txt")

# COMMAND ----------

#Broadcast the variable
sc = spark.sparkContext

broadcast_dept_names = sc.broadcast(dept_names)

# COMMAND ----------

#check Value of BC variable
broadcast_dept_names.value
broadcast_dept_names.value.get(1)

# COMMAND ----------

# Create UDF to return Department name

from pyspark.sql.functions import udf,col

@udf
def get_dept_name(dept_id):
    return broadcast_dept_names.value.get(dept_id)

emp_final = emp.withColumn("dept_name",get_dept_name(col("department_id")))

# COMMAND ----------

emp_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Accumulator
# MAGIC
# MAGIC ## Consider we have have to calculate total salary of Department 6 , Here problem is the data will passed to different executors and later we have to call all the data executors and do the sum, So there is shuffle involved.
# MAGIC
# MAGIC ## So accumulators will process the row by row in distributed process in Executors and do the sum, Once the all the variables updated at executors then we can get the final sum.
# MAGIC

# COMMAND ----------

# calculate total salary of Department 6

from pyspark.sql.functions import sum
emp.where("department_id=6").groupBy("department_id").agg(sum("salary").cast("long")).show()

# COMMAND ----------

# Define Accumulators

dept_sal = sc.accumulator(0)

# COMMAND ----------

#Use foreach -- to go to all the records for DF

def calculate_dalary(department_id,salary):

    if department_id == 6:
        dept_sal.add(salary)

emp.foreach(lambda row : calculate_dalary(row.department_id,row.salary))

# COMMAND ----------

#View Total sal
dept_sal.value

# COMMAND ----------


