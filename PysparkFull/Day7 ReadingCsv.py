# Databricks notebook source
df = spark.read.format("csv").load("/FileStore/tables/day7/emp_csv.txt")

# COMMAND ----------

#Read with Header True and InferSchema options to import DataTypes
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("/FileStore/tables/day7/emp_csv.txt")

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/FileStore/tables/day7/empnew"

# COMMAND ----------

# Reading with Schema
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"
df_schema = spark.read.format("csv").option("header",True).schema(_schema).load("/FileStore/tables/day7/emp_csv.txt")

# COMMAND ----------

df_schema.show()

# COMMAND ----------

#Csv Modes to Hadle Bad Records
#1.PERMISSIVE - Default Mode
#2.FAILFAST 
#3.DROPMALFORMED

#Permisive put Null for corrupted Records
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date,_corrupt_record string"
df_Permisive = spark.read.format("csv").option("header",True).schema(_schema).load("/FileStore/tables/day7/empnew/emp_new.txt")
df_Permisive.show()

# COMMAND ----------

df_Permisive.where("_corrupt_record is not null").show()

# COMMAND ----------

#to change Corrupt_record column name 
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date,bad_record string"
df_badrecord = spark.read.format("csv").option("header",True).option("columnNameOfCorruptRecord","bad_record").schema(_schema).load("/FileStore/tables/day7/empnew/emp_new.txt")
df_badrecord.show()

# COMMAND ----------

#Drop bad records
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"
df_dropmalformed = spark.read.format("csv").option("header",True).option("mode","DROPMALFORMED").schema(_schema).load("/FileStore/tables/day7/empnew/emp_new.txt")

# COMMAND ----------

df_dropmalformed.show()

# COMMAND ----------

#Fails the execution 
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"
df_failfast = spark.read.format("csv").option("header",True).option("mode","FAILFAST").schema(_schema).load("/FileStore/tables/day7/empnew/emp_new.txt")
df_failfast.show()

# COMMAND ----------

#Multiple Options in one sigle Dictionary
_options ={
    "header":"true",
    "inferSchema":"true",
    "mode":"PERMISSIVE"
}

df_withMultioptions = spark.read.format("csv").options(**_options).load("/FileStore/tables/day7/empnew/emp_new.txt")
df_withMultioptions.show()

# COMMAND ----------

#1.check how to read Multiple csv Files at a time?
