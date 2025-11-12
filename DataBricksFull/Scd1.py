# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table bronze.emp(
# MAGIC   emp_id int,
# MAGIC   ename string,
# MAGIC   deptid string,
# MAGIC   salary float
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.emp VALUES(1000,'Mohan','D101',10000);
# MAGIC INSERT INTO bronze.emp VALUES(1001,'Radha','D102',12000);
# MAGIC INSERT INTO bronze.emp VALUES(1002,'Sumanth','D101',15000);
# MAGIC INSERT INTO bronze.emp VALUES(1003,'Siri','D102',8000);
# MAGIC INSERT INTO bronze.emp VALUES(1004,'Subbaiah','D104',13000);
# MAGIC INSERT INTO bronze.emp VALUES(1005,'Laxmi','D103',5000);
# MAGIC INSERT INTO bronze.emp VALUES(1006,'Tanvi','D101',5000);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table bronze.emp_updates(
# MAGIC   emp_id int,
# MAGIC   ename string,
# MAGIC   deptid string,
# MAGIC   salary float
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.emp_updates VALUES(1000,'Mohan','D101',8000);
# MAGIC INSERT INTO bronze.emp_updates VALUES(1001,'Radha','D102',2000);
# MAGIC INSERT INTO bronze.emp_updates VALUES(1002,'Sumanth','D101',1000);
# MAGIC INSERT INTO bronze.emp_updates VALUES(1003,'Siri','D102',6000);
# MAGIC INSERT INTO bronze.emp_updates VALUES(1004,'Subbaiah','D104',9000);
# MAGIC INSERT INTO bronze.emp_updates VALUES(1005,'Laxmi','D103',2000);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update the Existing records and Insert new records

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.emp_updates u
# MAGIC USING bronze.emp e
# MAGIC ON e.emp_id=u.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC update set u.salary = e.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.emp_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete the records when not matched
# MAGIC ### This record is not available in Source Emp may be the employee moved to different organization

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.emp_updates VALUES(1007,'Kittu','D102',6000)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.emp_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.emp_updates u
# MAGIC USING bronze.emp e
# MAGIC ON e.emp_id=u.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC update set u.salary = e.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC DELETE 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## To Soft delete or Is_ative = 'N' when the record is not available in source (Instead of delete)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.emp_updates VALUES(1007,'Kittu','D102',6000)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze.emp_updates ADD COLUMNS (IS_Active STRING);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.emp_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.emp_updates u
# MAGIC USING bronze.emp e
# MAGIC ON e.emp_id=u.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC update set u.salary = e.salary,u.IS_Active='Y'
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT 
# MAGIC (
# MAGIC   emp_id,
# MAGIC   ename,
# MAGIC   deptid,
# MAGIC   salary,
# MAGIC   IS_active
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC   e.emp_id,
# MAGIC   e.ename,
# MAGIC   e.deptid,
# MAGIC   e.salary,
# MAGIC   'Y'
# MAGIC )
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC Update SET u.IS_Active = 'N' 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.emp_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## WE can add extra condition merge as well check belw WHEN NOT MATCHED BY SOURCE THEN 
# MAGIC
# MAGIC           %sql
# MAGIC           MERGE INTO bronze.emp_updates u
# MAGIC           USING bronze.emp e
# MAGIC           ON e.emp_id=u.emp_id
# MAGIC           WHEN MATCHED THEN
# MAGIC           update set u.salary = e.salary,u.IS_Active='Y'
# MAGIC           WHEN NOT MATCHED THEN
# MAGIC           INSERT 
# MAGIC           (
# MAGIC             emp_id,
# MAGIC             ename,
# MAGIC             deptid,
# MAGIC             salary,
# MAGIC             IS_active
# MAGIC           )
# MAGIC           VALUES
# MAGIC           (
# MAGIC             e.emp_id,
# MAGIC             e.ename,
# MAGIC             e.deptid,
# MAGIC             e.salary,
# MAGIC             'Y'
# MAGIC           )
# MAGIC           WHEN NOT MATCHED BY SOURCE THEN AND u.IS_Active='Y'
# MAGIC           Update SET u.IS_Active = 'N' 
# MAGIC           ;

# COMMAND ----------


