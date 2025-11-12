-- Databricks notebook source
use database f1_raw

-- COMMAND ----------

select * from driver

-- COMMAND ----------

select *,name.forename,name.surname from driver

-- COMMAND ----------

select date_format(dob,"dd-MMM-yyyy" )from driver

-- COMMAND ----------

select date_add(dob,2)   from driver

-- COMMAND ----------


