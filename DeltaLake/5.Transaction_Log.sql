-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Log will create json and parquet file for every transation. Every 10 Transactions Json file will convert as Parquet file so that datbricks efficiently manges the logs

-- COMMAND ----------

create or replace table f1_demo.driver_log(
driverId Int,
dob Date,
forename STRING,
surname string,
createdDate DATE,
updatedDate DATE
)
using delta

-- COMMAND ----------

insert into f1_demo.driver_log values(1,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp)

-- COMMAND ----------

insert into f1_demo.driver_log values(2,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);
insert into f1_demo.driver_log values(3,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);
insert into f1_demo.driver_log values(4,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);
insert into f1_demo.driver_log values(6,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);
insert into f1_demo.driver_log values(7,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);
insert into f1_demo.driver_log values(8,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);
insert into f1_demo.driver_log values(9,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);
insert into f1_demo.driver_log values(10,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);
insert into f1_demo.driver_log values(11,current_timestamp,"mohana","kapa",current_timestamp,current_timestamp);

-- COMMAND ----------


