-- Databricks notebook source
desc history f1_demo.results_managed

-- COMMAND ----------

select * from f1_demo.results_managed version as of 2;

-- COMMAND ----------

select * from f1_demo.results_managed timestamp as of '2023-10-18T18:38:14.000+0000';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Pyspark Style of Doing

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format("delta").option("timestampasof",'2023-10-18T18:38:14.000+0000').load("/mnt/delta/demo/results_managed")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Vacum to remove the Histroy it will keep the 7 Days

-- COMMAND ----------

VACUUM f1_demo.results_managed

-- COMMAND ----------

select * from f1_demo.results_managed timestamp as of '2023-10-18T18:38:14.000+0000';

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ######Still We see the Histroy Data as 7 days is the Retention period. Use 0 Hours to delete Forcefully

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled=false;
VACUUM f1_demo.results_managed RETAIN 0 HOURS

-- COMMAND ----------

##This is fine its just audit but we can't see the Data
desc history f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######The Below one gives error as we deleted Histroy

-- COMMAND ----------

select * from f1_demo.results_managed timestamp as of '2023-10-18T18:38:14.000+0000';

-- COMMAND ----------

select * from f1_demo.results_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##############Delete Some Data

-- COMMAND ----------

delete from f1_demo.results_managed where driverId=841

-- COMMAND ----------

describe history f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #######Retrive the Record without going back to Source

-- COMMAND ----------

MERGE INTO f1_demo.results_managed tgt
using f1_demo.results_managed VERSION AS OF 9 src
ON (tgt.driverId=src.driverId)
when NOT MATCHED THEN
Insert *

-- COMMAND ----------

desc history f1_demo.results_managed

-- COMMAND ----------

select * from f1_demo.results_managed where driverId=841

-- COMMAND ----------


