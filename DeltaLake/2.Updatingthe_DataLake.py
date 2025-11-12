# Databricks notebook source
# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set points=26
# MAGIC where driverId=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed where driverId=1;

# COMMAND ----------

#Predicate SQl
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark,"/mnt/delta/demo/results_managed")
deltaTable.update("position <=10",{"points":"21-position"})


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ######Deleting

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed where position<=10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

#Predicate SQl Delete
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark,"/mnt/delta/demo/results_managed")
deltaTable.delete("position =11")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------


