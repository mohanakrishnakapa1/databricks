# Databricks notebook source
# MAGIC %md
# MAGIC ## run the Child Notebook. 1.NoteBookName 2.Timeout Seconds 3.Parameter Values
# MAGIC

# COMMAND ----------

dbutils.notebook.run("Orchestation_Jobs_sche_child",600,{"DeptName":"Marketing"})

# COMMAND ----------

print(f"Child Notebook Count{_count})
