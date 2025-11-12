# Databricks notebook source
#Add Parameter

dbutils.widgets.text("DeptName","","Department Name")

# COMMAND ----------

# Get Value to Parameter

_dept = dbutils.widgets.get("DeptName")

print(_dept)

# COMMAND ----------

# MAGIC %md
# MAGIC ## There is another way for Below using Where 
# MAGIC
# MAGIC filterdf = df.where((upper(col("department")) == upper(lit(_dept))) & (col("active_record") == lit("1")))
# MAGIC
# MAGIC or
# MAGIC
# MAGIC filterdf = df.where(f"upper(department) = upper('{_dept}') AND active_record = '1'")
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,upper,lit
df = spark.read.csv("/databricks-datasets/retail-org/company_employees/company_employees.csv",header=True)
filterdf = df.filter(upper(col("department"))== upper(lit(_dept)))
#display(filterdf)

# COMMAND ----------

## We are going to write the data when we have count more than 1
_count = filterdf.count()
if _count > 0:
    #filterdf.write.mode("overwrite").saveAsTable(f"dev.bronze.dep_{_dept}")
    print(f"Write Completed for dept_{_dept}")
else:
    print("No Records available to create or write into Table")

# COMMAND ----------

# to pass values to Parent Notebook
dbutils.notebook.exit(_count)

# COMMAND ----------


