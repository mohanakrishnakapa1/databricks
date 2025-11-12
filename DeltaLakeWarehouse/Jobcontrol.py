def insert_log(spark,schema_name: str, table_name: str, max_timestamp: str,rundate: str) -> bool:
  """
  This function inserts a log record into the table 'log' in the schema 'schema_name'
  """
  try:
      _data = [
          [schema_name, table_name, max_timestamp,rundate]
               ]
      _columns = ['schema_name', 'table_name', 'max_timestamp','rundate']
      df = spark.createDataFrame(_data, _columns)

      #Create necessary Columns
      df_processed = df.selectExpr('schema_name','table_name','to_timestamp(max_timestamp) as max_timestamp','rundate','current_timestamp() as insert_dt')

      #write into Job Control
      df_processed.write.format('delta').mode('append').saveAsTable('warehouse.edw.job_control')
      return True
  except Exception as e:
      print(e)

#Get the Max_timestamp for a table

def get_max_timestamp(spark,schema_name: str, table_name: str):
    """
    This function returns the max timestamp for a table in the schema 'schema_name'
    """
    try:
        df = spark.sql(f"SELECT max(max_timestamp) as MX FROM warehouse.edw.job_control WHERE schema_name = '{schema_name}' AND table_name = '{table_name}'")
        
        # check the row count. We have a Max timestampe value exists or not. else the load is full

        if df.filter(df.MX.isNotNull()).count() > 0:
            return str(df.take(1)[0][2])
        else:
            return '1900-01-01 00:00:00.000000'
    except Exception as e:
        return None

# Remove table data based on rundate

def delete_table_data(spark,schema_name: str, table_name: str, rundate: str) -> bool:
    """
    This function deletes the data from the table 'table_name' in the schema 'schema_name' based on the rundate
    """
    try:
        spark.sql(f"DELETE FROM {schema_name}.{table_name} WHERE rundate = '{rundate}'")
        return True
    except Exception as e:
        print(e)
        return False

if __name__ == "__main__":
    insert_log('warehouse.edw','dim_store_stg','2023-01-01 00:00:00.000000','20230101')
    get_max_timestamp('warehouse.edw','dim_store_stg')
    delete_table_data('warehouse.edw','dim_store_stg','20230101')
