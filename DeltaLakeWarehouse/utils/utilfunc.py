from datetime import datetime, timedelta
from pyspark.sql.types import StringType
import json

# Date Utility to generate Date data

def date_data(start_run_dt: str = '20230101',num_years: int = 1) -> list:
    _data = []
    start_date = datetime.strptime(start_run_dt, '%Y%m%d')
    _data.append([start_date.strftime('%Y-%m-%d'),start_date.strftime('%d'),start_date.strftime('%m'),start_date.strftime('%Y'),start_date.strftime('%A')])
    for i in range(0,num_years*365):
        _next_date = start_date + timedelta(days=i+1)
        _data.append([_next_date.strftime('%Y-%m-%d'),_next_date.strftime('%d'),_next_date.strftime('%m'),_next_date.strftime('%Y'),_next_date.strftime('%A')])

    return _data

def get_rundate():
    try:
        with open('/Workspace/Users/mohanakapa@outlook.com/databricks/DeltaLakeWarehouse/config/run_config.txt','r') as f:
            data = json.load(f)
            return data['rundate']
    except Exception as e:
        print(e)
        return '19000101'