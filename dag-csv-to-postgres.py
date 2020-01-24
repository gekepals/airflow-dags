# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 16:17:36 2020

@author: alina.mihalache

DAG to copy data from CVS to db in postgres
"""

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 20),
    'retries': 1,
}

# Set Schedule: none
schedule_interval = None

# Variables
# the path is saved as a variable in airflow {key: 'csv_path', value:'<your_path>'}
csv_path = Variable.get("csv_path")

# Python function
def etl():
    conn = PostgresHook(postgres_conn_id='postgres_my_db').get_conn()
    cur = conn.cursor()
    with open(csv_path + 'test_csv_to_pg.csv', 'r') as f:
        next(f)
        cur.copy_from(f, 'test', sep = '\t')
    conn.commit()   
    cur.close()
    conn.close()
    
# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    dag_id='dag-csv-to-postgres',
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

#SQL query used for the test
insert_row_query = """COPY test(name, age) 
FROM {} DELIMITER ';' CSV HEADER;""".format("'"+csv_path + 'test_csv_to_pg.csv'+"'")
            
# Define tasks
t1 = PythonOperator(
    task_id='csv-to-postgres',
    dag=dag,
    python_callable=etl) 

t1