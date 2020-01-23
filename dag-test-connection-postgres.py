# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 16:17:36 2020

@author: alina.mihalache

DAG for testing connection to db in postgres
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 20),
    'retries': 1,
}

# Set Schedule
schedule_interval = None

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    dag_id='dag-test-connection-postgres',
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

#SQL query used for the test
insert_row_query = """insert into test(name, age)
                values ('Bdfds sdf', 22);"""
            
# Define tasks
t1 = PostgresOperator(
    task_id='test_connection',
    dag=dag,
    sql = insert_row_query, 
    postgres_conn_id='postgres_my_db') # postgres_my_db is made in the Airflow UI with Admin > Connections > Create
                                       # this connection contains the name of the db, host and login credentials

t1