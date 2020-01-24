# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 16:17:36 2020

@author: alina.mihalache

DAG for read and modify a CSV
"""

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import csv

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

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'dag-read-csv',
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

# Python function to read the csv 
def read_from_csv():
    with open(csv_path + 'test.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            print(row)


# Python function to write in the csv 
def write_to_csv():
    with open(csv_path + 'test.csv', mode='w') as csvfile:
        cwriter = csv.writer(csvfile, delimiter=',')
        cwriter.writerow('test2')
            
# Define tasks
t1 = PythonOperator(
    task_id='test_csv',
    dag=dag,
    python_callable = write_to_csv)

t2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t1 >> t2