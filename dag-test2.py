### dag die een file inleest en hier vervolgens 'text added' aan toevoegt

import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def read_file():
    myfile = "/home/gekep/Documents/Code/dag-text.txt"
    
    with open(myfile, "r") as f:
        data = f.read().replace('\n', '')
        print(data)
        f.close()
    return data

def append_file():
    myfile = "/home/gekep/Documents/Code/dag-text.txt"
    
    with open(myfile, "a") as f:
        f.write("text added \n")
        f.close()
    return 'text added'


default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2019, 12, 15, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('dag-test2',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         ) as dag:

    opr_start = BashOperator(task_id = 'start',
                            bash_command = 'echo "Start proces!"')
    
    opr_read = PythonOperator(task_id = 'read_file',
                                python_callable = read_file)

    opr_changed = BashOperator(task_id = 'file_read',
                                bash_command = 'echo "file read!"')
    
    opr_append = PythonOperator(task_id = 'append_file',
                                python_callable = append_file)

    opr_end = BashOperator(task_id = 'end',
                            bash_command = 'echo "Done proces!"')

    
opr_start >> opr_read >> opr_changed >> opr_append >> opr_end
