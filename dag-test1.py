### dag die een file inleest, deze bewerkt en teruggeeft, en vervolgens weer terugbewerkt

import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import re


def change_to_789():
    myfile = "/home/gekep/Documents/Code/dag-text.txt"

    with open(myfile, "w") as filetowrite:
        filetowrite.write('789')
        filetowrite.close()
    return '789'

def change_to_123():
    myfile = "/home/gekep/Documents/Code/dag-text.txt"

    with open(myfile, "w") as filetowrite:
        filetowrite.write('123')
        filetowrite.close()
    return '123'


default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2019, 12, 15, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('dag-test1',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         ) as dag:

    opr_start = BashOperator(task_id = 'start',
                            bash_command = 'echo "Start proces!"')
    
    opr_change1 = PythonOperator(task_id = 'change_to_789',
                                python_callable = change_to_789)

    opr_changed = BashOperator(task_id = 'changed_text',
                                bash_command = 'echo "Text file changed"')
    
    opr_change2 = PythonOperator(task_id = 'change_to_123',
                                python_callable = change_to_123)

    opr_end = BashOperator(task_id = 'end',
                            bash_command = 'echo "Done proces!"')

    
opr_start >> opr_change1 >> opr_changed >> opr_change2 >> opr_end