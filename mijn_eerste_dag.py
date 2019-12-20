import datetime as dt 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

###
### bepaal de default arguments default_args = {}
###
default_args = {
    #define owner of the workflow
    'owner': 'me',
    #sinds wanneer is de workflow geldig? Oftewel de startdatum
    #Let op: year, month, day
    'start_date': dt.datetime(2019, 12, 15),
    #retries bepaalt het aantal keer dat de DAG mag runnen in het geval dat het niet succesvol is
    'retries': 1,
    #bepaalt de delay tussen een retry. In dit geval 5 minuten
    'retry_delay': dt.timedelta(minutes=5)
}

###
### creeer de DAG with DAG() as dag:
###

###
### definieer eventuele python functies voor de PythonOperators
###

def print_world():
    print('world')


with DAG(
    #de naam van de dag
    'mijn_eerste_dag',
    #de default args terugkoppelen
    default_args = default_args,
    #specificeer het interval van de run: in dit geval op elk uur 0, oftewel elke dag om 00:00
    #dit is een cron schedule expression
    #je kan ook strings als '@daily' of '@hourly' gebruiken
    schedule_interval='0 * * * *',
) as dag:

    ###
    ### creeer de taken
    ###
    
    #definieer of een taak een BashOperator of PythonOperator is
    #geef elke taak een task_id
    #in het geval van BashOperator: definieer de bash_command
    #let op: bij een PythonOperator hoort een python_callable die verwijst naar een Python functie

    start1 = BashOperator(task_id = 'start1',
                                bash_command = 'echo "start 1"')
    
    start2 = BashOperator(task_id = 'start2',
                                bash_command = 'echo "start 2"')
    
    sleep = BashOperator(task_id = 'sleep',
                        bash_command = 'sleep 2')
    
    samen12 = BashOperator(task_id = 'samen12',
                                bash_command = 'echo "samenkomst 1 en 2"')
    
    los1 = BashOperator(task_id = 'los1',
                                bash_command = 'echo "los 1"')

    los2 = BashOperator(task_id = 'los2',
                                bash_command = 'echo "los 2"')
    
    eind = BashOperator(task_id = 'eind',
                                bash_command = 'echo "eind"')

    

###
### bepaal de volgorde van de taken (construeer de DAG)
###

[start1, start2] >> sleep >> samen12 >> [los1, los2] >> eind