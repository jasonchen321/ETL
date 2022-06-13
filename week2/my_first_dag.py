#import the library
from datetime import timedelta
#The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
#Operators; we need this to write tasks
from airflow.operators.bash_operator import BashOperator
#This makes scheduling easy
from airflow.utils.dates import days_ago

default_args={
    'owner':'Jason',
    'start_date':days_ago(0),
    'email':['yoyoyo0321@hotmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag=DAG(
dag_id='my-first-dag',
default_args=default_args,
description='My first ETL DAG',
schedule_interval=timedelta(days=1)
)

#define the tasks

#define the first task named extract
extract=BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > extracted-data.txt'
    dag=dag
)

#define the second task named transform
transform_and_load=BashOperator(
    task_id='transform',
    bash_command='tr ":""," < extracted-data.txt > transformed-data.csv'
    dag=dag
)

# #define the third task named load
# load=BashOperator(
#     task_id='load',
#     bash_command='echo "load"',
#     dag=dag
# )

# task pipeline
extract >> transform_and_load

#-------------------------------------Terminal----------------------------------------#
#copy dag to AIRFLOW
#cp my_first_dag.py $AIRFLOW_HOME/dags

#list out all the existing DAGs in Airflow
#airflow dags list

#Verify that my-first-dag is a part of the output
#airflow dags list|grep "my-first-dag"

#Run the command below to list out all the tasks in my-first-dag.
#airflow tasks list my-first-dag

