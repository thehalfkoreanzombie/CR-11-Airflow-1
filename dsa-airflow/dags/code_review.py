# Essential imports
from datetime import timedelta, datetime
from airflow import DAG
import random

# Operators
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago # handy scheduling tool

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

def read_file():
    with open('/opt/airflow/code_review.txt', 'r') as name_file:
        file_read = name_file.read()
        print(f"Greetings {file_read}, fellow earthling")

def random_apple():
    print(f"This apple is a {random.choice(APPLES)} apple")

default_args = {
    'start_date': days_ago(2), 
    'schedule_interval': timedelta(days=1), 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
}

with DAG(
    'apple_picking',
    description='A DAG to echo a string to a filename and read it',
    default_args=default_args, 
) as dag:

    echo_to_file_task = BashOperator(
        task_id='echo_to_file_task',
        bash_command='echo "Alex Wallace" >> /opt/airflow/code_review.txt'
    )        

    greeting_task = PythonOperator(
        task_id = 'greeting_task',
        python_callable = read_file
    )

    apple_string_task = BashOperator(
        task_id = 'apple_string_task',
        bash_command = 'echo "Picking three random apples"'
    )

    apple_task_1 = PythonOperator(
        task_id = 'apple_task_1',
        python_callable = random_apple
    )

    apple_task_2 = PythonOperator(
        task_id = 'apple_task_2',
        python_callable = random_apple
    )
    
    apple_task_3 = PythonOperator(
        task_id = 'apple_task_3',
        python_callable = random_apple
    )

    end_task = EmptyOperator(
        task_id = 'end_task',
        dag = dag
    )

    echo_to_file_task >> greeting_task >> apple_string_task >> [apple_task_1, apple_task_2, apple_task_3] >> end_task