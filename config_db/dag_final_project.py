from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define your DAG
dag = DAG(
    'my_bash_dag',
    schedule_interval=None,  # Define your desired schedule interval
    start_date=datetime(2023, 9, 25),
    catchup=False  # Set to True if you want historical DAG runs
)

# Define tasks using BashOperator
task1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello from Task 1"',
    dag=dag
)

task2 = BashOperator(
    task_id='task_2',
    bash_command='echo "Hello from Task 2"',
    dag=dag
)

# Set task dependencies
task1 >> task2  # task1 must run before task2

# You can add more tasks and define their dependencies as needed
