from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

pyjobs = '/opt/airflow/dags/pyjobs'
# Define your DAG
with DAG(
        dag_id = 'dag_final_project',
        schedule_interval=None,  # Define your desired schedule interval
        start_date=datetime(2023, 9, 25),
        catchup=False  # Set to True if you want historical DAG runs
        ) as dag:

    # Define tasks using BashOperator
    task1 = BashOperator(
        task_id='get_data_from_api',
        bash_command=f'python {pyjobs}/stagging_api_mysql.py',
        
    )

    task2 = BashOperator(
        task_id='generate_dim',
        bash_command=f'python {pyjobs}/dim_datamart_mysql_psql.py',
        
    )
    task3 = BashOperator(
        task_id='insert_district_daily',
        bash_command=f'python {pyjobs}/fact_datamart_district_daily_mysql_psql.py',
        
    )
    task4 = BashOperator(
        task_id='insert_province_daily',
        bash_command=f'python {pyjobs}/fact_datamart_province_daily_mysql_psql.py',
        
    )

    # Set task dependencies
    task1 >> task2  >> [task3,task4]# task1 must run before task2

 
