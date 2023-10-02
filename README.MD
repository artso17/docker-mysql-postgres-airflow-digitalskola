# ETL Data Pipelines Using Apache Airflow-MySQL-PostgreSQL-Docker

## Project Brief
The Project aimed as final project in Data Engineer Bootcamp at Digital Skola to test my skills in ETL Data Pipelines end-to-end using Apache Airflow as orchestrator and scheduler, MySQL as Data Storage for Stagging Area, PostgreSQL as Data Warehouse, and all of them need to run on Docker container. Data Source retrieved from Application Programming Interface (API) and Data Warehouse has 5 Data Marts. Developing DAG with 4 tasks following the diagram below.

![final_project](https://github.com/artso17/docker-flask-api-ml/assets/78079780/8e58427d-06bc-4d9d-940b-33c818e2831a)


## Success Criteria
- Setup docker compose to run all tools and run it
- Develop Extract and Load to MySQL Stagging Area script
- Develop Transform and Load script to make 3 Data Marts Dimensional Tables with following specification:
  1. Province table
     - province_id
     - province_name
  1. District table
     - district_id
     - province_id
     - district_name
  2. Case table
     - Id
     - Status name (suspect, closecontact, probable, confirmation)
     - Status detail
- Develop Transform and Load script to make 2 Data Marts Fact Tables with following specification:
  1. Province Daily Table
     - Id (auto generate)
     - province_id
     - case_id
     - date
     - total
  2. District Daily Table
     - Id (auto generate)
     - district_id
     - case_id
     - date
     - total
- Develop DAG with necessary tasks following the diagram
- Simulate the ETL Data Pipelines process

## Result
- All set with 2 files of docker compose 
  - `aiflow/docker-compose.yml` 
  - `config_db/docker-compose.yml`
- Developed Extract and Load to Stagging Area namely `aiflow/dags/pyjobs/stagging_api_mysql.py`
- Developed Transform and Load script to make 3 Data Marts Dimensional Tables namely `aiflow/dags/pyjobs/dim_datamart_mysql_psql.py`
- Developed Transform and Load script to make 2 Data Marts Fact Tables namely `aiflow/dags/pyjobs/fact_datamart_district_daily.py` and `aiflow/dags/pyjobs/fact_datamart_provice_daily.py`
- Developed DAG with sequences of task `task1 >> task2  >> [task3,task4]` namely `aiflow/dags/dag_final_project.py`
- All run well simulation of ETL Data Pipelines Process
