# user_processing_dag.py
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.insert(0, '/opt/airflow/scripts')
from insert_util import *
from fetch_util import fetch_csv_data

"""
default_args berisi semua arguments 
yang sering digunakan oleh task, 
contohnya : start_date
"""
default_args = {
    'owner': 'Aldy',
    'start_date': datetime(2022, 1, 8),
    'retry': 1,
}

with DAG('churn_pipeline', schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    
    creating_raw_churn_operator = PostgresOperator(
        task_id='create_raw_churn_table',  # Task ID
        postgres_conn_id='airflow_postgres',  # Connection ID
        sql="""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
        Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
        Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)"""
    )
    
    # Define task to extract data from API
    fetch_csv_data_operator = PythonOperator(
        task_id="fetch_csv_data",
        python_callable=fetch_csv_data,
        op_kwargs={"url": "https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv",
                   "output_dir": "/opt/airflow/dags"},
    )
    
    insert_churn_operator = PythonOperator(
        task_id='insert_churn_data',
        python_callable=insert_to_postgres,
        op_kwargs={"dir": "/opt/airflow/dags/",},
    )    
    
    create_df_and_insert_operator = PythonOperator(
        task_id='create_df_and_insert',
        python_callable=create_df_and_insert,
    )


# Set task dependencies
creating_raw_churn_operator >> fetch_csv_data_operator >> insert_churn_operator >> create_df_and_insert_operator
