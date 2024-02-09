# user_processing_dag.py
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import psycopg2
import wget
import logging

"""
default_args berisi semua arguments 
yang sering digunakan oleh task, 
contohnya : start_date
"""

# import airflow.task logger
_LOGGER = logging.getLogger("airflow.task")


default_args = {
    'owner': 'Aldy',
    'start_date': datetime(2022, 1, 8),
    'retry': 1,
}

def fetch_taxi_data(base_url):
    """
    Function to fetch data from an URL.
    
    Args:
        url (str): The URL from which to fetch data.
    
    Returns:
        -
    """
    for month in range(1, 12):
        # Format the month and day to have leading zeros if needed
        month_str = str(month).zfill(2)
        
        # Construct the URL for the file
        url = f"{base_url}-{month_str}.parquet"
        
        try:
                # Download the file
                filename = wget.download(url)
                _LOGGER.info(f"Downloaded: {filename}")
        except Exception as e:
                _LOGGER.warning(f"Failed to download {url}: {e}")
                
def fetch_bike_data(url):
    """
    Function to fetch data from an URL.
    
    Args:
        url (str): The URL from which to fetch data.
    
    Returns:
        -
    """
    try:
        # Download the file
            filename = wget.download(url)
            _LOGGER.info(f"Downloaded: {filename}")
    except Exception as e:
            _LOGGER.warning(f"Failed to download {url}: {e}")
    
    
def clean_and_insert(**context):
    """
    Function to clean data and insert it to postgres.
    
    Args:
        context from previous task
    
    Returns:
        -
    """
    task_instance = context['task_instance']
    data = task_instance.xcom_pull(task_ids='fetch_data')
    df = pd.DataFrame(data)
    # Clean data
    cleaned_df = clean_data(df)
    
    # Insert to postgres
    insert_to_postgres(cleaned_df)
    
    
def clean_data(df):
    
    return df

def insert_to_postgres(df):
    with psycopg2.connect(
        database="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    ) as conn:
        with conn.cursor() as cur:
            for index, row in df.iterrows():
                cur.execute(
                    "INSERT INTO nyc_taxi (vendorid, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, ratecodeid, pulocationid, dolocationid, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        row['vendorid'],
                        row['lpep_pickup_datetime'],
                        row['lpep_dropoff_datetime'],
                        row['store_and_fwd_flag'],
                        row['ratecodeid'],
                        row['pulocationid'],
                        row['dolocationid'],
                        row['passenger_count'],
                        row['trip_distance'],
                        row['fare_amount'],
                        row['extra'],
                        row['mta_tax'],
                        row['tip_amount'],
                        row['tolls_amount'],
                        row['improvement_surcharge'],
                        row['total_amount'],
                        row['payment_type'],
                        row['trip_type'],
                        row['congestion_surcharge']
                    )
                )
            conn.commit()
            
with DAG('taxi_bike_pipeline', schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    
    # Define task to extract data from API
    fetch_taxi_data_operator = PythonOperator(
        task_id="fetch_taxi_data",
        python_callable=fetch_taxi_data,
        op_kwargs={"base_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023"},
    )
    
    # Define task to extract data from API
    fetch_bike_data_operator = PythonOperator(
        task_id="fetch_bike_data",
        python_callable=fetch_bike_data,
        op_kwargs={"url": "https://s3.amazonaws.com/tripdata/201306-citibike-tripdata.zip"},
    )
    
    # Define task to clean and insert data
    clean_and_insert_operator = PythonOperator(
        task_id="insert_data",
        python_callable=clean_and_insert,
        provide_context=True
    )

    [fetch_taxi_data_operator, fetch_bike_data_operator ]