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
import os
import logging
import urllib.request
import zipfile

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

def fetch_taxi_data(base_url, output_dir):
    """
    Function to fetch data from a URL.
    
    Args:
        base_url (str): The base URL from which to construct the file URLs.
        output_dir (str): The directory where the downloaded files will be saved.
    
    Returns:
        -
    """
    month_str = "01"  # January
    
    # Construct the URL for the file
    url = f"{base_url}-{month_str}.parquet"
    
    try:
        # Download the file and save it to the output directory
        filename, headers = urllib.request.urlretrieve(url, os.path.join(output_dir, f"yellow_tripdata_2023-{month_str}.parquet"))
        logging.info(f"Downloaded: {filename}")
        print(f"Downloaded: {filename}")
    except Exception as e:
        logging.warning(f"Failed to download {url}: {e}")

def fetch_bike_data(url, output_dir):
    """
    Function to fetch data from an URL.
    
    Args:
        url (str): The URL from which to fetch data.
        output_dir (str): The directory where the downloaded file will be saved.
    
    Returns:
        -
    """
    try:
        # Open the URL and download the file
        response = urllib.request.urlopen(url)
        # Extract filename from URL
        filename = os.path.join(output_dir, url.split('/')[-1])
        with open(filename, 'wb') as f:
            f.write(response.read())
                
        _LOGGER.info(f"Downloaded: {filename}")
        print(f"Downloaded: {filename}")
    except Exception as e:
        _LOGGER.warning(f"Failed to download {url}: {e}")

def unzip_citibike_data():
    zip_file_path = '/opt/airflow/dags/JC-202301-citibike-tripdata.csv.zip'
    extraction_path = '/opt/airflow/dags'

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extraction_path)
    
def insert_taxi(dir):
        """
        Function to insert taxi data to postgres.
        
        Args:
            dir: directory
            filename: filename
        
        Returns:
            -
        """
        # Read and insert data from each Parquet file into SQL database
        # for file_name in file_names:
        file_name = 'yellow_tripdata_2023-01.parquet'
        df_taxi = pd.read_parquet(dir+file_name)
        
        _LOGGER.info(print(df_taxi.head()))
        _LOGGER.warning(df_taxi.head())
        _LOGGER.debug("cok")
        insert_to_postgres_taxi(df_taxi)

def clean_and_insert_bike(dir):
    """
    Function to clean data and insert it to postgres.
    
    Args:
        context from previous task
    
    Returns:
        -
    """

    filename = 'JC-202301-citibike-tripdata.csv'
    df_bike = pd.read_csv(dir+filename)
    dtype_mapping = {
        'started_at': 'datetime64[ns]',
        'ended_at': 'datetime64[ns]',
    }
    df_bike = df_bike.astype(dtype_mapping)

    # Insert to postgres
    insert_to_postgres_bike(df_bike)

def insert_to_postgres_taxi(df):
    try:
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
                        "INSERT INTO nyc_taxi_raw (vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, store_and_fwd_flag, ratecodeid, pulocationid, dolocationid, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, payment_type, congestion_surcharge, airport_fee) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            row['VendorID'],
                            row['tpep_pickup_datetime'],
                            row['tpep_dropoff_datetime'],
                            row['store_and_fwd_flag'],
                            row['RatecodeID'],
                            row['PULocationID'],
                            row['DOLocationID'],
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
                            row['congestion_surcharge'],
                            row['airport_fee']
                        )
                    )
                conn.commit()
    except Exception as e:
        logging.error(f"Error occurred while inserting data into PostgreSQL: {str(e)}")
            
def insert_to_postgres_bike(df):
    try:
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
                        "INSERT INTO nyc_bike_raw (ride_id, rideable_type, started_at, ended_at, start_station_id, start_station_name, start_lat, start_lng, end_station_id, end_station_name, end_lat, end_lng, member_casual) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            row['ride_id'],
                            row['rideable_type'],
                            row['started_at'],
                            row['ended_at'],
                            row['start_station_id'],
                            row['start_station_name'],
                            row['start_lat'],
                            row['start_lng'],
                            row['end_station_id'],
                            row['end_station_name'],
                            row['end_lat'],
                            row['end_lng'],
                            row['member_casual']
                        )
                    )
                conn.commit()
    except Exception as e:
        logging.error(f"Error occurred while inserting data into PostgreSQL: {str(e)}")


with DAG('taxi_bike_pipeline', schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    # Define task to create raw postgres taxi table
    creating_raw_taxi_table_operator = PostgresOperator(
        task_id='create_raw_taxi_table',  # Task ID
        postgres_conn_id='airflow_postgres',  # Connection ID
        sql="""
            CREATE TABLE IF NOT EXISTS nyc_taxi_raw (
                vendorid INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                store_and_fwd_flag VARCHAR(1),
                ratecodeid INT,
                pulocationid INT,
                dolocationid INT,
                passenger_count INT,
                trip_distance FLOAT,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                payment_type INT,
                congestion_surcharge FLOAT,
                airport_fee FLOAT
            )
            """
    )
    
    creating_raw_bike_table_operator = PostgresOperator(
        task_id='create_raw_bike_table',  # Task ID
        postgres_conn_id='airflow_postgres',  # Connection ID
        sql="""
            CREATE TABLE IF NOT EXISTS nyc_bike_raw (
                ride_id VARCHAR,
                rideable_type VARCHAR,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                start_station_name VARCHAR,
                start_station_id VARCHAR,
                end_station_name VARCHAR,
                end_station_id VARCHAR,
                start_lat FLOAT,
                start_lng FLOAT,
                end_lat FLOAT,
                end_lng FLOAT,
                member_casual VARCHAR
            )
            """
    )
    
    # Define task to extract data from API
    fetch_taxi_data_operator = PythonOperator(
        task_id="fetch_taxi_data",
        python_callable=fetch_taxi_data,
        op_kwargs={"base_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023",
                   "output_dir": "/opt/airflow/dags"},
    )
    
    # Define task to extract data from API
    fetch_bike_data_operator = PythonOperator(
        task_id="fetch_bike_data",
        python_callable=fetch_bike_data,
        op_kwargs={"url": "https://s3.amazonaws.com/tripdata/JC-202301-citibike-tripdata.csv.zip",
                   "output_dir": "/opt/airflow/dags"},
    )
    
    extract_citibike_data_operator = PythonOperator(
        task_id='extract_citibike_data',
        python_callable=unzip_citibike_data,
    )
    
    insert_taxi_operator = PythonOperator(
        task_id='insert_taxi_data',
        python_callable=insert_taxi,
        op_kwargs={"dir": "/opt/airflow/dags/"},
    )
    
    insert_bike_operator = PythonOperator(
        task_id='insert_bike_data',
        python_callable=clean_and_insert_bike,
        op_kwargs={"dir": "/opt/airflow/dags/"},
    )
   


# Set task dependencies
creating_raw_bike_table_operator >> fetch_bike_data_operator >> extract_citibike_data_operator >> insert_bike_operator
creating_raw_taxi_table_operator >> fetch_taxi_data_operator >> insert_taxi_operator