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
import numpy as np
import traceback

# import airflow.task logger
_LOGGER = logging.getLogger("airflow.task")

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

try:
    conn = psycopg2.connect(
            database="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
    )
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")

def fetch_csv_data(url, output_dir):
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

def insert_to_postgres(dir):
    filename = 'Churn_Modelling.csv'
    df_churn = pd.read_csv(dir+filename)
    
    with conn.cursor() as cur:
                inserted_row_count = 0
                    
                for _, row in df_churn.iterrows():
                   count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}"""
                   cur.execute(count_query)
                   result = cur.fetchone()
                   if result[0] == 0:
                        inserted_row_count += 1
                        cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
                        Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""", 
                        (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))

                logging.info(f' {inserted_row_count} rows from csv file inserted into churn_modelling table successfully')
                conn.commit()
                cur.close()
                conn.close()
        
def create_base_df(cur):
    """
    Base dataframe of churn_modelling table
    """
    cur.execute("""SELECT * FROM churn_modelling""")
    rows = cur.fetchall()

    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)

    df.drop('rownumber', axis=1, inplace=True)
    index_to_be_null = np.random.randint(10000, size=30)
    df.loc[index_to_be_null, ['balance','creditscore','geography']] = np.nan
    
    most_occured_country = df['geography'].value_counts().index[0]
    df['geography'].fillna(value=most_occured_country, inplace=True)
    
    avg_balance = df['balance'].mean()
    df['balance'].fillna(value=avg_balance, inplace=True)

    median_creditscore = df['creditscore'].median()
    df['creditscore'].fillna(value=median_creditscore, inplace=True)

    return df


def create_creditscore_df(df):
    df_creditscore = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean', 'exited':'sum'})
    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
    df_creditscore.reset_index(inplace=True)

    df_creditscore.sort_values('avg_credit_score', inplace=True)

    return df_creditscore


def create_exited_age_correlation(df):
    df_exited_age_correlation = df.groupby(['geography', 'gender', 'exited']).agg({
    'age': 'mean',
    'estimatedsalary': 'mean',
    'exited': 'count'
    }).rename(columns={
        'age': 'avg_age',
        'estimatedsalary': 'avg_salary',
        'exited': 'number_of_exited_or_not'
    }).reset_index().sort_values('number_of_exited_or_not')

    return df_exited_age_correlation


def create_exited_salary_correlation(df):
    df_salary = df[['geography','gender','exited','estimatedsalary']].groupby(['geography','gender']).agg({'estimatedsalary':'mean'}).sort_values('estimatedsalary')
    df_salary.reset_index(inplace=True)

    min_salary = round(df_salary['estimatedsalary'].min(),0)

    df['is_greater'] = df['estimatedsalary'].apply(lambda x: 1 if x>min_salary else 0)

    df_exited_salary_correlation = pd.DataFrame({
    'exited': df['exited'],
    'is_greater': df['estimatedsalary'] > df['estimatedsalary'].min(),
    'correlation': np.where(df['exited'] == (df['estimatedsalary'] > df['estimatedsalary'].min()), 1, 0)
    })

    return df_exited_salary_correlation

def create_new_tables_in_postgres():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT,number_of_exited_or_not INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation  (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")
        logging.info("3 tables created successfully in Postgres server")
    except Exception as e:
        traceback.print_exc()
        logging.error(f'Tables cannot be created due to: {e}')
        
def insert_creditscore_table(df_creditscore):
    query = "INSERT INTO churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES (%s,%s,%s,%s)"
    row_count = 0
    for _, row in df_creditscore.iterrows():
        values = (row['geography'],row['gender'],row['avg_credit_score'],row['total_exited'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table churn_modelling_creditscore")


def insert_exited_age_correlation_table(df_exited_age_correlation):
    query = """INSERT INTO churn_modelling_exited_age_correlation (Geography, Gender, exited, avg_age, avg_salary, number_of_exited_or_not) VALUES (%s,%s,%s,%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_age_correlation.iterrows():
        values = (row['geography'],row['gender'],row['exited'],row['avg_age'],row['avg_salary'],row['number_of_exited_or_not'])
        cur.execute(query,values)
        row_count += 1
    
    logging.info(f"{row_count} rows inserted into table churn_modelling_exited_age_correlation")


def insert_exited_salary_correlation_table(df_exited_salary_correlation):
    query = """INSERT INTO churn_modelling_exited_salary_correlation (exited, is_greater, correlation) VALUES (%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_salary_correlation.iterrows():
        values = (int(row['exited']),int(row['is_greater']),int(row['correlation']))
        cur.execute(query,values)
        row_count += 1

    logging.info(f"{row_count} rows inserted into table churn_modelling_exited_salary_correlation")

def create_df_and_insert():
    main_df = create_base_df(cur)
    df_creditscore = create_creditscore_df(main_df)
    df_exited_age_correlation = create_exited_age_correlation(main_df)
    df_exited_salary_correlation = create_exited_salary_correlation(main_df)

    create_new_tables_in_postgres()
    insert_creditscore_table(df_creditscore)
    insert_exited_age_correlation_table(df_exited_age_correlation)
    insert_exited_salary_correlation_table(df_exited_salary_correlation)

    conn.commit()
    cur.close()
    conn.close()

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
