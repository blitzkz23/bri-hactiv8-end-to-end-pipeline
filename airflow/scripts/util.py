import psycopg2
import logging
import traceback
import pandas as pd

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