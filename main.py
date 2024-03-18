import os
import requests
import pandas as pd
from dotenv import load_dotenv
from snowflake.connector import connect, Error
from prefect import flow, task

load_dotenv()

@task(log_prints=True, retries=3)
def fetch_data():
    url = "https://api.energidataservice.dk/dataset/PowerSystemRightNow/"
    data = requests.get(url).json()
    return data['records']

@task(log_prints=True, retries=3)
def transformation(json_data):
    df = pd.DataFrame(json_data)
    df = df.drop(columns=["aFRR_ActivatedDK1", "aFRR_ActivatedDK2", "mFRR_ActivatedDK1", "mFRR_ActivatedDK2", "ImbalanceDK1", "ImbalanceDK2"])
    return df

@task(log_prints=True, retries=3)
def load(clean_data, snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse, snowflake_database, snowflake_schema):
    # Connect to Snowflake
    conn = connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )
    cursor = conn.cursor()

    clean_data_columns = clean_data.columns.tolist()
    for _, row in clean_data.iterrows():
        placeholders = ', '.join(['%s'] * len(clean_data_columns))
        columns = ', '.join(clean_data_columns)
        # Construct the SQL query
        sql_query = f"""
            INSERT INTO emission ({columns}) 
            VALUES ({placeholders})
        """
        # Execute the SQL query
        cursor.execute(sql_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    print("Data loaded successfully into Snowflake.")

@flow(name="Energy API ingest")
def energy_main():
    snowflake_user = os.getenv("snowflake_user")
    snowflake_password = os.getenv("snowflake_password")
    snowflake_account = os.getenv("snowflake_account")
    snowflake_warehouse = os.getenv("snowflake_warehouse")
    snowflake_database = os.getenv("snowflake_database")
    snowflake_schema = os.getenv("snowflake_schema")

    json_data = fetch_data()
    clean_data = transformation(json_data)
    load(clean_data, snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse, snowflake_database, snowflake_schema)

if __name__ == "__main__":
    energy_main()
