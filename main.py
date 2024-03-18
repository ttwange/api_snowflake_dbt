import os
import requests
import pandas as pd
import snowflake.connector
from prefect import flow, task

@task(log_prints=True, retries=3)
def fetch_data():
    url = "https://api.energidataservice.dk/dataset/PowerSystemRightNow/"
    data = requests.get(url).json()
    return data['records']

@task(log_prints=True, retries=3)
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("snowflake_user"),
            password=os.getenv("snowflake_password"),
            account=os.getenv("snowflake_account"),
            warehouse=os.getenv("snowflake_warehouse"),
            database=os.getenv("snowflake_database"),
            schema=os.getenv("snowflake_schema")
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")

@task(log_prints=True, retries=3)
def transformation(json_data):
    df = pd.DataFrame(json_data)
    df = df.drop(columns=["aFRR_ActivatedDK1", "aFRR_ActivatedDK2", "mFRR_ActivatedDK1", "mFRR_ActivatedDK2", "ImbalanceDK1", "ImbalanceDK2"])
    return df

@task(log_prints=True, retries=3)
def load(data, conn):
    try:
        snowflake_insert_sql = """
            INSERT INTO emission (
            Minutes1UTC, Minutes1DK, CO2Emission, ProductionGe100MW,
            ProductionLt100MW, SolarPower, OffshoreWindPower,
            OnshoreWindPower, Exchange_Sum, Exchange_DK1_DE,
            Exchange_DK1_NL, Exchange_DK1_GB, Exchange_DK1_NO,
            Exchange_DK1_SE, Exchange_DK1_DK2, Exchange_DK2_DE,
            Exchange_DK2_SE, Exchange_Bornholm_SE
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Create a cursor from the Snowflake connection
        cursor = conn.cursor()
        cursor.executemany(snowflake_insert_sql, data)
        cursor.close()
        conn.commit()
    except Exception as e:
        print(f"Error inserting data to Snowflake: {e}")


@flow(name="snowflake_ingest")
def energy_main():
    json_data = fetch_data()
    conn = connect_to_snowflake()
    clean_data = transformation(json_data)
    load(clean_data, conn)
    print(clean_data)

if __name__ == "__main__":
    energy_main()
