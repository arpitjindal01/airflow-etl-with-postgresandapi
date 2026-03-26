from airflow.sdk import dag, task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


# This will be used to extract and save data in a postgres db

@dag(start_date=datetime(2026, 1, 1), schedule=None, default_args={"owner": "arpit"}, catchup=False)
def etl():

    # Task 1 : Connect with the Postgres DB
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres")
        print("Connected to Postgres database")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        
        postgres_hook.run(create_table_query)
    @task
    def fetching_data():
        url = "https://api.nasa.gov/planetary/apod"
        params = {"api_key": "ExOHO8vhY7pLEUomBxlsrVg2YZBJXvGGEAgAGNOB"}
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data

    @task
    def transforming_data(data):
        apod_data = {
        'date' : data["date"],
        'explanation' : data["explanation"],
        'url' : data["url"],
        'media_type' : data["media_type"],
        'title' : data["title"]
        }
        return apod_data

    @task
    def loading_data(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres")

        insert_query = """
                       INSERT INTO apod_data (title, explanation, url, date, media_type)
                       VALUES (%s, %s, %s, %s, %s); \
                       """

        postgres_hook.run(
            insert_query,
            parameters=(
                apod_data["title"],
                apod_data["explanation"],
                apod_data["url"],
                apod_data["date"],
                apod_data["media_type"]
            )
        )

        print("Data inserted successfully")

    create = create_table()
    raw_data = fetching_data()
    transformed = transforming_data(raw_data)
    load = loading_data(transformed)

    create >> raw_data >> transformed >> load


etl()


