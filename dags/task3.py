import pandas as pd
import psycopg2


def task_3():
    """ Insert the data from CSV file into the Weather table """
    cur = None
    conn = None

    # Reading data from CSV file
    def fetch_data():
        df = pd.read_csv("/Users/shashankdey/airflow-assignment/dags/weather_data.csv")
        print(df)
        return df

    def connect():
        # DB variables
        db_host = "postgres"
        db_name = "airflow"
        db_user = "airflow"
        db_pass = "airflow"
        port_no = 5432

        # Connecting with the local server database
        print("Connecting to the PostgresSQL database...")
        conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host, port=port_no)
        cur = conn.cursor()
        return cur, conn

    def insert(cur, conn, df):
        query = """ insert into weather (state, description, temperature, temperature_feels_like, min_temperature,
                max_temperature, humidity, clouds) """
        data = []
        for row in df.iterrows():
            # Creating tuples from dataframe
            record_to_insert = (row[1]['State'], row[1]['Description'], row[1]['Temperature'],row[1]['Temperature feels like'],
                                row[1]['Min temperature'], row[1]['Max temperature'], row[1]['Humidity'], row[1]['Clouds'])
            data.append(record_to_insert)

            # Inserting data into the table
        cur.executemany(query, data)
        conn.commit()

    try:
        cur, conn = connect()
        df = fetch_data()
        insert(cur, conn, df)

    except:
        print("Error in the connection...")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    task_3()