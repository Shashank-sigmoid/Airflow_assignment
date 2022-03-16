import psycopg2


def task_2():
    conn = None
    cur = None

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

    def create_table(cur):
        query = """ create table if not exists Weather(
        State varchar(20),
        Description varchar(20),
        Temperature decimal,
        Temperature_feels_like decimal,
        Min_temperature decimal,
        Max_temperature decimal,
        Humidity numeric,
        Clouds numeric) """
        cur.execute(query)
        print("Table created successfully...")

    try:
        cur, conn = connect()
        create_table(cur)

    except:
        print("Error in the connection")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    task_2()