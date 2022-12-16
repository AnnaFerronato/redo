import psycopg2



def connect_database():
    try:
        conn = psycopg2.connect(
            database = "log",
            user = "postgres",
            password = "4108357923",
            host = "127.0.0.1",
            port = "5432"
        )

        return conn
    except psycopg2.DatabaseError as error:
        print(error)
        exit()


