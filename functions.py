import psycopg2



def connect_database():
    try:
        conn = psycopg2.connect(
            database = "log",
            user = "postgres"
        )