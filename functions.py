import psycopg2
import json

def connect_database():
    try:
        conn = psycopg2.connect(
            database = "log",
            user = "postgres",
            password = "12345",
            host = "localhost",
            port = "5432"
        )

        return conn
    except psycopg2.DatabaseError as error:
        print(error)
        exit()


def close_database(conn):
    if conn is not None:
        conn.close()


def create_table():
    commands = (
        """drop table if exists dados;""",

        """create table dados(
            id serial,
            A integer not null,
            B integer not null,
            constraint pk_dados primary key (id)
        );"""
    )

    try:
        conn = connect_database()
        cur = conn.cursor()

        for command in commands:
            cur.execute(command)
        
        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    close_database(cur)

def insert_data():
  file = open('metadado.json', 'r')

  try:
    data = json.load(file)['INITIAL']
    tuples = list( zip(data['A'], data['B']) )

    conn = connect_database()
    cur = conn.cursor()
    
    for tuple in tuples:
      tuple = [str(column) for column in tuple]
      values = ', '.join(tuple)
      command = ("""INSERT INTO dados(a, b) VALUES ("""+ values +""")""")
      cur.execute(command)
    
    cur.close()
    conn.commit()

  finally:
    close_database(cur)
    file.close()


create_table()
insert_data()