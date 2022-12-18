import psycopg2
import json
import re

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

def redo():
    file = open('entradaLog', 'rb')
    log = file.read().decode()
    logSplit = log.split('\n')
    logSplit.reverse()

    listaCommit = []
    listaStart = []
    listaCKPT = []
    listaOperacoes = []


    for line in logSplit:
        match = re.search("commit", line)    
        if match:
            listaCommit.append(line)
            continue

        match = re.search("start", line)
        if match:
            listaStart.append(line)
            continue

        match = re.search("CKPT", line)
        if match and len(listaCKPT) == 0:
            listaCKPT.append(line)
            continue

        match = re.search("<T", line)
        if match:
            listaOperacoes.append(line)
            continue
        



        
    
        

            

redo()

# TRUNCATE nome_tabela