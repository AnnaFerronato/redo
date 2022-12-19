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
      command = ("""INSERT INTO dados(A, B) VALUES ("""+ values +""")""")
      cur.execute(command)
    
    cur.close()
    conn.commit()

  finally:
    close_database(cur)
    file.close()

def read_log():
    file = open('entradaLog', 'rb')
    log = file.read().decode()
    logSplit = log.split('\n')
    logSplit.reverse()
    file.close()

    listaCommit = []
    listaStart = []
    listaCKPT = []
    listaOperacoes = []


    for line in logSplit:
        if len(listaCKPT) >= 1: # só precisamos pegar as transações em aberto
            match = re.search("start", line)
            if match:
                for t in listaCKPT:
                    match = re.search(t, line)
                    if match:
                        listaStart.append(line)
                        continue

            match = re.search("<T", line)
            if match:
                for t in listaCKPT:
                    match = re.search(t, line)
                    if match:
                        listaOperacoes.append(line)
                        continue
        
        else:
            match = re.search("commit", line)    
            if match:
                listaCommit.append(line)
                continue

            match = re.search("start", line)
            if match:
                listaStart.append(line)
                continue

            match = re.search("<T", line)
            if match:
                listaOperacoes.append(line)
                continue

            match = re.search("CKPT", line)
            if match and len(listaCKPT) == 0:
                transacao = re.split(" ", line)

                if len(transacao) == 1: # se não tem nenhuma transação em aberto no checkpoint então só precisa refazer as que estão abaixo dele
                    break

                transacaoS = re.sub('[(+*)>]', '', transacao[1])
                transacao = re.split(",", transacaoS)

                for t in transacao:
                    listaCKPT.append(t)
                continue

    redo(listaCKPT, listaOperacoes, listaCommit)

def redo(CKPT, OP, COMMIT):
    redo = []
    listaOperacao = []

    for commmit in COMMIT:
        commmit = re.sub('[<>]', '', commmit)
        commmit = re.split(" ", commmit)

        print(commmit[1] + " realizou REDO")
        redo.append(commmit[1])

        if len(CKPT) >= 1:
            CKPT.remove(commmit[1])

    if len(CKPT) >= 1:     
        for trn in CKPT:
            print(trn + " não realizou REDO")
    
    
    for red in redo:
        for operacao in OP:
            match = re.search(red, operacao)
            if match:
                listaOperacao.append(operacao)
    
    listaOperacao.reverse()

    for operacao in listaOperacao:
        operacao = re.sub('[<>]', '', operacao)
        operacao = re.split(",", operacao)

        idTupla = operacao[1]
        coluna = operacao[2]
        valorNovo = operacao[4]

        try:
            conn = connect_database()
            cur = conn.cursor()

            command = ("""SELECT """ + coluna + """ FROM dados WHERE id = """ + idTupla)

            
            cur.execute(command)
            tuple = cur.fetchone()[0]

            if tuple == valorNovo: 
                break
            else:    
                command = ("""UPDATE dados SET """+ coluna +""" = """ + valorNovo +""" where id = """ + idTupla)
                cur.execute(command)
                
                cur.close()
                conn.commit()

        finally:
            close_database(cur)