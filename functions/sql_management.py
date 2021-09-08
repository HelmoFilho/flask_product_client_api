import sqlite3
import pandas as pd
import os.path
import data_treatment as dt

def create_sqlite():
    """
    Pega o arquivo excel e o transforma em um banco de dados sqlite
    """
    path = "database\database.db"

    #se o arquivo já existir, para de tentar criar o arquivo
    if os.path.isfile(path): return  
    
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    #Tabela CLIENTS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS CLIENTS (
        Concatenar TEXT NOT NULL,
        "Cód Cliente" TEXT NOT NULL,
        Cidade TEXT NOT NULL,
        Estado TEXT NOT NULL,
        Valor REAL,
        Faixas TEXT NOT NULL,
        "Concatenar Distri" TEXT NOT NULL,
        Perfil TEXT NOT NULL,
        "Area Nielsen" TEXT NOT NULL, 
        TIPO TEXT NOT NULL, 
        "CHECK" TEXT NOT NULL, 
        "." TEXT,
        Status TEXT, 
        "Rede / Grupo" TEXT, 
        "Faixa do Sortimento" TEXT
    );""")

    #Tabela PRODUCTS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS PRODUCTS (
        'REGIÃO' TEXT NOT NULL,
        'EAN' INTEGER NOT NULL,
        'FAM DESC PROG' TEXT,
        'STATUS DE PRODUÇÃO' TEXT,
        'FOCO/LANÇAMENTO' TEXT,
        'PACKS/DISPLAYS' TEXT,
        'SKUs PUSH & PULL - EB2B' TEXT NOT NULL,
        'CATEGORIA' TEXT NOT NULL,
        'MARCA' TEXT NOT NULL, 
        'SUB MARCA' TEXT NOT NULL, 
        'SUB MARCA GRAMA' TEXT NOT NULL, 
        'DESC' TEXT NOT NULL,
        'FAIXA 06' BOOLEAN, 
        'FAIXA 05' BOOLEAN, 
        'FAIXA 04' BOOLEAN,
        'FAIXA 03' BOOLEAN
    );""")

    CLIENTES = dt.get_dataframe(url = "database\data_base.xlsx", spreadsheet = "CLIENTES")
    CLIENTES["Valor"] = CLIENTES["Valor"].astype(float)

    PRODUTOS = dt.get_dataframe(url = "database\data_base.xlsx", spreadsheet = "SORTIMENTO")
    
    CLIENTES.to_sql('CLIENTS', conn, if_exists = 'append', index = False)
    PRODUTOS.to_sql('PRODUCTS', conn, if_exists = 'append', index = False)


def get_sql(query: str) -> pd.DataFrame:
    """Realiza uma consulta no banco de dados

    Args:
        query (str): código sqlite para realizar a query

    Returns:
        pd.DataFrame: resultado da query
    """
    
    conn = sqlite3.connect("database\database.db")
    return pd.read_sql_query(query, conn)


def sql_post(*args):
    pass