import sqlite3
import pandas as pd
from unidecode import unidecode

def get_dataframe(url: str, spreadsheet: list[str] = 0, columns_wanted: list = []) -> pd.DataFrame:
    """Pega um arquivo excel e o transforma em um dataframe

    Args:
        url (str): Localização atual do arquivo excel
        
        spreadsheet (list[str], optional): Lista de nomes, 
            ou nome especifíco ou index especifico do spreadsheet escolhido. Padrão para 0 para pegar o primeiro spreadsheet.
        
        columns_wanted (list, optional): colunas da tabelas que você quer que venham. padrão para todas as colunas.

    Returns:
        pd.DataFrame: Arquivo transformado em DataFrame
    """
    
    dataframe = pd.read_excel(url, spreadsheet)
    
    #Necessário devido a natureza do arquivo excel especifico (sem coluna e linha inicial e colunas com nomes errados)
    dataframe = dataframe.iloc[:,1:]
    dataframe.columns = list(dataframe.iloc[0,:].values)
    dataframe = dataframe.iloc[1:,:]
    dataframe.reset_index(drop = True, inplace = True)

    if columns_wanted: return dataframe[columns_wanted]

    return dataframe


def create_sqlite():
    """
    Pega o arquivo excel e o transforma em um banco de dados sqlite
    """

    conn = sqlite3.connect("database\database.db")
    cursor = conn.cursor()

    # #Tabela CLIENTS
    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS CLIENTS (
    #     Concatenar TEXT NOT NULL,
    #     "Cód Cliente" TEXT NOT NULL,
    #     Cidade TEXT NOT NULL,
    #     Estado TEXT NOT NULL,
    #     Valor REAL,
    #     Faixas TEXT NOT NULL,
    #     "Concatenar Distri" TEXT NOT NULL,
    #     Perfil TEXT NOT NULL,
    #     "Area Nielsen" TEXT NOT NULL, 
    #     TIPO TEXT NOT NULL, 
    #     "CHECK" TEXT NOT NULL, 
    #     "." TEXT,
    #     Status TEXT, 
    #     "Rede / Grupo" TEXT, 
    #     "Faixa do Sortimento" TEXT
    # );""")

    # #Tabela PRODUCTS
    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS PRODUCTS (
    #     'REGIÃO' TEXT NOT NULL,
    #     'EAN' INTEGER NOT NULL,
    #     'FAM DESC PROG' TEXT,
    #     'STATUS DE PRODUÇÃO' TEXT,
    #     'FOCO/LANÇAMENTO' TEXT,
    #     'PACKS/DISPLAYS' TEXT,
    #     'SKUs PUSH & PULL - EB2B' TEXT NOT NULL,
    #     'CATEGORIA' TEXT NOT NULL,
    #     'MARCA' TEXT NOT NULL, 
    #     'SUB MARCA' TEXT NOT NULL, 
    #     'SUB MARCA GRAMA' TEXT NOT NULL, 
    #     'DESC' TEXT NOT NULL,
    #     'FAIXA 06' BOOLEAN, 
    #     'FAIXA 05' BOOLEAN, 
    #     'FAIXA 04' BOOLEAN,
    #     'FAIXA 03' BOOLEAN
    # );""")

    CLIENTES = get_dataframe(url = "database\data_base.xlsx", spreadsheet = "CLIENTES")
    CLIENTES["Valor"] = CLIENTES["Valor"].astype(float)

    PRODUTOS = get_dataframe(url = "database\data_base.xlsx", spreadsheet = "SORTIMENTO")
    
    #try para quando reiniciar servidor, não dar erro
    try:
        CLIENTES.to_sql('CLIENTS', conn, if_exists='fail', index=False)
    except: pass

    try:
        PRODUTOS.to_sql('PRODUCTS', conn, if_exists='fail', index=False)
    except: pass


def get_sql(query: str) -> pd.DataFrame:
    """Realiza uma consulta no banco de dados

    Args:
        query (str): código sqlite para realizar a query

    Returns:
        pd.DataFrame: resultado da query
    """
    
    conn = sqlite3.connect("database\database.db")
    return pd.read_sql_query(query, conn)


def dataframe_normalization(data: pd.DataFrame) -> pd.DataFrame:
    """Normaliza os dados do dataframe (transforma string em lowercase e retira acentos)

    Args:
        data (pd.DataFrame): dataframe que deve ser normalizado

    Returns:
        pd.DataFrame: dataframe normalizado
    """

    dataframe = data.copy().astype(str)
    #Foram adicionados estes replaces por causa de um problema com o nome do "cód cliente" que vem com ' ou "
    dataframe.columns = [unidecode((column.replace("\'","").replace("\"","")).lower()) for column in dataframe.columns]

    for column in dataframe.columns:

        dataframe[column] = (dataframe[column].str.lower()).astype(str)
        dataframe[column] = dataframe[column].apply(unidecode)

    return dataframe


def dict_normalization(data: dict) -> dict:
    """Normaliza os dados do dicionário do request (transforma string em lowercase e retira acentos)

    Args:
        data (dict): dicionário que deve ser normalizado

    Returns:
        dict: dicionário normalizado
    """
    if data:
        return {unidecode(key.casefold()): unidecode(str(value).casefold())  for key, value in data.items()}

    return {}
    

if __name__ == "__main__":
    pass