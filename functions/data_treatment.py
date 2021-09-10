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