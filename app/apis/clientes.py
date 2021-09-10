from flask import request
from unidecode import unidecode
import functions.data_treatment as dt
import functions.sql_management as sqlm
import sqlite3

def get():
    """
    Método GET do servidor dos clientes
    """

    filters = dt.dict_normalization(request.get_json())
    
    condition = "WHERE"

    for key, value in filters.items():

        #exclusivo para o "cód clientes"
        if "cod" in key and "cliente":
            
            #se "incompleto" estiver na chave, procura o valor em qualquer ponto do valor
            if "inco" in key:
                condition += f""" "cód cliente" like "%{value}%" and"""
            
            #se "incompleto" não estiver na chave, procura o valor da esquerda para direita
            else:
                condition += f""" "cód cliente" == {value} and"""

        #exclusivo para o "valor"
        elif "valor" in key:

            #se "maximo" estivem na chave, determina um valor máximo para o "valor"
            if " ma" in key:
                condition += f""" "valor" <= {value} and"""

            #se "minimo" estivem na chave, determina um valor mínimo para o "valor"
            elif " mi" in key:
                condition += f""" "valor" >= {value} and"""
            
            #procura o valor exato em "valor"
            else:
                condition += f""" "valor" == {value} and"""

        #procura o valor em qualquer ponto do valor de "faixas" (independente de caixa alta ou baixa)
        elif "faixas" in key:
            condition += f""" "faixas" like "%{value}%" COLLATE NOCASE and"""

        #procura o valor da esquerda para a direita das chaves restantes (independente de caixa alta ou baixa)
        else:
            condition += f""" "{key}" like "{value}%" COLLATE NOCASE and"""

    #caso não tenha passado nenhum argumento
    if condition == "WHERE":
        condition = ""

    #Remove o ultimo 'and'
    else:
        condition = condition.rsplit(' ', 1)[0]

    #realiza a query com relação as condições formadas
    response = sqlm.get_sql(f"SELECT * FROM CLIENTS {condition}")

    if response.empty:
        return {"error": "No response"}, 204

    return {"data": response.to_dict("records")}    


def post():
    """
    Método POST do servidor dos clientes
    """
    filters = dict(request.get_json())

    #
    columns = list((sqlm.get_sql("SELECT * FROM CLIENTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    #
    not_null = ['Cód Cliente','Cidade', 'Estado', 'Valor', 'Faixas', 'Perfil', 'Area Nielsen', 'TIPO', 'CHECK', 'Faixa do Sortimento']
    normalized_not_null = [unidecode(column.lower())   for column in not_null]

    #
    filter_keys = list(filters.keys())
    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    filter_keys = dict(zip(normalized_keys, filter_keys))

    # 
    unnecessary_keys_normalized = [un_key  for un_key in normalized_columns  if un_key not in normalized_not_null]
    
    #
    missing_keys = []
    for key in normalized_not_null:
        if key not in normalized_keys:
            missing_keys.append(columns[key])

    if missing_keys:
            return {"Erro - Chaves faltando": missing_keys}

    #
    invalid_keys = []
    for key in normalized_keys:
        if key not in normalized_columns:
            invalid_keys.append(filter_keys[key])

    if invalid_keys:
        return {"Erro - Chaves invalidas": invalid_keys}

    #
    invalid_values = []
    for key, value in zip(filter_keys, filters.values()):
        if not value and key in normalized_not_null:
            invalid_values.append(key)
        else:
            pass
    
    if invalid_values:
        return {"Erro - Valores invalidos": invalid_values}

    #
    incorrect_values = []
    for key, value in zip(filter_keys, filters.values()):
        
        #exclusivo para o "cód clientes"
        if "cod" in key and "cliente":
            try: 
                int(str(value))
            except: 
                incorrect_values.append(columns[key])
        
        #exclusivo para o "valor"
        elif key == "valor":
            try: 
                float(str(value))
            except: 
                incorrect_values.append(columns[key])

        #para todos os outros
        else:
            if type(value) is not str and key not in unnecessary_keys_normalized:
                incorrect_values.append(columns[key])

    if incorrect_values:
        return {"Erro - Valores de tipo incorreto": incorrect_values}

    if not sqlm.get_sql(f'SELECT * FROM CLIENTS WHERE "Cód Cliente" = {filters["Cód Cliente"]}').empty:
        return {"Erro": "Cliente já existe"}
    
    for correction in unnecessary_keys_normalized:
        
        if correction == "concatenar":
            filters[columns[correction]] = filters["Cidade"] + str(filters["Cód Cliente"])
        
        if correction == "concatenar distri":
            filters[columns[correction]] = filters["Cidade"] + str(filters["Estado"])

        else:
            if correction in filter_keys:
                if filters[columns[correction]] == None:  pass
                elif filters[columns[correction]] == "":
                    filters[columns[correction]] = None
            
            elif correction not in filter_keys:
                filters[columns[correction]] = None

    
    filters = {columns[key]: filters[columns[key]]   for key in normalized_columns}

    sqlm.sql_post("CLIENTS", *filters.values())

    return {"Status": "Cliente criado"}


def put():

    columns = list((sqlm.get_sql("SELECT * FROM CLIENTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    filters = dict(request.get_json())
    normalized_filters = dt.dict_normalization(filters)

    if "cod cliente" not in normalized_filters:
        return {"Status": "Código do Cliente necessário"}



    return {"Status": "Cliente PUT"}


def delete():
    
    columns = list((sqlm.get_sql("SELECT * FROM CLIENTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    filters = dict(request.get_json())
    normalized_filters = dt.dict_normalization(filters)

    if "cod cliente" not in normalized_filters:
        return {"Erro": "Código do Cliente necessário"}

    if sqlm.get_sql(f'SELECT * FROM CLIENTS WHERE "Cód Cliente" = {filters["Cód Cliente"]}').empty:
        return {"Erro": "Cliente não existe"}

    query = f"""DELETE FROM CLIENTS WHERE "{columns["cod cliente"]}" = {filters[columns["cod cliente"]]}"""
    
    conn = sqlite3.connect("database\database.db")
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    
    return {"Status": "Cliente deletado"}


if __name__ == "__main__":
    pass