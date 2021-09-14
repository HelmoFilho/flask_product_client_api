from flask import request
from unidecode import unidecode
import numpy as np
import sqlite3

import functions.data_treatment as dt
import functions.sql_management as sqlm
import functions.autocorrection as autc

def get():
    """
    Método GET do servidor dos produtos
    """
    #pega a tabela e cria uma copia normalizada
    wanted_data = sqlm.get_sql(f"SELECT * FROM PRODUCTS")
    response = dt.dataframe_normalization(wanted_data.copy())

    response = autc.table_correction(response)
    
    #cria a bolsa de palavras para correção de erros de digitação
    autc.bag_of_words(response)

    #normaliza o request
    filters = request.get_json()
    filters = dt.dict_normalization(filters)

    for key in ["desc", "sub marca", "sub marca grama", "marca", "categoria"]:
        try:
            #realiza correções de erros de digitação
            hold = autc.correct(filters[key])
            
            if key != "desc":
                filters[key] = " ".join(hold.split(" "))
            
            else:
                filters[key] = hold
                #realiza correções de erros pontuais
                filters[key] = autc.f_correction(filters[key])

        except: pass

    print(filters)

    #if not response.empty:
    #    return {"lista": response.to_dict("records")}, 200
    
    #checa na tabela por cada key do request
    for key in filters.keys():
        
        if "desc" not in key:
            if ("ean" in key and "inco" in key) or "ean" not in key:
                wanted_data = wanted_data.loc[response[key].str.contains(filters[key])]
            
            elif key == "ean":
                wanted_data = wanted_data.loc[response[key].str.startswith(filters[key])]

        #realiza a busca palavra a palavra
        elif "desc" in key:
    
            for value in filters['desc'].split():
                wanted_data = wanted_data.loc[response[key].str.contains(value)]


    wanted_data["EAN"] = wanted_data["EAN"].astype(str)
    
    if wanted_data.empty:
        return {"Erro": "Dados não encontrados"}, 200

    return {"Data": wanted_data.to_dict("records")}, 200


def post():
    """
    Método POST do servidor dos produtos
    """
    filters = dict(request.get_json())

    # Serve para criar um dicionário para pegar o nome original atravéz do nome normalizado
    columns = list((sqlm.get_sql("SELECT * FROM PRODUCTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    # Serve para criar uma lista das colunas que não podem receber valores nulos
    not_null = ['REGIÃO','EAN', 'SKUs PUSH & PULL - EB2B', 'CATEGORIA', 'MARCA', 'SUB MARCA', 'SUB MARCA GRAMA', 'DESC']
    normalized_not_null = [unidecode(column.lower())   for column in not_null]

    # cria um dicionário com o nome original como valor e o nome normalizado como chave
    filter_keys = list(filters.keys())
    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    filter_keys = dict(zip(normalized_keys, filter_keys))

    # lista com chaves que não são obrigatórias
    unnecessary_keys_normalized = [un_key  for un_key in normalized_columns  if un_key not in normalized_not_null]

    # Verifica se alguma das chaves obrigatórias está faltando
    missing_keys = []
    for key in normalized_not_null:
        if key not in normalized_keys:
            missing_keys.append(columns[key])

    if missing_keys:
            return {"Erro - Chaves faltando": missing_keys}

    # Verifica se existe alguma chave que não existe na tabela
    invalid_keys = []
    for key in normalized_keys:
        if key not in normalized_columns:
            invalid_keys.append(filter_keys[key])

    if invalid_keys:
        return {"Erro - Chaves invalidas": invalid_keys}

    # Verifica se as chaves obrigatórias receberam valor nulo ou vazio
    invalid_values = []
    for key, value in zip(filter_keys, filters.values()):
        if (value == None or value == "") and key in normalized_not_null:
            invalid_values.append(key)
    
    if invalid_values:
        return {"Erro - Valores invalidos": invalid_values}

    # Verifica se os valores estão no tipo correto
    incorrect_values = []
    for key, value in zip(filter_keys, filters.values()):
        
        #exclusivo para o "EAN"
        if key == "ean":
            try: 
                int(str(value))
            except: 
                incorrect_values.append(columns[key])

        if "faixa" in key:
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

    # Realiza correções em certas chaves
    for correction in unnecessary_keys_normalized:

        if correction in filter_keys:
            if filters[columns[correction]] == None:  pass
            elif filters[columns[correction]] == "":
                filters[columns[correction]] = None
            #Correção para as faixas
            elif float(filters[columns[correction]]) > 1:
                 filters[columns[correction]] = 1
            elif float(filters[columns[correction]]) < 0:
                 filters[columns[correction]] = 0
            elif float(filters[columns[correction]]) < 1 and float(filters[columns[correction]]) > 0:
                 filters[columns[correction]] = np.round(filters[columns[correction]], 0)
        
        elif correction not in filter_keys:
            filters[columns[correction]] = None

    # recria o dicionário
    filter_keys = list(filters.keys())
    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    filter_keys = dict(zip(normalized_keys, filter_keys))

    filters = {columns[key]: filters[filter_keys[key]]   for key in normalized_columns}

    # Verifica se o dado já existe
    if not sqlm.get_sql(f'SELECT * FROM PRODUCTS WHERE "EAN" = {filters[filter_keys["ean"]]}').empty:
        return {"Erro": "Produto já registrado"}

    sqlm.sql_post("PRODUCTS", *filters.values())

    return {"Status": "Produto registrado"}


def put():
    """
    Método PUT do servidor dos produtos
    """
    filters = dict(request.get_json())

    # Serve para criar um dicionário para pegar o nome original atravéz do nome normalizado
    columns = list((sqlm.get_sql("SELECT * FROM PRODUCTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    # Serve para criar uma lista das colunas que não podem receber valores nulos
    not_null = ['REGIÃO','EAN', 'SKUs PUSH & PULL - EB2B', 'CATEGORIA', 'MARCA', 'SUB MARCA', 'SUB MARCA GRAMA', 'DESC']
    normalized_not_null = [unidecode(column.lower())   for column in not_null]

    # cria um dicionário com o nome original como valor e o nome normalizado como chave
    keys = list(filters.keys())
    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    keys = dict(zip(normalized_keys, keys))

    # lista com chaves que não são obrigatórias
    unnecessary_keys_normalized = [un_key  for un_key in normalized_columns  if un_key not in normalized_not_null]

    # Checa se o código do cliente foi fornecido
    if "ean" not in normalized_keys:
        return {"Erro": "EAN do produto necessário"}

    # Verifica se existe alguma chave que não existe na tabela
    invalid_keys = []
    for key in normalized_keys:
        if key not in normalized_columns:
            invalid_keys.append(keys[key])

    if invalid_keys:
        return {"Erro - Chaves invalidas": invalid_keys}

    # Verifica se as chaves obrigatórias receberam valor nulo ou vazio
    invalid_values = []
    for key, value in zip(keys, filters.values()):
        if (value == None or value == "") and key in normalized_not_null:
            invalid_values.append(key)
    
    if invalid_values:
        return {"Erro - Valores invalidos": invalid_values}

    # Verifica se os valores estão no tipo correto
    incorrect_values = []
    for key, value in zip(keys, filters.values()):
        
        #exclusivo para o "EAN"
        if key == "ean":
            try: 
                int(str(value))
            except: 
                incorrect_values.append(columns[key])

        if "faixa" in key:
            if value != None: 
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

    # Realiza correções para as faixas
    for correction in unnecessary_keys_normalized:

        if correction in normalized_keys and "faixa" in correction:
            
            if filters[columns[correction]] == None:
                filters[columns[correction]] = None
            elif float(filters[columns[correction]]) > 1:
                filters[columns[correction]] = 1
            elif float(filters[columns[correction]]) < 0:
                filters[columns[correction]] = 0
            elif float(filters[columns[correction]]) < 1 and float(filters[columns[correction]]) > 0:
                filters[columns[correction]] = np.round(filters[columns[correction]], 0)

    # Cria a string para a query
    string = ''
    
    for key in normalized_keys:
    
        if key != "ean":
            string += f""""{columns[key]}" = ?,"""
    
    # Remove a ultima vírgula
    string = string[:-1]

    # Cria a string completa para realizar a query de update
    full_string = f"""UPDATE PRODUCTS SET {string} WHERE "EAN" = {filters[keys["ean"]]}"""

    # Verifica se o dado já existe
    if sqlm.get_sql(f'SELECT * FROM PRODUCTS WHERE "EAN" = {filters[keys["ean"]]}').empty:
        return {"Erro": "Produto não registrado"}
    
    del filters[keys["ean"]]

    # Realiza o update
    conn = sqlite3.connect("database\database.db")
    cursor = conn.cursor()
    cursor.execute(full_string, list(filters.values()))
    conn.commit()

    return {"Status": "Produto atualizado"}


def delete():
    """
    Método DELETE do servidor dos produtos
    """
    # Serve para criar  um dicionário para pegar o nome original atravéz do nome normalizado
    columns = list((sqlm.get_sql("SELECT * FROM PRODUCTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    # Pega o request
    filters = dict(request.get_json())
    
    # Cria um dicionário com o nome das chaves como valor e as chaves normalizadas como chave
    keys = list(filters.keys())
    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    keys = {key: value  for key, value in zip(normalized_keys, keys)}

    # Checa se o código do cliente foi fornecido
    if "ean" not in normalized_keys:
        return {"Erro": "EAN do produto necessário"}

    # Checa se o cliente existe
    if sqlm.get_sql(f'SELECT * FROM PRODUCTS WHERE "EAN" = {filters[keys["ean"]]}').empty:
        return {"Erro": "Produto não registrado"}

    # Cria a string completa para realizar a query de delete
    query = f"""DELETE FROM PRODUCTS WHERE "{columns["ean"]}" = {filters[keys["ean"]]}"""
    
    # Realiza o delete
    conn = sqlite3.connect("database\database.db")
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    
    return {"Status": "Produto deletado"}