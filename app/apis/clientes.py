from flask import request
from unidecode import unidecode
import sqlite3

import functions.data_treatment as dt
import functions.sql_management as sqlm


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
        return {"Erro": "Cliente não existe"}

    return {"data": response.to_dict("records")}    


def post():
    """
    Método POST do servidor dos clientes
    """
    filters = dict(request.get_json())

    # Serve para criar um dicionário para pegar o nome original atravéz do nome normalizado
    columns = list((sqlm.get_sql("SELECT * FROM CLIENTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    # Serve para criar uma lista das colunas que não podem receber valores nulos
    not_null = ['Cód Cliente','Cidade', 'Estado', 'Valor', 'Faixas', 'Perfil', 'Area Nielsen', 'TIPO', 'CHECK', 'Faixa do Sortimento']
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
    
    # Realiza correções em certas chaves
    for correction in unnecessary_keys_normalized:
        
        if correction == "concatenar":
            filters[columns[correction]] = filters[filter_keys["cidade"]] + str(filters[filter_keys["cod cliente"]])
        
        if correction == "concatenar distri":
            filters[columns[correction]] = filters[filter_keys["cidade"]] + str(filters[filter_keys["estado"]])

        else:
            if correction in filter_keys:
                if filters[columns[correction]] == None:  pass
                elif filters[columns[correction]] == "":
                    filters[columns[correction]] = None
            
            elif correction not in filter_keys:
                filters[columns[correction]] = None

    # recria o dicionário
    filter_keys = list(filters.keys())
    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    filter_keys = dict(zip(normalized_keys, filter_keys))

    filters = {columns[key]: filters[filter_keys[key]]   for key in normalized_columns}

    # Verifica se o dado já existe
    if not sqlm.get_sql(f'SELECT * FROM CLIENTS WHERE "Cód Cliente" = {filters[filter_keys["cod cliente"]]}').empty:
        return {"Erro": "Cliente já existe"}
    
    sqlm.sql_post("CLIENTS", *filters.values())

    return {"Status": "Cliente criado"}


def put():
    """
    Método PUT do servidor dos clientes
    """

    # Serve para criar um dicionário para pegar o nome original atravéz do nome normalizado
    columns = list((sqlm.get_sql("SELECT * FROM CLIENTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    # Serve para pegar o dado pelo request
    filters = dict(request.get_json())
    
    # Cria um dicionário com o nome das chaves como valor e as chaves normalizadas como chave
    keys = list(filters.keys())
    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    keys = {key: value  for key, value in zip(normalized_keys, keys)}

    unnecessary_keys_normalized = ["concatenar", "concatenar distri", ".", "status", "rede / grupo"]

    # Checa se o código do cliente está presente visto que ele é necessário para encontrar o cliente
    if "cod cliente" not in normalized_keys:
        return {"Status": "Código do Cliente necessário"}

    return_data = sqlm.get_sql(f'SELECT * FROM CLIENTS WHERE "Cód Cliente" = {filters[keys["cod cliente"]]}')

    # Checa se o dado existe
    if return_data.empty:
        return {"Erro": "Cliente não existe"}

    # Verifica se existe alguma chave na requisição que não existe na tabela
    invalid_keys = []
    for key in normalized_keys:
        if key not in normalized_columns:
            invalid_keys.append(keys[key])

    if invalid_keys:
        return {"Erro - Chaves invalidas": invalid_keys}

    # Verifica se as chaves obrigatórias receberam valor nulo ou vazio
    invalid_values = []
    for key, value in zip(keys, filters.values()):
        if (value == None or value == "") and key not in unnecessary_keys_normalized:
            invalid_values.append(key)
    
    if invalid_values:
        return {"Erro - Valores invalidos": invalid_values}

    # Verifica se os valores estão no tipo correto
    incorrect_values = []
    for key, value in zip(keys, filters.values()):
        
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

    try: del filters[keys["concatenar"]]
    except: pass

    try: del filters[keys["concatenar distri"]]
    except: pass

    # Exclusivo para a coluna "Concatenar"
    if "cidade" in normalized_keys:
        filters["Concatenar"] = filters[keys['cidade']] + str(filters[keys['cod cliente']])

    # Exclusivo para a coluna "Concatenar Distri"
    if any(test in normalized_keys  for test in ["cidade", "estado"]):
        
        if "cidade" in normalized_keys and "estado" in normalized_keys:
            filters["Concatenar Distri"] = filters[keys['cidade']] + filters[keys['estado']]
        
        elif "cidade" not in normalized_keys:
            filters["Concatenar Distri"] = return_data["Cidade"].values[0] + filters[keys['estado']]
        
        if "estado" not in normalized_keys:
            filters["Concatenar Distri"] =  filters[keys['cidade']] + return_data["Estado"].values[0]

    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    keys = {key: value  for key, value in zip(normalized_keys, filters.keys())}
    
    # Cria a string para a query
    string = ''
    
    for key in normalized_keys:

        if key != "cod cliente":
            string += f""""{columns[key]}" = ?,"""

    # Remove a ultima vírgula
    string = string[:-1]

    # Cria a string completa para realizar a query de update
    full_string = f"""UPDATE CLIENTS SET {string} WHERE "Cód Cliente" = {filters[keys["cod cliente"]]}"""
    
    del filters[keys["cod cliente"]]

    # Realiza o update
    conn = sqlite3.connect("database\database.db")
    cursor = conn.cursor()
    cursor.execute(full_string, list(filters.values()))
    conn.commit()

    return {"Status": "Cliente atualizado"}


def delete():
    """
    Método DELETE do servidor dos clientes
    """
    # Serve para criar  um dicionário para pegar o nome original atravéz do nome normalizado
    columns = list((sqlm.get_sql("SELECT * FROM CLIENTS LIMIT 5")).columns)
    normalized_columns = [unidecode(column.lower())   for column in columns]
    columns = dict(zip(normalized_columns, columns))

    # Pega o request
    filters = dict(request.get_json())
    
    # Cria um dicionário com o nome das chaves como valor e as chaves normalizadas como chave
    keys = list(filters.keys())
    normalized_keys = [unidecode(key.lower())   for key in filters.keys()]
    keys = {key: value  for key, value in zip(normalized_keys, keys)}

    # Checa se o código do cliente foi fornecido
    if "cod cliente" not in normalized_keys:
        return {"Erro": "Código do Cliente necessário"}

    # Checa se o cliente existe
    if sqlm.get_sql(f'SELECT * FROM CLIENTS WHERE "Cód Cliente" = {filters[keys["cod cliente"]]}').empty:
        return {"Erro": "Cliente não existe"}

    # Cria a string completa para realizar a query de delete
    query = f"""DELETE FROM CLIENTS WHERE "{columns["cod cliente"]}" = {filters[keys["cod cliente"]]}"""
    
    # Realiza o delete
    conn = sqlite3.connect("database\database.db")
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    
    return {"Status": "Cliente deletado"}


if __name__ == "__main__":
    pass