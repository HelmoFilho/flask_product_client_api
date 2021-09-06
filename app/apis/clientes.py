import functions.data_treatment as dt
from flask import request

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
    response = dt.get_sql(f"SELECT * FROM CLIENTS {condition}")

    if response.empty:
        return {"error": "No response"}, 204

    return {"data": response.to_dict("records")}    


def post():
    """
    Método POST do servidor dos clientes
    """

    return {"texto": "Cliente POST"}

if __name__ == "__main__":
    pass