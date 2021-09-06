from flask import request
import functions.data_treatment as dt
import functions.autocorrection as autc

def get():
    """
    Método GET do servidor dos produtos
    """

    return {"texto": "Produto GET"}


def post():
    """
    Método POST do servidor dos produtos
    """

    #pega a tabela e cria uma copia normalizada
    wanted_data = dt.get_sql(f"SELECT * FROM PRODUCTS")
    response = dt.dataframe_normalization(wanted_data.copy())
    
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
                filters[key] = "".join(hold.split(" "))
            
            else:
                filters[key] = hold
                #realiza correções de erros pontuais
                filters[key] = autc.f_correction(filters[key])

        except: pass

    response = autc.table_correction(response)

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
                wanted_data = wanted_data.loc[response[key].str.contains(f"{value}")]


    wanted_data["EAN"] = wanted_data["EAN"].astype(str)
    
    if wanted_data.empty:
        return {"lista": "No data to return"}, 204

    return {"lista": wanted_data.to_dict("records")}, 200