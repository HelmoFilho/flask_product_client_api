import difflib as dl
from collections import Counter
import json
from sklearn.feature_extraction.text import CountVectorizer
import re
import numpy as np
import pandas as pd

def bag_of_words(data: pd.DataFrame):
    """Cria uma bolsa de palavras para serem utilizadas para filtrar erros de escrita do usuário

    Args:
        data (DataFrame): dataframe de onde será retirado as palavras
    """

    data_copy = data.copy()

    #pequena correção para a coluna de categoria
    data_copy["categoria"]  = data_copy["categoria"].str.replace("lim.", "limpador", regex = False)

    full_data = list(set(data_copy["categoria"].values + " " +
                data_copy["marca"].values + " " +
                data_copy["sub marca grama"].values + " " + 
                data_copy["desc"].values))

    #cria uma 'bag of words' utilizando o padrão
    vectorizer = CountVectorizer(analyzer='word', token_pattern = r"[a-z\-]{3,}")
    vectorizer.fit_transform(full_data)

    #Proposito único de criar o arquivo caso ele não exista
    try: 
        with open(r"database\autocomplete.json",'x') as file: 
            file_data = {"words": []}
            file.seek(0)
            json.dump(file_data, file, indent = 4)
    except: pass

    with open(r"database\autocomplete.json",'r+') as file:
        
        file_data = json.load(file)
        file_data["words"] = vectorizer.get_feature_names()
        file.seek(0)
        json.dump(file_data, file, indent = 4)
        file.truncate()


def correct(to_correct: str) -> str:
    """Corrige uma string conforme as palavras conseguidas na "bag of words"

    Args:
        to_correct (str): string a ser corrigida

    Returns:
        str: string corrigida
    """

    with open(r"database\autocomplete.json", "r") as f:
        data_to_substitute = json.load(f)["words"]
    bag = []
    for word in to_correct.split():
        #Arbitrário ser maior que duas letras
        if len(word) > 2:
            suggestions = []
            for suggestion in data_to_substitute:
                if len(suggestion) >= len(word):
                    bigger, lower = suggestion, word
                else:
                    bigger, lower = word, suggestion

                #diminui o tamanho da palavra maior para ficar igual ao da palavra menor
                number = len(bigger) - len(lower)
                pattern = "[a-z]{," + f"{number}" + "}\\b"
                bigger = re.sub(pattern,"",bigger)
                
                #conta a quantidade de letras iguais
                counter = sum((Counter(bigger) & Counter(lower)).values())

                #separa palavra somente se a quantidade de palavras iguais for maior que a metade de letras (arbitrário)
                if counter >= int(np.ceil(len(bigger)/2)):
                    suggestions.append(suggestion)

            #checa das palavras separadas qual a mais parecida com a requerida
            f_suggestion = dl.get_close_matches(word, suggestions, cutoff = 0.75)

            #se tiver salva na bolsa
            if f_suggestion:
                bag.append(f_suggestion[0])
            #caso não, retorna a palavra original
            else:
                bag.append(word)
        else:
            bag.append(word)

    #se existirem palavras na bolsa
    if bag:
        return " ".join(bag)

    #caso não, retorna a original
    return to_correct


def f_correction(data: str) -> str:
    """Realiza uma correção final em uma string para ficar na mesma conformidade da tabela

    Args:
        data (str): string a ser corrigida

    Returns:
        str: string corrigida
    """
    with open(r"database\clean_data.json", "r") as f:
        substitute = json.load(f)

    for master_key in substitute.keys():
        
        for key in substitute[master_key]:

            key_bool = True

            for value in substitute[master_key][key]:

                if key_bool and (value in data):

                    data = re.sub(f"\\b{value}", key, data)
                    key_bool = False

    return data


def table_correction(data: pd.DataFrame) -> pd.DataFrame:
    """Realiza a correção de erros na descrição dos produtos

    Args:
        data (pd.DataFrame): tabela a ser corrigida

    Returns:
        pd.DataFrame: tabela corrigida
    """

    with open(r"database\clean_data.json", "r") as f:
        substitute = json.load(f)

    wanted_data = data["desc"].to_list()

    for position in range(len(wanted_data)):

        #Serve para corrigir essa linha: "Sab Liq PROTEX INTIMO FRESH EQUILIB200ML"
        try:
            wanted_data[position] = re.sub("\\d+m", " " + re.findall(r'\d+', wanted_data[position])[0], wanted_data[position])
        except: pass

        for master_key in substitute.keys():
            
            for key in substitute[master_key]:

                key_bool = True

                for value in substitute[master_key][key]:

                    if key_bool and (value in wanted_data[position]):

                        wanted_data[position] = wanted_data[position].replace(value, key)
                        key_bool = False

    data["desc"] = pd.Series(wanted_data)

    return data


if __name__ == "__main__":
    print("t.")