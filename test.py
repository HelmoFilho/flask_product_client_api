import sqlite3

# path = "database\database.db"

# conn = sqlite3.connect(path)
# cursor = conn.cursor()

# query = 'INSERT INTO CLIENTS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
# columns = ("CE999999999999", 999999999999, "CE", "FORTALEZA", 10000000.44, "Faixa 8", "CEFORTALEZA", "Atac Ind", "Area I", "CNPJ", "PDV EXISTENTE", None, None, None,"Faixa 8")

# cursor.execute(query, columns)
# conn.commit()

def sql_post(table, *args):

    table = table.upper()

    base = f"INSERT INTO {table} VALUES ("

    for count in range(len(args)):
        base += "?"
        if count != len(args) - 1:
            base += ","
    
    base += ")"
    
    path = "database\database.db"

    print(args)
    print(base)

    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    cursor.execute(base, args)
    conn.commit()

data = (None, 9999999999999999, "CE", "FORTALEZA", 99999999.99, "Faixa 8", "CEFORTALEZA", "Atac Ind", "Area I", "CNPJ", "PDV EXISTENTE", None, None, None, "Faixa 8")


if __name__ == "__main__":
    x = None

    if "" == None:
        print(True)