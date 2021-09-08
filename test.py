import sqlite3

path = "database\database.db"

conn = sqlite3.connect(path)
cursor = conn.cursor()

query = 'INSERT INTO CLIENTS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
columns = ("CE999999999999", 999999999999, "CE", "FORTALEZA", 10000000.44, "Faixa 8", "CEFORTALEZA", "Atac Ind", "Area I", "CNPJ", "PDV EXISTENTE", None, None, None,"Faixa 8")

cursor.execute(query, columns)
conn.commit()