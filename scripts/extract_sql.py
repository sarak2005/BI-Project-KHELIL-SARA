import pyodbc #pouvoir se connecter a sql server
import pandas as pd #transformer les tables sql server en csv
import os #creer des dossier

#Connexion à SQL Server
conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost\\SQLEXPKHELIL;"
    "Database=Northwind;"
    "UID=sa;"
    "PWD=khelil2059;"
)

conn = pyodbc.connect(conn_str)

#Tables à extraire
tables = [
    "Customers",
    "Employees",
    "Orders"
]

#Dossier de sortie
output_folder = "../data/raw/sql_sources/"
os.makedirs(output_folder, exist_ok=True)

#Extraction + export
for table in tables:
    query = f"SELECT * FROM [{table}]"
    df = pd.read_sql(query, conn)

    file_name = table.replace(" ", "_") + ".csv"
    output_path = os.path.join(output_folder, file_name)

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f" Table exportée : {table}")

conn.close()

print("\n Extraction SQL Server terminée ! Les fichiers sont dans data/raw/sql_sources/")
