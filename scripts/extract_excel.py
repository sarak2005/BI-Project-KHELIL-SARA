import pandas as pd
import os

# Dossier source (Excel exporté depuis Access)
input_folder = "data/sources/"

# Dossier de sortie
output_folder = "../data/raw/excel_sources/"
os.makedirs(output_folder, exist_ok=True)

# Tables Excel à charger
excel_files = {
    "Customers": "Customers.xlsx",
    "Employees": "Employees.xlsx",
    "Orders": "Orders.xlsx"
}

for name, file in excel_files.items():
    path = os.path.join(input_folder, file)
    df = pd.read_excel(path)

    output_path = os.path.join(output_folder, name + ".csv")
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f" Fichier Excel importé : {file}")

print("\n Extraction Excel terminée ! Les fichiers sont dans data/raw/excel_sources/")
