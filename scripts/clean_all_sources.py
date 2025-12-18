import pandas as pd
import os
import unidecode

SQL = "data/raw/sql_sources/"
EXCEL = "data/processed/excel_sources/"
OUT = "../data/processed/final/"
os.makedirs(OUT, exist_ok=True)

def normalize(s):
    if pd.isna(s): return ""
    return unidecode.unidecode(str(s).strip().lower())

# ------------------------
# CUSTOMERS
# ------------------------
sql = pd.read_csv(SQL + "Customers.csv")
sql["company_norm"] = sql["CompanyName"].apply(normalize)
sql["source"] = "sql"

ex = pd.read_csv(EXCEL + "customers_norm.csv")
ex["source"] = "excel"

final = pd.concat([sql, ex], ignore_index=True)
final.to_csv(OUT + "customers_all.csv", index=False)
print("✔ customers_all.csv (union SQL+Excel)")

# ------------------------
# EMPLOYEES
# ------------------------
sql = pd.read_csv(SQL + "Employees.csv")
sql["emp_norm"] = (sql["FirstName"] + " " + sql["LastName"]).apply(normalize)
sql["source"] = "sql"

ex = pd.read_csv(EXCEL + "employees_norm.csv")
ex["source"] = "excel"

final = pd.concat([sql, ex], ignore_index=True)
final.to_csv(OUT + "employees_all.csv", index=False)
print("✔ employees_all.csv")

# ------------------------
# ORDERS
# ------------------------
sql = pd.read_csv(SQL + "Orders.csv")
sql["OrderDate"] = pd.to_datetime(sql["OrderDate"], errors="coerce")
sql["ShippedDate"] = pd.to_datetime(sql["ShippedDate"], errors="coerce")
sql["delivered"] = sql["ShippedDate"].notna().astype(int)
sql["source"] = "sql"

# On doit aussi générer normalisation SQL:
sql["customer_norm"] = sql["CustomerID"].apply(lambda x: normalize(x))
sql["employee_norm"] = sql["EmployeeID"].apply(lambda x: normalize(x))

ex = pd.read_csv(EXCEL + "orders_norm.csv")
ex["source"] = "excel"

final = pd.concat([sql, ex], ignore_index=True)
final.to_csv(OUT + "orders_all.csv", index=False)
print("✔ orders_all.csv")
