import pandas as pd
import os
import unidecode

RAW = "data/raw/excel_sources/"
OUT = "../data/processed/excel_sources/"
os.makedirs(OUT, exist_ok=True)

def normalize_text(s):
    if pd.isna(s): return ""
    s = str(s).strip().lower()
    s = unidecode.unidecode(s)
    return " ".join(s.split())

# --------------------
# Customers
# --------------------
df = pd.read_csv(RAW + "Customers.csv")

norm = pd.DataFrame()
norm["customer_source_id"] = df["ID"]
norm["companyname"] = df["Company"]
norm["contactname"] = df["First Name"] + " " + df["Last Name"]
norm["address"]     = df["Address"]
norm["city"]        = df["City"]
norm["region"]      = df["State/Province"]
norm["postalcode"]  = df["ZIP/Postal Code"]
norm["country"]     = df["Country/Region"]
norm["phone"]       = df["Business Phone"]
norm["fax"]         = df["Fax Number"]
norm["company_norm"] = norm["companyname"].apply(normalize_text)

norm.to_csv(OUT + "customers_norm.csv", index=False)
print("✔ customers_norm.csv généré")

# --------------------
# Employees
# --------------------
df = pd.read_csv(RAW + "Employees.csv")

norm = pd.DataFrame()
norm["employee_source_id"] = df["ID"]
norm["firstname"] = df["First Name"]
norm["lastname"]  = df["Last Name"]
norm["title"]     = df["Job Title"]
norm["address"]   = df["Address"]
norm["city"]      = df["City"]
norm["region"]    = df["State/Province"]
norm["postalcode"]= df["ZIP/Postal Code"]
norm["country"]   = df["Country/Region"]
norm["notes"]     = df["Notes"]
norm["emp_norm"]  = (norm["firstname"] + " " + norm["lastname"]).apply(normalize_text)

norm.to_csv(OUT + "employees_norm.csv", index=False)
print("✔ employees_norm.csv généré")

# --------------------
# Orders
# --------------------
df = pd.read_csv(RAW + "Orders.csv")

df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
df["Shipped Date"] = pd.to_datetime(df["Shipped Date"], errors="coerce")

norm = pd.DataFrame()
norm["order_source_id"] = df["Order ID"]
norm["customer_norm"] = df["Customer"].apply(normalize_text)
norm["employee_norm"] = df["Employee"].apply(normalize_text)
norm["orderdate"]  = df["Order Date"]
norm["shippeddate"] = df["Shipped Date"]
norm["shipcountry"] = df["Ship Country/Region"]
norm["delivered"] = norm["shippeddate"].notna().astype(int)

norm.to_csv(OUT + "orders_norm.csv", index=False)
print("✔ orders_norm.csv généré")
