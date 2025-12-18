import os
import pandas as pd
import numpy as np
import unidecode

BASE = os.path.join(os.path.dirname(__file__), "..")
RAW_SQL = os.path.join(BASE, "data", "raw", "sql_sources")
RAW_EXCEL = os.path.join(BASE, "data", "raw", "excel_sources")
WAREHOUSE = os.path.join(BASE, "data", "warehouse")

os.makedirs(WAREHOUSE, exist_ok=True)

# -------------------------
# Helpers
# -------------------------
def find_csv(folder, candidates):
    """Return path to first existing candidate file in folder (case-insensitive)."""
    if not os.path.exists(folder):
        return None
    files = {f.lower(): f for f in os.listdir(folder)}
    for c in candidates:
        key = c.lower()
        if key in files:
            return os.path.join(folder, files[key])
    return None

def normalize_text(s):
    if pd.isna(s):
        return ""
    s = str(s)
    s = unidecode.unidecode(s).lower().strip()
    s = s.replace("\n", " ").replace("\r", " ")
    s = s.replace("-", " ").replace("_", " ")
    s = " ".join(s.split())
    return s

def safe_read_csv(path):
    if path is None or not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])

# -------------------------
# Discover files (SQL)
# -------------------------
sql_customers_path = find_csv(RAW_SQL, ["Customers.csv", "customers.csv"])
sql_employees_path = find_csv(RAW_SQL, ["Employees.csv", "employees.csv"])
sql_orders_path = find_csv(RAW_SQL, ["Orders.csv", "orders.csv"])

# Excel/raw-excel (after extract_excel)
excel_customers_path = find_csv(RAW_EXCEL, ["customers.csv", "customers_excel.csv", "Customers.csv"])
excel_employees_path = find_csv(RAW_EXCEL, ["employees.csv", "employees_excel.csv", "Employees.csv"])
excel_orders_path = find_csv(RAW_EXCEL, ["orders.csv", "orders_excel.csv", "Orders.csv"])

# Load
sql_customers = safe_read_csv(sql_customers_path)
sql_employees = safe_read_csv(sql_employees_path)
sql_orders = safe_read_csv(sql_orders_path)

excel_customers = safe_read_csv(excel_customers_path)
excel_employees = safe_read_csv(excel_employees_path)
excel_orders = safe_read_csv(excel_orders_path)

# -------------------------
# Standardize / minimal rename
# -------------------------
# Customers: create columns -> customerid_sql (from SQL), customer_source_id (from Excel), companyname, region, city, country
def standardize_customers_sql(df):
    if df.empty:
        return pd.DataFrame(columns=["customerid_sql","companyname","region","city","country","phone","fax","company_norm"])
    df2 = df.copy()
    # common SQL names: CustomerID, CompanyName, Region, City, Country, Phone, Fax
    df2.columns = [c.strip() for c in df2.columns]
    out = pd.DataFrame()
    out["customerid_sql"] = df2.get("CustomerID", df2.get("customerid", pd.NA)).astype(str)
    out["companyname"] = df2.get("CompanyName", df2.get("Company", df2.get("companyname", "")))
    out["region"] = df2.get("Region", df2.get("region", ""))
    out["city"] = df2.get("City", df2.get("city", ""))
    out["country"] = df2.get("Country", df2.get("country", ""))
    out["phone"] = df2.get("Phone", "")
    out["fax"] = df2.get("Fax", "")
    out["company_norm"] = out["companyname"].apply(normalize_text)
    return out

def standardize_customers_excel(df):
    if df.empty:
        return pd.DataFrame(columns=["customer_source_id","companyname","region","city","country","phone","fax","company_norm"])
    df2 = df.copy()
    df2.columns = [c.strip() for c in df2.columns]
    out = pd.DataFrame()
    # Excel may have ID, Company, State/Province, Country/Region, Business Phone, Fax Number
    out["customer_source_id"] = df2.get("ID", df2.get("Id", ""))
    out["companyname"] = df2.get("Company", df2.get("company", ""))
    out["region"] = df2.get("State/Province", df2.get("State", ""))
    out["city"] = df2.get("City", "")
    out["country"] = df2.get("Country/Region", df2.get("Country", ""))
    out["phone"] = df2.get("Business Phone", "")
    out["fax"] = df2.get("Fax Number", "")
    out["company_norm"] = out["companyname"].apply(normalize_text)
    return out

# Employees standardization
def standardize_employees_sql(df):
    if df.empty:
        return pd.DataFrame(columns=["employeeid_sql","firstname","lastname","title","emp_norm"])
    df2 = df.copy()
    df2.columns = [c.strip() for c in df2.columns]
    out = pd.DataFrame()
    out["employeeid_sql"] = df2.get("EmployeeID", df2.get("employeeid", pd.NA)).astype(str)
    out["firstname"] = df2.get("FirstName", df2.get("firstname", ""))
    out["lastname"] = df2.get("LastName", df2.get("lastname", ""))
    out["title"] = df2.get("Title", "")
    out["emp_norm"] = (out["firstname"].fillna("") + " " + out["lastname"].fillna("")).apply(normalize_text)
    return out

def standardize_employees_excel(df):
    if df.empty:
        return pd.DataFrame(columns=["employee_source_id","firstname","lastname","title","emp_norm"])
    df2 = df.copy()
    df2.columns = [c.strip() for c in df2.columns]
    out = pd.DataFrame()
    out["employee_source_id"] = df2.get("ID", df2.get("Id", ""))
    out["firstname"] = df2.get("First Name", df2.get("FirstName", ""))
    out["lastname"] = df2.get("Last Name", df2.get("LastName", ""))
    out["title"] = df2.get("Job Title", df2.get("Title", ""))
    out["emp_norm"] = (out["firstname"].fillna("") + " " + out["lastname"].fillna("")).apply(normalize_text)
    return out

# Orders standardization
def standardize_orders_sql(df):
    if df.empty:
        return pd.DataFrame(columns=["orderid_sql","customerid_sql","employeeid_sql","orderdate","shippeddate","freight"])
    df2 = df.copy()
    df2.columns = [c.strip() for c in df2.columns]
    out = pd.DataFrame()
    out["orderid_sql"] = df2.get("OrderID", df2.get("orderid", pd.NA)).astype(str)
    out["customerid_sql"] = df2.get("CustomerID", df2.get("customerid", ""))
    out["employeeid_sql"] = df2.get("EmployeeID", df2.get("employeeid", ""))
    out["orderdate"] = pd.to_datetime(df2.get("OrderDate", df2.get("orderdate", pd.NaT)), errors="coerce")
    out["shippeddate"] = pd.to_datetime(df2.get("ShippedDate", df2.get("shippeddate", pd.NaT)), errors="coerce")
    out["freight"] = pd.to_numeric(df2.get("Freight", df2.get("freight", 0)), errors="coerce").fillna(0)
    return out

def standardize_orders_excel(df):
    if df.empty:
        return pd.DataFrame(columns=["orderid_ex","customer_source_ref","employee_source_ref","orderdate","shippeddate","freight"])
    df2 = df.copy()
    df2.columns = [c.strip() for c in df2.columns]
    out = pd.DataFrame()
    out["orderid_ex"] = df2.get("Order ID", df2.get("OrderID", pd.NA)).astype(str)
    out["customer_source_ref"] = df2.get("Customer", df2.get("Company", ""))
    out["employee_source_ref"] = df2.get("Employee", df2.get("EmployeeName", ""))
    out["orderdate"] = pd.to_datetime(df2.get("Order Date", df2.get("OrderDate", pd.NaT)), errors="coerce")
    out["shippeddate"] = pd.to_datetime(df2.get("Shipped Date", df2.get("ShippedDate", pd.NaT)), errors="coerce")
    out["freight"] = pd.to_numeric(df2.get("Shipping Fee", df2.get("Freight", 0)), errors="coerce").fillna(0)
    # normalized refs
    out["customer_norm"] = out["customer_source_ref"].apply(normalize_text)
    out["employee_norm"] = out["employee_source_ref"].apply(normalize_text)
    return out

# -------------------------
# Standardize dataframes
# -------------------------
sql_c = standardize_customers_sql(sql_customers)
sql_e = standardize_employees_sql(sql_employees)
sql_o = standardize_orders_sql(sql_orders)

ex_c = standardize_customers_excel(excel_customers)
ex_e = standardize_employees_excel(excel_employees)
ex_o = standardize_orders_excel(excel_orders)

# -------------------------
# Build dim_customers: union but keep company_norm as dedupe key
# -------------------------
cust_all = pd.concat([
    sql_c.rename(columns={"customerid_sql":"customerid"}).assign(source="sql"),
    ex_c.rename(columns={"customer_source_id":"customerid"}).assign(source="excel")
], ignore_index=True, sort=False)

# Ensure company_norm exists
cust_all["company_norm"] = cust_all["company_norm"].fillna(cust_all["companyname"].apply(normalize_text))

# Deduplicate by normalized company name, prefer SQL rows when both exist
cust_all["source_rank"] = cust_all["source"].map({"sql": 0, "excel": 1})
cust_all = cust_all.sort_values(["company_norm","source_rank"]).drop_duplicates(subset=["company_norm"], keep="first").reset_index(drop=True)

# Assign numeric surrogate key
cust_all.insert(0, "customer_key", range(1, len(cust_all)+1))

# Save dim_customers with fields commonly used
dim_customers = cust_all[["customer_key", "customerid", "companyname", "company_norm", "region", "city", "country", "phone", "fax", "source"]]
dim_customers.to_csv(os.path.join(WAREHOUSE, "dim_customers.csv"), index=False)

# -------------------------
# Build dim_employees
# -------------------------
emp_all = pd.concat([
    sql_e.rename(columns={"employeeid_sql":"employeeid"}).assign(source="sql"),
    ex_e.rename(columns={"employee_source_id":"employeeid"}).assign(source="excel")
], ignore_index=True, sort=False)

emp_all["emp_norm"] = emp_all["emp_norm"].fillna(
    (emp_all["firstname"].fillna("") + " " + emp_all["lastname"].fillna("")).apply(normalize_text)
)

emp_all["source_rank"] = emp_all["source"].map({"sql":0,"excel":1})
emp_all = emp_all.sort_values(["emp_norm","source_rank"]).drop_duplicates(
    subset=["emp_norm"], keep="first"
).reset_index(drop=True)

emp_all.insert(0, "employee_key", range(1, len(emp_all)+1))

# Colonnes réellement disponibles
employee_cols = [c for c in [
    "employee_key","employeeid","firstname","lastname","title",
    "emp_norm","city","region","country","homephone","notes","source"
] if c in emp_all.columns]

dim_employees = emp_all[employee_cols]
dim_employees.to_csv(os.path.join(WAREHOUSE, "dim_employees.csv"), index=False)

# -------------------------
# Build dim_temps (full calendar)
# -------------------------
# Determine min/max across both sources
min_date = min(
    pd.to_datetime(sql_o["orderdate"].min(), errors="coerce"),
    pd.to_datetime(ex_o["orderdate"].min(), errors="coerce")
)
max_date = max(
    pd.to_datetime(sql_o["orderdate"].max(), errors="coerce"),
    pd.to_datetime(ex_o["orderdate"].max(), errors="coerce")
)
if pd.isna(min_date) or pd.isna(max_date):
    today = pd.Timestamp.today().normalize()
    min_date = today - pd.Timedelta(days=365)
    max_date = today
all_dates = pd.date_range(start=min_date.normalize(), end=max_date.normalize(), freq="D")
dim_temps = pd.DataFrame({"date": all_dates})
dim_temps["date_key"] = dim_temps["date"].dt.strftime("%Y%m%d").astype(int)
dim_temps["year"] = dim_temps["date"].dt.year
dim_temps["month"] = dim_temps["date"].dt.month
dim_temps["day"] = dim_temps["date"].dt.day
dim_temps["weekday"] = dim_temps["date"].dt.day_name()
dim_temps.to_csv(os.path.join(WAREHOUSE, "dim_temps.csv"), index=False)

# -------------------------
# Build fact_orders (UNION SQL + EXCEL) and map keys
# -------------------------
# Prepare SQL orders
if not sql_o.empty:
    sql_o2 = sql_o.rename(columns={
        "orderid_sql":"orderid",
        "customerid_sql":"customerid",
        "employeeid_sql":"employeeid"
    }).copy()
    sql_o2["source"] = "sql"
    # For mapping, compute company_norm via customerid -> lookup companyname in sql customers
    sql_customer_map = sql_c.set_index("customerid_sql")["company_norm"].to_dict()
    sql_o2["company_norm"] = sql_o2["customerid"].map(lambda x: sql_customer_map.get(str(x), ""))
    # For employee: map id to emp_norm
    sql_emp_map = sql_e.set_index("employeeid_sql")["emp_norm"].to_dict()
    sql_o2["employee_norm"] = sql_o2["employeeid"].map(lambda x: sql_emp_map.get(str(x), ""))
else:
    sql_o2 = pd.DataFrame(columns=["orderid","customerid","employeeid","orderdate","shippeddate","freight","source","company_norm","employee_norm"])

# Prepare Excel orders
if not ex_o.empty:
    ex_o2 = ex_o.rename(columns={
        "orderid_ex":"orderid",
        "customer_source_ref":"customer_source",
        "employee_source_ref":"employee_source"
    }).copy()
    ex_o2["source"] = "excel"
    # company_norm and employee_norm already exist in ex_o
    ex_o2["company_norm"] = ex_o2.get("customer_norm", ex_o2.get("customer_source","")).apply(normalize_text)
    ex_o2["employee_norm"] = ex_o2.get("employee_norm", ex_o2.get("employee_source","")).apply(normalize_text)
else:
    ex_o2 = pd.DataFrame(columns=["orderid","customer_source","employee_source","orderdate","shippeddate","freight","source","company_norm","employee_norm"])

# Union orders
orders_union = pd.concat([sql_o2, ex_o2], ignore_index=True, sort=False)
orders_union["orderdate"] = pd.to_datetime(orders_union["orderdate"], errors="coerce")
orders_union["shippeddate"] = pd.to_datetime(orders_union["shippeddate"], errors="coerce")
orders_union["delivered"] = orders_union["shippeddate"].notna().astype(int)

# Map customer_key: first try matching company_norm to dim_customers.company_norm
cust_norm_map = dim_customers.set_index("company_norm")["customer_key"].to_dict()
orders_union["customer_key"] = orders_union.get("company_norm","").map(cust_norm_map)

# Map employee_key via emp_norm
emp_norm_map = dim_employees.set_index("emp_norm")["employee_key"].to_dict()
orders_union["employee_key"] = orders_union.get("employee_norm","").map(emp_norm_map)

# For rows where mapping failed but a SQL customerid exists, try mapping SQL id -> dim_customers.customerid
# Build map of customerid_sql -> customer_key
cust_id_map = dim_customers.set_index("customerid")["customer_key"].to_dict()
orders_union.loc[orders_union["customer_key"].isna() & orders_union.get("customerid").notna(), "customer_key"] = \
    orders_union.loc[orders_union["customer_key"].isna() & orders_union.get("customerid").notna(), "customerid"].map(lambda x: cust_id_map.get(str(x)))

# For employee: try employeeid mapping if present
emp_id_map = dim_employees.set_index("employeeid")["employee_key"].to_dict()
orders_union.loc[orders_union["employee_key"].isna() & orders_union.get("employeeid").notna(), "employee_key"] = \
    orders_union.loc[orders_union["employee_key"].isna() & orders_union.get("employeeid").notna(), "employeeid"].map(lambda x: emp_id_map.get(str(x)))

# Map date_key
date_map = dim_temps.set_index(dim_temps["date"].dt.strftime("%Y-%m-%d"))["date_key"].to_dict()
orders_union["orderdate_key"] = orders_union["orderdate"].dt.strftime("%Y-%m-%d").map(date_map)
orders_union["shippeddate_key"] = orders_union["shippeddate"].dt.strftime("%Y-%m-%d").map(date_map)

# Create surrogate fact_key numeric
orders_union = orders_union.reset_index(drop=True)
orders_union.insert(0, "fact_key", range(1, len(orders_union)+1))

# Select and save final fact_orders with orderdate included (for reporting)
fact_orders = orders_union[[
    "fact_key",
    "orderid",
    "source",
    "orderdate",
    "orderdate_key",
    "shippeddate",
    "shippeddate_key",
    "customer_key",
    "employee_key",
    "delivered",
    "freight",
    "company_norm",
    "employee_norm"
]].copy()

fact_orders.to_csv(os.path.join(WAREHOUSE, "fact_orders.csv"), index=False)

# Save processed copies of dims for traceability
dim_customers.to_csv(os.path.join(WAREHOUSE, "dim_customers.csv"), index=False)
dim_employees.to_csv(os.path.join(WAREHOUSE, "dim_employees.csv"), index=False)
dim_temps.to_csv(os.path.join(WAREHOUSE, "dim_temps.csv"), index=False)

print("✅ Data warehouse construit :")
print(" - dim_customers:", os.path.join(WAREHOUSE, "dim_customers.csv"))
print(" - dim_employees:", os.path.join(WAREHOUSE, "dim_employees.csv"))
print(" - dim_temps   :", os.path.join(WAREHOUSE, "dim_temps.csv"))
print(" - fact_orders :", os.path.join(WAREHOUSE, "fact_orders.csv"))
print(f"Nombre de lignes fact_orders = {len(fact_orders)}")
