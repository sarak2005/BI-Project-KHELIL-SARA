"""
kpi_analysis.py
Calcul des KPI principaux (Total commandes, Livrées, Non livrées, Taux de livraison),
par pays, par employé et par mois. Résultats affichés et sauvegardés.
"""
import os
import pandas as pd

BASE = os.path.join(os.path.dirname(__file__), "..")
WH = os.path.join(BASE, "data", "warehouse")

fact_path = os.path.join(WH, "fact_orders.csv")
dim_c_path = os.path.join(WH, "dim_customers.csv")
dim_e_path = os.path.join(WH, "dim_employees.csv")
dim_t_path = os.path.join(WH, "dim_temps.csv")

# Load
fact = pd.read_csv(fact_path, parse_dates=["orderdate","shippeddate"], keep_default_na=False, na_values=[""])
dim_c = pd.read_csv(dim_c_path, keep_default_na=False, na_values=[""])
dim_e = pd.read_csv(dim_e_path, keep_default_na=False, na_values=[""])
dim_t = pd.read_csv(dim_t_path, parse_dates=["date"], keep_default_na=False, na_values=[""])

# Safeguard
if fact.empty:
    print("⚠️ fact_orders.csv est vide — exécute datawarehouse.py d'abord.")
    raise SystemExit

# Basic KPIs
total_orders = len(fact)
delivered = int(fact["delivered"].sum())
not_delivered = total_orders - delivered
delivered_rate = (delivered / total_orders * 100) if total_orders > 0 else 0.0

print("\n===== KPI GLOBAUX =====")
print(f"Total commandes : {total_orders}")
print(f"Commandes livrées : {delivered}")
print(f"Commandes non livrées : {not_delivered}")
print(f"Taux de livraison : {delivered_rate:.2f}%")

# Orders by country (use dim_customers mapping)
# Merge to get country
fact_c = fact.merge(dim_c[['customer_key','country']], left_on='customer_key', right_on='customer_key', how='left')
orders_by_country = fact_c.groupby('country').agg(total_orders=('fact_key','count'), delivered=('delivered','sum')).reset_index()
orders_by_country['not_delivered'] = orders_by_country['total_orders'] - orders_by_country['delivered']
print("\n===== Commandes par pays (résumé) =====")
print(orders_by_country.sort_values('total_orders', ascending=False).head(20).to_string(index=False))

# Orders by employee
fact_e = fact.merge(dim_e[['employee_key','firstname','lastname']], left_on='employee_key', right_on='employee_key', how='left')
fact_e['employee_name'] = fact_e['firstname'].fillna('') + ' ' + fact_e['lastname'].fillna('')
orders_by_employee = fact_e.groupby('employee_name').agg(total_orders=('fact_key','count'), delivered=('delivered','sum')).reset_index()
orders_by_employee['not_delivered'] = orders_by_employee['total_orders'] - orders_by_employee['delivered']
print("\n===== Commandes par employé (résumé) =====")
print(orders_by_employee.sort_values('total_orders', ascending=False).head(20).to_string(index=False))

# Orders by month (use orderdate)
fact['period'] = pd.to_datetime(fact['orderdate'], errors='coerce').dt.to_period('M')
orders_by_month = fact.groupby('period').agg(total_orders=('fact_key','count'), delivered=('delivered','sum')).reset_index()
orders_by_month['not_delivered'] = orders_by_month['total_orders'] - orders_by_month['delivered']
print("\n===== Commandes par mois =====")
print(orders_by_month.sort_values('period').to_string(index=False))

# Save summaries
out_dir = os.path.join(WH, "kpi_summaries")
os.makedirs(out_dir, exist_ok=True)
orders_by_country.to_csv(os.path.join(out_dir, "orders_by_country.csv"), index=False)
orders_by_employee.to_csv(os.path.join(out_dir, "orders_by_employee.csv"), index=False)
orders_by_month.to_csv(os.path.join(out_dir, "orders_by_month.csv"), index=False)

print(f"\n✅ KPI summary files saved to {out_dir}")
