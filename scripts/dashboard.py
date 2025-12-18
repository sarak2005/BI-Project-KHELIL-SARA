import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html

# ====== LOAD FACT & DIMENSIONS ======
fact = pd.read_csv("data/warehouse/fact_orders.csv")
dim_customer = pd.read_csv("data/warehouse/dim_customers.csv")
dim_employee = pd.read_csv("data/warehouse/dim_employees.csv")
dim_time = pd.read_csv("data/warehouse/dim_temps.csv")

# ====== CLEAN COLUMN NAMES ======
for df in [fact, dim_customer, dim_employee, dim_time]:
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# ====== CREATE DESCRIPTIVE COLUMNS ======
dim_customer['customer and company'] = dim_customer['customerid'].astype(str) + " - " + dim_customer['companyname']
dim_employee['employee name'] = dim_employee['firstname'] + " " + dim_employee['lastname']

# ====== STAR SCHEMA CREATION ======
fact = fact.merge(
    dim_customer[['customer_key', 'customer and company', 'region', 'customerid']],
    left_on='customer_key', right_on='customer_key', how='left'
)
fact = fact.merge(
    dim_employee[['employee_key', 'employee name']],
    left_on='employee_key', right_on='employee_key', how='left'
)
fact = fact.merge(
    dim_time[['date_key', 'date', 'day', 'month', 'year']],
    left_on='orderdate_key', right_on='date_key', how='left'
)

# ====== ADD STATUS ======
fact['status'] = fact['delivered'].apply(lambda x: "Livré" if x == 1 else "Non livré")

# ====== CALCULATE KPIs ======
total = len(fact)
liv = fact["delivered"].sum()
non = total - liv
taux = round((liv / total * 100), 2)

# ====== CREATE FIGURES ======
# Commandes au fil du temps (darker bars)
time_fig = px.bar(
    fact,
    x="date",
    title="Commandes au fil du temps",
    color_discrete_sequence=['#1f2c56']  
)

# Commandes par employé
emp_fig = px.histogram(fact, x="employee name", title="Commandes par Employé")

# Commandes par client
client_fig = px.histogram(fact, x="customer and company", title="Commandes par Client",width=3000)

# Livré / Non Livré
delivery_fig = px.pie(fact, names="status", title="Livré / Non Livré")

# Répartition des livraisons par région (larger pie)
region_pie = px.pie(
    fact,
    names="region",
    title="Répartition des livraisons par région",
    color="region",
    width=1000,
    height=1000
)

# Nombre total livré vs non livré (narrower bars)
delivery_bar = px.bar(
    fact.groupby("status").size().reset_index(name="count"),
    x="status",
    y="count",
    title="Nombre total livré vs non livré",
    color="status"
)
delivery_bar.update_traces(width=0.4)

# Employés par région
employee_region = px.bar(
    fact.groupby(["region", "employee name"]).size().reset_index(name="count"),
    x="region",
    y="count",
    color="employee name",
    title="Employés par Région"
)

# 3D OLAP Cube
cube = go.Figure()
cube.add_trace(go.Scatter3d(
    x=fact["employee name"],
    y=fact["customer and company"],
    z=fact["date"],
    mode="markers",
    marker=dict(
        size=8,
        color=fact["status"].map({"Livré": "green", "Non livré": "red"}),
        opacity=0.9
    ),
    text=(
        "Employé : " + fact["employee name"] + "<br>" +
        "Client : " + fact["customer and company"] + "<br>" +
        "Date : " + fact["date"].astype(str) + "<br>" +
        "Statut : " + fact["status"]
    ),
    hoverinfo="text"
))
cube.update_layout(scene=dict(
    xaxis_title="Employé",
    yaxis_title="Client",
    zaxis_title="Date"
))

# ====== DASHBOARD ======
app = Dash(__name__)

app.layout = html.Div([
    html.H1("📊 Dashboard of the Business Intelligence Project", style={"textAlign": "center"}),

    # KPIs
    html.Div([
        html.Div(f"Total Commandes : {total}", className="kpi-box"),
        html.Div(f"Livrées : {liv}", className="kpi-box"),
        html.Div(f"Non Livrées : {non}", className="kpi-box"),
        html.Div(f"Taux de Livraison : {taux}%", className="kpi-box"),
    ], style={"display": "flex", "justifyContent": "space-around"}),

    html.Br(),

    # Graphs
    dcc.Graph(figure=time_fig),
    dcc.Graph(figure=emp_fig),
    dcc.Graph(figure=client_fig),
    dcc.Graph(figure=delivery_fig),

    html.H2("🔷 3D Cube Graph (using OLAP) : Employé X Client X Date"),
    dcc.Graph(figure=cube),

    dcc.Graph(figure=region_pie),
    dcc.Graph(figure=delivery_bar),
    dcc.Graph(figure=employee_region),
])

if __name__ == "__main__":
    app.run(debug=True)
