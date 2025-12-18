Business Intelligence Project – avec SQL Server & Microsoft access

## 1. Description du projet

Ce projet met en œuvre une **chaîne complète de Business Intelligence (BI)** à partir de deux sources de données hétérogènes :

* Une base de données **SQL Server (Northwind)**
* Des fichiers **Excel exportés depuis Microsoft Access**

L’objectif est de :

1. Extraire les données depuis les sources
2. Les nettoyer et normaliser
3. Les fusionner dans un **Data Warehouse** (schéma en étoile)
4. Calculer des **KPI métiers**
5. Visualiser les résultats via un **dashboard interactif (Dash / Plotly)**

---

## 2. Technologies utilisées

* **Python 3.9+**
* **SQL Server** (Northwind)
* **Pandas**
* **PyODBC**
* **Unidecode**
* **Plotly / Dash**

---



## 3. Prérequis

### 3.1 Environnement Python

Installer les librairies nécessaires :

```bash
pip install pandas pyodbc unidecode plotly dash
```

### 3.2 Base de données SQL Server

* SQL Server local installé
* Base **Northwind** disponible
* Modifier si besoin la chaîne de connexion dans `extract_sql.py` :

```python
Server=localhost\\SQLEXPKHELIL
Database=Northwind
UID=sa
PWD=khelil2059
```

### 3.3 Données Excel

Placer les fichiers suivants dans :

```
data/sources/
```

* `Customers.xlsx`
* `Employees.xlsx`
* `Orders.xlsx`

---

## 4. Exécution du projet (ordre obligatoire)

veuillez ouvrir visual studio code pour éxecuter les scripts

### Étape 1 – Extraction des données Excel

```bash
python scripts/extract_excel.py
```

 Génère des CSV dans `data/raw/excel_sources/`

---

### Étape 2 – Extraction des données SQL Server

```bash
python scripts/extract_sql.py
```

 Génère des CSV dans `data/raw/sql_sources/`

---

### Étape 3 – Transformation & normalisation Excel

```bash
python scripts/transform_excel.py
```

 Nettoyage, normalisation des noms, dates et champs texte
 Sortie : `data/processed/excel_sources/`

---

### Étape 4 – Fusion SQL + Excel

```bash
python scripts/clean_all_sources.py
```

 Union des sources
 Création de fichiers consolidés :

* `customers_all.csv`
* `employees_all.csv`
* `orders_all.csv`

et seront ajoutés au path data/processed/final
---

### Étape 5 – Construction du Data Warehouse

```bash
python scripts/datawarehouse.py
```

 Création du schéma en étoile :

* **dim_customers**
* **dim_employees**
* **dim_temps**
* **fact_orders**

 Déduplication par clés normalisées
 Génération de clés substituts

---

### Étape 6 – Calcul des KPI

```bash
python scripts/kpi_analysis.py
```

#### KPI calculés :

* Total commandes
* Commandes livrées / non livrées
* Taux de livraison
* Analyse par :

  * Pays
  * Employé
  * Mois

 Résultats sauvegardés dans :

```
data/warehouse/kpi_summaries/
```

---

### Étape 7 – Lancement du Dashboard

```bash
python scripts/dashboard.py
```

 Dashboard interactif accessible via navigateur :

```
http://127.0.0.1:8050
```

#### Visualisations disponibles :

* Commandes dans le temps
* Commandes par employé
* Commandes par client
* Livré vs Non livré
* Répartition par région
* Cube OLAP 3D (Employé × Client × Date)
* Employés par région

---

## 5. Résultats attendus

* Data Warehouse cohérent et dédupliqué
* KPI exploitables pour l’aide à la décision
* Dashboard interactif pour analyse métier

---

## 6. Remarques

* Le projet est entièrement **reproductible** en suivant l’ordre des scripts
* Les chemins relatifs sont utilisés (exécution depuis la racine du projet)
* Le dashboard nécessite que `fact_orders.csv` soit généré

---

## 7. Auteur

Projet réalisé dans le cadre d’un **projet Business Intelligence / Data Warehouse**.

---

*Fin du README*
