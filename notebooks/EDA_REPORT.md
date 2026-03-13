# EDA Report — Dislog PFE (Phase 2)

This document summarizes the Exploratory Data Analysis (EDA) performed in **Phase 2** of the Dislog PFE project.  
All analyses are implemented in the notebook `notebooks/02_eda.ipynb` and run on top of the SQL Server **star schema** loaded in Phase 1.

---

## 1. Data Sources

The EDA works on the **data warehouse** (not directly on CSV files). The following tables are used:

- **Dimensions**
  - `DimDate`
  - `DimCustomer`
  - `DimSeller`
  - `DimProduct`
  - `DimPromotion`
  - `DimPaymentMethod`
- **Facts**
  - `FactSales` — ~9,229,070 rows (line-level sales)
  - `FactInvoices` — ~1,437,421 rows (invoices/payments)

The notebook connects via SQLAlchemy using `src.config.get_sqlserver_connection_string()` and pulls the tables with `pd.read_sql`.

---

## 2. Table Profiling

For each table, the notebook performs a **structured profile** using two helper functions:

- `profile_dataframe(name, df)` for **dimension tables**
- `profile_fact_table(name, df, distinct_checks=...)` for **fact tables**

Each profile includes:

- **Shape and types**
  - Number of rows and columns
  - Data types per column
- **Column overview**
  - Null counts and null percentages, with visual cues:
    - Green for 0 nulls
    - Red for columns with nulls
- **Sample rows**
  - First 5 rows in a styled HTML table to quickly inspect realistic values
- **Numeric summary**
  - `describe()` for all numeric columns (min, max, mean, std, quartiles)
- **Key metrics (facts only)**
  - Distinct counts for business keys:
    - `FactSales`: `SaleID`, `CustomerKey`, `SellerKey`, `ProductKey`
    - `FactInvoices`: `InvoiceID`, `CustomerKey`, `SaleID`

### 2.1 Dimensions — Highlights

- `DimCustomer` (~87k rows)
  - One row per customer, enriched with region and sector from DimRegion/DimSector.
  - Missing or `"Unknown"` account names are fixed in ETL as `CLIENT_{AccountID}`, so EDA no longer sees anonymous customers.
- `DimProduct` (~2.3k rows)
  - Catalog of products (itemid, name, alias, brand).
  - No serious quality issues (few nulls in brand and minor whitespace).
- `DimSeller` (~400 rows)
  - Clean seller master; no nulls or duplicates.
- `DimPromotion`, `DimPaymentMethod`
  - Small lookup tables built from distinct promotion types and payment method codes in the source.

### 2.2 Facts — Highlights

- `FactSales` (~9.2M rows)
  - Grain: **one row per sales line** (saleid + itemid).
  - Keys: OrderDateKey, DeliveryDateKey, CustomerKey, SellerKey, ProductKey, PromotionKey.
  - Measures:
    - Line-level: Quantity, UnitPrice, LineBruteAmount, LineDiscountAmount, LineNetAmount, LineTaxAmount, LineTotalAmount.
    - Header-level denormalized: HeaderBruteAmount, HeaderNetAmount, HeaderTaxAmount, HeaderTotalAmount.
- `FactInvoices` (~1.4M rows)
  - Grain: **one row per invoice**.
  - Keys: PaymentDateKey, CustomerKey, PaymentMethodKey, SaleID (degenerate).
  - Measure: PaymentAmount.

---

## 3. Integrity & Date Checks

After profiling, the notebook validates:

- **Date ranges**
  - `OrderDateKey` min/max in `FactSales`
  - `PaymentDateKey` min/max in `FactInvoices`
  - Confirms that dates fall within the expected analysis window (e.g. 2024).

- **Referential integrity**
  - `FactSales.CustomerKey ⊆ DimCustomer.CustomerKey`
  - `FactInvoices.CustomerKey ⊆ DimCustomer.CustomerKey`
  - Any violation would be printed explicitly; with the current ETL, all keys are valid.

These checks confirm that the ETL’s referential integrity enforcement is respected in the warehouse.

---

## 4. RFM Features

The notebook computes **RFM** features at the **customer level**:

- **Recency (R)**  
  Number of days since the last activity (last order or payment) of each customer, relative to a reference date (typically the max order/payment date in the data).

- **Frequency (F)**  
  Number of transactions per customer (e.g. distinct orders or invoices).

- **Monetary (M)**  
  Total amount spent per customer (sum of sales line totals and/or payment amounts).

The pipeline:

1. Aggregates FactSales (and optionally FactInvoices) by `CustomerKey`.
2. Computes last date, count of transactions, and total amounts.
3. Joins the result to `DimCustomer` to keep customer attributes (region, sector, etc.).

### 4.1 Quartiles & Segments

- Each of R, F, M is split into **quartiles** (1–4):
  - For Recency: 4 = clients les plus récents, 1 = les plus anciens.
  - Pour Frequency & Monetary: 4 = très fréquent / très dépensier.
- A composite RFM score and a **segment label** are assigned:
  - **Champions**: R, F, M élevés (clients très actifs et très rentables).
  - **Fidèles**, **Loyalistes potentiels**, **À Risque**, **Dormants**, etc.
- Les règles de segmentation ont été ajustées pour ne pas inverser l’interprétation de la récence (un R “faible” n’est pas un meilleur client).

Ces features RFM seront réutilisées dans la Phase 3 pour:

- Le clustering (K-Means / DBSCAN).
- La prédiction de churn.
- Le calcul/validation de la CLV (Phase 4).

---

## 5. Customer Behavior & Distributions

Au-delà du RFM, le notebook explore:

- **Comportement client**
  - Nombre de commandes et factures par client.
  - Valeur moyenne de commande.
  - Nombre de produits distincts achetés.
  - Fréquence d’achat dans le temps.

- **Analyses temporelles**
  - Chiffre d’affaires par mois.
  - Nombre de commandes par mois.
  - Patterns par jour de la semaine ou par trimestre.

- **Distributions**
  - Histogrammes de R, F, M (asymétriques, avec une longue queue de petits comptes).
  - Boxplots de Monetary par segment RFM pour visualiser les écarts entre segments.

Ces analyses confirment:

- Une base client très déséquilibrée (pareto-like): peu de clients génèrent une grande partie du CA.
- Des différences claires de comportement et de valeur entre segments RFM.

---

## 6. Visualisations Produites

Les principales visualisations dans `02_eda.ipynb` sont:

- **Top clients**
  - Deux bar charts horizontaux côte à côte:
    - Top 10 clients par **Frequency**.
    - Top 10 clients par **Monetary**.

- **Régions / Secteurs**
  - Grille 2×2:
    - Revenue & nombre de commandes par **Region** (barres + lignes).
    - Revenue & nombre de commandes par **Sector**.

- **Top produits**
  - Deux bar charts horizontaux:
    - Top 15 produits par **revenue**.
    - Top 15 produits par **quantité vendue**, avec noms produits tronqués pour lisibilité.

- **Dashboard RFM (2×3)**
  - Ligne du haut: histogrammes Recency, Frequency, Monetary (avec annotations sur l’asymétrie).
  - Ligne du bas: bar chart par segment, heatmap de corrélation R/F/M, boxplot de Monetary par segment.

- **Séries temporelles**
  - 3 panneaux verticaux:
    - Revenue mensuel (courbe + remplissage).
    - Nombre de commandes mensuel (courbe + remplissage).
    - Nombre de lignes de vente mensuelles (bar chart).

Toutes les figures sont configurées pour être directement réutilisables dans les rapports et les slides (titres clairs, labels d’axes, légendes).

---

## 7. Memory & Performance Considerations

- Les faits complets (`FactSales` ~9.2M, `FactInvoices` ~1.4M) sont lourds pour un notebook sur une machine à **12 Go de RAM**.
- Pour éviter les crashes:
  - Le notebook propose un flag (ou une logique équivalente) pour charger **TOP N** lignes au lieu du full dataset pendant l’EDA.
  - Les agrégations RFM sont faites **après agrégation au niveau client**, pas sur les tables de faits brutes à chaque étape.

Recommandations:

- Utiliser l’échantillonnage (TOP N) pour prototyper les analyses et visualisations.
- Réserver les exécutions full facts aux environnements avec suffisamment de RAM ou aux runs “finals” avant rapport.

---

## 8. How to Reproduce the EDA

1. Exécuter l’ETL Phase 1 et vérifier les row counts (`check_db`).
2. Lancer Jupyter:

   ```bash
   pip install -r requirements.txt
   jupyter notebook
   ```

3. Ouvrir `notebooks/02_eda.ipynb`.
4. Vérifier que `.env` est correctement configuré (SQL Server, base `DislogDWH`).
5. Exécuter les cellules dans l’ordre:
   - Connexion / chargement des 8 tables.
   - Profiling dimensionnel et fact.
   - Contrôles de dates et d’intégrité référentielle.
   - Calcul des features RFM et segments.
   - Visualisations (top clients/produits, régions/secteurs, RFM, séries temporelles).

Le notebook constitue la **source de vérité** pour tous les résultats décrits dans ce `EDA_REPORT.md` et dans `reports/phase2_report.tex`.

