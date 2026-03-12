# PFE Project: CLV & Segmentation + Recommendation System

## Phase 0: Planning

- [x] Create comprehensive implementation plan
- [x] Create cahier de charge (Word + Markdown)
- [x] Get user approval on the plan

## Phase 1: Project Setup & ETL (Month 1)

- [x] Set up project structure (folders, virtual env, dependencies)
- [x] Push to GitHub
- [x] Data profiling — explore raw CSVs to understand data quality
- [x] Design star schema for the data warehouse
- [x] Build ETL pipeline (extract from CSV, transform, load to SQL Server)
- [x] Data quality checks & validation  
  _(Status: completed — ETL fully loaded to SQL Server star schema: FactSales 9,229,070 rows; FactInvoices 1,437,421 rows; DimCustomer 87,653 rows. See ETL README and check_db for live counts.)_

## Phase 2: EDA & Feature Engineering (Month 2)

- [x] Exploratory Data Analysis on all tables
- [x] Build RFM features (Recency, Frequency, Monetary)
- [x] Customer behavior analysis
- [x] Statistical summaries and distributions
- [x] Visualizations (Jupyter notebooks)  
  _(Status: completed — `notebooks/02_eda.ipynb` connects to SQL Server, profiles all 8 tables, computes RFM features, and produces story-telling charts for customers, products, regions/sectors, RFM distributions, and revenue over time.)_

## Phase 3: ML Models — Segmentation & Churn (Month 3)

- [ ] Customer segmentation with K-Means / DBSCAN
- [ ] Optimal cluster selection (Elbow, Silhouette)
- [ ] Churn prediction model (XGBoost / Random Forest)
- [ ] Model evaluation & feature importance
- [ ] Segment profiling and business interpretation  
  _(Next up: will reuse RFM features and star schema; no code started yet.)_

## Phase 4: CLV Prediction & Dashboards (Month 4)

- [ ] CLV calculation and predictive model
- [ ] Interactive dashboards (Power BI or Streamlit)
- [ ] KPI tracking views

## Phase 5: Bonus — Recommendation System (Month 4-5)

- [ ] Market basket analysis (Apriori / FP-Growth)
- [ ] Lightweight collaborative filtering recommendation engine
- [ ] Integration with customer segments

## Phase 6: Report & Defense (Month 5-6)

- [ ] Write PFE report
- [ ] Polish all visualizations
- [ ] Prepare defense slides
- [ ] Rehearse presentation
