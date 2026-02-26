# PFE Project Plan — Master 2 Data Science & AI

## Recommended project title

**"Plateforme d'Analyse Décisionnelle et Prédictive pour l'Optimisation des Ventes"**

*(Decision and Predictive Analytics Platform for Sales Optimization)*

---

## Why this is an excellent choice

| Criteria | Your project |
|----------|--------------|
| **README subjects** | Covers **Subject 1, 2, 3, 5** (and optionally 4 & 6) |
| **Data Science & AI** | ML (churn, forecasting, CLV, recommendations), EDA, DW |
| **Deliverables** | Data warehouse, pipelines, models, dashboard, API |
| **Business impact** | KPIs, segmentation, predictions, recommendations |

---

## Mapping: your plan ↔ README subjects

| Your component | README subject | Main tasks |
|----------------|----------------|------------|
| **Sales Performance** | **Subject 1** | Sales trends, geography, top sellers, retention, dashboards |
| **Customer Segmentation (RFM) + CLV + Churn** | **Subject 2** | CLV, RFM tiers, loyalty, churn prediction, re-engagement |
| **Product Profitability + Cross-sell** | **Subject 3** | Profitability, co-purchase, bundling, promotion impact |
| **Promotional optimization** | **Subject 5** | Campaign performance, ROI, segment response, recommendations |
| **Market Basket / Recommendations** | **Subject 6** *(add explicitly)* | Association rules, recommendation system (you already have it in ML) |
| **Payment & risk** *(bonus)* | **Subject 4** | Payment trends, late payments, risk score, default prediction |

**Recommendation:**  
- Keep **1, 2, 3, 5** as core.  
- **Explicitly add Subject 6** (Market Basket + Recommendation) — you already planned it; just label it as Subject 6.  
- **Subject 4 = BONUS** — only if you finish the core work before the deadline (payment/risk + optional ML).
---

## Timeline: is 12 weeks enough?

**Yes.** The plan is designed so that the **core** (Subjects 1, 2, 3, 5, 6) fits in 12 weeks. Subject 4 is **bonus** — do it only if you are ahead of schedule.

**If you run short on time**, protect in this order (do not drop earlier items):
1. **Must have:** Data foundation (ETL + DQ) + at least 2 strong ML use cases (e.g. Churn + Sales forecasting) + Dashboard + API.
2. **Should have:** Full EDA for all 5 subjects, all 4 ML use cases (churn, forecasting, CLV, recommendations).
3. **Nice to have:** Subject 4 (payment/risk), extra polish, Docker, advanced visualizations.

---

## Refined 12-week plan (concrete and “perfect” for Master 2)

### Phase 1 — Data foundation (Weeks 1–2)

- **Data warehouse**
  - Star/snowflake schema (you already have `Region`, `Sector`, `Customer`, `Seller`, `Product`, `SalesHeader`, `SalesLine`, `Invoice`).
  - Add a proper **date dimension** (year, quarter, month, week, day, is_weekend, etc.) for time-series and reporting.
- **ETL pipeline**
  - Extract from CSV → staging → dimension/fact tables.
  - Use Python (pandas + SQLAlchemy or similar) or a small orchestration (e.g. Airflow/Dagster) for reproducibility.
- **Data quality**
  - Nulls, duplicates, referential integrity, basic distributions.
  - Document rules and results in a short “Data Quality Report”.

---

### Phase 2 — Exploratory analysis (Weeks 3–4)

- **Subject 1 — Sales performance**
  - Monthly/quarterly/yearly trends, seasonality (e.g. decomposition or simple visual checks).
  - By region/sector, top sellers, retention (new vs returning), basic cohort view if time allows.
- **Subject 2 — Customers**
  - RFM computation and segmentation (e.g. High/Medium/Low value).
  - CLV (historical); optional: simple CLV model for later comparison with ML-based CLV.
- **Subject 3 — Products**
  - Profitability (revenue, margin) per product; note: use `SalesLine` + `SalesHeader`; if no cost data, use margin proxies or state assumptions clearly.
  - Co-purchase patterns (counts, simple association stats) as input for Phase 3.
- **Subject 5 — Promotions**
  - Compare volume/revenue with vs without promotion; by `promotype`/`promovalue`; simple ROI view.

Deliverable: **Jupyter notebook(s)** or scripted EDA with clear sections and a one-page “Key findings” summary.

---

### Phase 3 — Machine learning (Weeks 5–8)

- **Churn prediction (Subject 2)**
  - Define churn (e.g. no purchase in last N months).
  - Features: recency, frequency, monetary, tenure, region, sector, promotion exposure.
  - Model: e.g. Logistic Regression + Tree-based (Random Forest/XGBoost); report precision, recall, AUC; optional SHAP for interpretability.
- **Sales forecasting (Subject 1)**
  - Time series: e.g. Prophet, ARIMA, or simple ML (lag features + date features). Level: global or by region/product if data allows.
  - Metrics: MAE, RMSE, MAPE; visual backtest.
- **Customer Lifetime Value prediction (Subject 2)**
  - Regression (e.g. predict next 6–12 months revenue per customer) or use a simple probabilistic model (e.g. BG/NBD + Gamma-Gamma if you want to go deeper).
- **Market basket & recommendations (Subject 3 + Subject 6)**
  - Association rules: Apriori or FP-Growth; support, confidence, lift; document top rules.
  - Recommendation: collaborative filtering (e.g. matrix factorization) or item-based similarity on baskets; optionally content-based from product attributes.
- **Optional — Payment risk (Subject 4)**
  - Target: late payment or “has delay” binary; features from Invoice + Sales; simple classifier and risk score.

Deliverable: **Notebooks per use case** + **saved models** + short **model cards** (task, features, metric, limitation).

---

### Phase 4 — Dashboard (Weeks 9–10)

- **Tool:** Power BI or Tableau (or Streamlit/Dash if you prefer code-first).
- **Content:**
  - **Subject 1:** Sales trends, geography, sellers, retention.
  - **Subject 2:** RFM/segments, CLV distribution, churn risk list.
  - **Subject 3:** Product profitability, cross-sell matrix or network.
  - **Subject 5:** Promotion performance, ROI.
  - **Subject 6:** Top association rules or “customers who bought X also bought Y”.
- **Interactivity:** filters (date, region, sector), drill-downs, and if possible **one “recommendation” block** (e.g. top 5 products per segment or per customer).

---

### Phase 5 — Deployment & API (Weeks 11–12)

- **API (FastAPI recommended)**
  - Endpoints examples:  
    - `GET /kpis/sales` (aggregates).  
    - `GET /customers/{id}/segment` and `GET /customers/{id}/churn_risk`.  
    - `GET /customers/{id}/recommendations`.  
    - `POST /forecast/sales` (parameters: horizon, level).
  - Use your saved models and DW/DB; keep logic clean (service layer).
- **Simple UI**
  - Minimal web page or Streamlit that calls the API (search customer, see segment, risk, recommendations).
- **Documentation**
  - README: setup, env vars, how to run ETL, train models, run API.  
  - API docs: OpenAPI/Swagger (FastAPI auto-generates).  
  - Short “Methodology” section: schema, ETL, models, metrics.

---

## What would make it “perfect” for a Master 2 jury

1. **Clear link to README subjects** — In the report/slides, add a table: “Subject 1 → Section X, Subject 2 → Section Y”, etc.
2. **Methodology over tools** — Explain why you chose each model (e.g. why RF for churn, why Prophet for series), and how you validated (train/test, temporal split for series).
3. **One strong ML highlight** — e.g. full pipeline for churn (feature engineering → model → SHAP → action: “top 100 at-risk customers”).
4. **Reproducibility** — `requirements.txt`, config files, one-command run for ETL and API; Docker optional but impressive.
5. **Limitations and next steps** — e.g. “no cost data so profitability is revenue-based”; “future: real-time scoring”.

---

## Suggested final subject combination

- **Core (mandatory):** **Subjects 1, 2, 3, 5**  
- **Explicitly add:** **Subject 6** (Market Basket + Recommendation system)  
- **Bonus (if time allows):** **Subject 4** (Payment & risk) as one more ML use case  

This gives you **5 subjects clearly covered** (or 6 if you include Subject 4), with a single coherent narrative: “From raw sales data to a decision and predictive platform for sales optimization.”

---

## Next steps

1. Confirm scope: 1+2+3+5+6 (and optionally 4).  
2. Repo structure and ETL are in place: `etl/`, `notebooks/`, `models/`, `api/`, `dashboard/`, `docs/`.  
3. **Database: SQL Server** — Create the schema with `Schema.sql`, then run the ETL: `python -m etl.run_etl` (see `docs/ETL_SETUP.md`).  
4. Add data quality checks and document in `docs/` (Phase 1 completion).
