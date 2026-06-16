# An End-to-End ETL Pipeline for Brazilian E-Commerce Retail Analytics

From raw Kaggle CSVs to a live PostgreSQL database: a complete data engineering pipeline ingesting 99,441 real Brazilian e-commerce orders across 9 relational tables, orchestrated with Apache Airflow, and delivered as a Power BI retail analytics dashboard.

---
## Pipeline Architecture

![Architecture Diagram](https://raw.githubusercontent.com/RenatoMateo/olist-ecommerce-data-pipeline/main/Architecture_diagram.png)

| Stage | Tool | What It Does |
|-------|------|-------------|
| **1. Source** | Kaggle API | Programmatic extraction of 9 CSV files (~100k orders, 2016–2018) |
| **2. Ingest** | Python / Pandas | Load raw CSVs into memory via `pd.read_csv()` |
| **3. Transform** | Pandas | Null handling, deduplication, type casting, FK reconciliation |
| **4. Load** | SQLAlchemy / psycopg2 | Insert clean DataFrames into PostgreSQL via `to_sql()` |
| **5. Validate** | SQL checks | ACID-compliant row count verification, FK integrity checks |
| **6. Orchestrate** | Apache Airflow 2.9.2 | DAG scheduling, task dependency enforcement, failure alerting |
| **7. Visualize** | Power BI Desktop | Live PostgreSQL connection, 3-tab retail analytics dashboard |

---

## Dataset

| Field | Detail |
|-------|--------|
| **Source** | Olist Brazilian E-Commerce Public Dataset |
| **URL** | kaggle.com/datasets/olistbr/brazilian-ecommerce |
| **Volume** | ~99,441 orders across 9 interrelated CSV files |
| **Period** | 2016 to 2018 |
| **Tables** | orders, customers, order_items, products, sellers, payments, reviews, geolocation, category_translation |

---

## Tech Stack

| Category | Tool | Version |
|----------|------|---------|
| Language | Python | 3.11 |
| Data Processing | Pandas, NumPy | Latest |
| Database | PostgreSQL | 18 |
| DB Connection | SQLAlchemy, psycopg2 | Latest |
| Orchestration | Apache Airflow | 2.9.2 |
| Visualization | Power BI Desktop | Latest |
| Environment | Jupyter Notebooks, WSL Ubuntu | — |
| Version Control | Git & GitHub | — |

---

## Schema Design

![ER Diagram](https://raw.githubusercontent.com/RenatoMateo/olist-ecommerce-data-pipeline/main/ERP_diagram.png)

The schema follows **3NF (Third Normal Form)**, each piece of information lives in exactly one table, connected to others through primary and foreign keys. No data is repeated, no column depends on anything other than its table's primary key.

### Table Classification

**Reference tables** are standalone with no FK dependencies, and they are loaded first:

| Table | Primary Key | Notes |
|-------|-------------|-------|
| sellers | seller_id | Merchants listing products |
| customers | customer_id | One unique ID per order (not per person) |
| category_translation | product_category_name | Portuguese to English category mapping |
| products | product_id | FK to category_translation |
| geolocation | — | No PK, the zip prefix maps to multiple coordinates |

**Transaction tables**depend on reference tables, loaded second:

| Table | Primary Key | Foreign Keys |
|-------|-------------|-------------|
| orders | order_id | connect to customers |
| order_items | order_id + order_item_id (composite) | connect to orders, products, sellers |
| payments | order_id + payment_sequential (composite) | connect to orders |
| reviews | review_id | connect to orders |

### Key Design Decisions

**Composite primary keys** — `order_items` and `payments` use two columns together as the PK. Neither column is unique on its own: one order can contain multiple items, and one order can be paid via multiple methods. The combination is always unique.

**Geolocation has no primary key** — the same zip code prefix maps to multiple lat/lng coordinate points across different streets and neighborhoods. No single column or combination qualifies as a unique identifier, so no PK was assigned.

**FK load order enforced** — PostgreSQL checks foreign key references at table creation time. Reference tables must exist before transaction tables can be created. This is not optional — the schema creation order encodes the dependency chain of the business itself.

---

## Data Quality — What We Found and Fixed

All cleaning was conducted in Python before any load in PostgreSQL. 

| Table | Issue | Rows Affected | Decision | Reason |
|-------|-------|--------------|----------|--------|
| orders | Null delivery dates | 2,965 | **Preserved** | Represent genuinely undelivered orders — valid business state |
| products | Missing category names | 610 | **Filled with 'unknown'** | Category must exist in category_translation for FK constraint |
| products | Missing physical dimensions | 2 | **Filled with median** | Median avoids skew from extreme product weights |
| reviews | Missing comment titles | 87,656 | **Filled with 'No title'** | Most customers skip written feedback — expected behavior |
| reviews | Missing comment messages | 58,247 | **Filled with 'No comment'** | Same reasoning |
| reviews | Duplicate review_ids | ~400 | **Keep first, drop rest** | Bug in Olist's review submission system — same ID reused |
| category_translation | Missing categories (pc_gamer, market_place, others) | 3 | **Added programmatically** | Products referenced categories not present in translation table |

### How Missing Categories Were Detected

```python
# Find all category names in products not present in category_translation
missing_categories = products[
    ~products['product_category_name'].isin(category_translation['product_category_name'])
]['product_category_name'].unique()

# Add all missing categories programmatically
missing_rows = pd.DataFrame({
    'product_category_name': missing_categories,
    'product_category_name_english': missing_categories
})
category_translation = pd.concat([category_translation, missing_rows], ignore_index=True)
```

---

## Dev / Test Split & Validation

The dataset was split **75/25** to validate that the pipeline generalises beyond the data it was initially built on. Mirroring how production pipelines are tested before deployment.

### Split Logic

- Split anchored on the **orders table** (the central transaction table)
- All transaction tables filtered to match their respective split's order IDs
- Reference tables (products, sellers, geolocation, category_translation) loaded in full in both splits. 

```python
orders_dev  = orders.sample(frac=0.75, random_state=42)  # 74,581 orders
orders_test = orders.sample(frac=0.25, random_state=42)  # 24,860 orders
```

### Dev Set Row Counts (PostgreSQL)

| Table | Rows |
|-------|------|
| orders | 74,581 |
| customers | 74,581 |
| order_items | 84,439 |
| payments | 77,931 |
| reviews | 73,804 |
| products | 32,951 |
| sellers | 3,095 |
| geolocation | 1,000,163 |
| category_translation | 74 |

### Test Set Validation — FK Integrity Checks

All four referential integrity checks were run in Python against the test set before loading:

| Check | Result | Status |
|-------|--------|--------|
| Order items to valid order_id | 0 broken references | PASS |
| Orders to valid customer_id | 0 broken references | PASS |
| Order items to valid product_id | 0 broken references | PASS |
| Order items to valid seller_id | 0 broken references | PASS |
| Test orders row count | 24,860 (expected ~24,860) | PASS |

```python
# Example check — every order_item must point to a valid order
valid_orders = set(orders_test['order_id'])
invalid_items = order_items_test[~order_items_test['order_id'].isin(valid_orders)]
print(f"Invalid order references: {len(invalid_items)}")  # → 0
```

This confirms the pipeline would handle the test batch cleanly. Replicating the same referential integrity that PostgreSQL enforces on load was verified in Python first.

---

## Orchestration — Airflow DAG

![Airflow DAG](https://raw.githubusercontent.com/RenatoMateo/olist-ecommerce-data-pipeline/main/UI_DAG_olist.JPG)

![Airflow Success](https://raw.githubusercontent.com/RenatoMateo/olist-ecommerce-data-pipeline/main/Total_Sucess_Airflow.JPG)

The pipeline is orchestrated by an Apache Airflow DAG (`olist_pipeline`) with four tasks running in strict sequential order:

```
extract → clean → validate → notify
```

| Task | Type | What It Does |
|------|------|-------------|
| extract | PythonOperator | Confirms all 9 raw CSV files are present in the source folder |
| clean | PythonOperator | Applies full transformation logic — null filling, deduplication, type casting |
| validate | PythonOperator | Connects to PostgreSQL and counts rows in all 9 tables |
| notify | PythonOperator | Logs pipeline completion timestamp |

**Schedule:** `@daily` — runs automatically every 24 hours without manual intervention.

**Retry logic:** 1 automatic retry per task, 5-minute delay before retry.

**Dependency enforcement:** If `extract` fails, `clean` never starts. If `clean` fails, `validate` never starts. The chain is strict — no task runs until its predecessor succeeds.

### WSL Setup Required (Windows Only)

Airflow does not run natively on Windows. The following setup is required:

1. Install WSL (Windows Subsystem for Linux) with Ubuntu
2. Create a Python 3.11 virtual environment inside WSL (system Python 3.14 is incompatible with Airflow 2.9.2)
3. Find the Windows host IP from WSL: `cat /etc/resolv.conf` → nameserver IP
4. Whitelist the WSL subnet in PostgreSQL's `pg_hba.conf`: `172.17.0.0/16 trust`

---

## Analytical Outputs — Power BI Dashboard

Power BI connects directly to the PostgreSQL database via the native PostgreSQL connector. The dashboard reads live from the database — no CSV exports or manual refreshes required.

![Dashboard Overview](https://raw.githubusercontent.com/RenatoMateo/olist-ecommerce-data-pipeline/main/Dashboard_Tab_One_Overview.JPG)

![Dashboard Delivery](https://raw.githubusercontent.com/RenatoMateo/olist-ecommerce-data-pipeline/main/Dashboard_Tab_Two_Delivery.JPG)

![Dashboard Commerce](https://raw.githubusercontent.com/RenatoMateo/olist-ecommerce-data-pipeline/main/Dashboard_Tab_Three_Commerce.JPG)

**Tab 1 — Overview**
Total orders, total revenue, average review score, total sellers. Orders over time (line chart), orders by state, order status breakdown.

**Tab 2 — Delivery Performance**
On-time vs late deliveries, average delivery days, performance by state and month. Delivery status derived via DAX: `IF(delivered_date <= estimated_date, "On Time", "Late")`.

**Tab 3 — Commerce & Reviews**
Revenue by product category, top 10 sellers by revenue, review score distribution, average review score.

### Key Findings

- São Paulo accounts for 40% of all orders — consistent with Brazil's economic geography
- 97% on-time delivery rate across the full dataset
- Average customer review score: **4.09 / 5**
- Review distribution follows a J-curve, with 5-star reviews most common, 1-star is the second most common
- Order items always exceed order count — one order can contain multiple products
- Payments exceed order count — some customers split payments across multiple methods

---

## ⚙️ Key Engineering Challenges

| Challenge | Root Cause | Resolution |
|-----------|-----------|------------|
| SSL certificate errors on pip and Kaggle API | University network SSL proxy intercepting HTTPS | Added `--trusted-host` flags to all pip installs |
| Python 3.14 incompatible with Airflow 2.9.2 | System Python version too new | Created Python 3.11 venv inside WSL |
| WSL could not reach Windows PostgreSQL | `localhost` in WSL points to Linux, not Windows | Found Windows host IP via `/etc/resolv.conf`, whitelisted WSL subnet in `pg_hba.conf` |
| FK violations on product load | Products referenced category names missing from category_translation | Programmatically detected and inserted all missing categories before loading |
| Duplicate review_ids causing PK violations | Bug in Olist's review submission system — same ID reused | `drop_duplicates(subset='review_id', keep='first')` |

---

## 📦 Repository Structure

```
olist-ecommerce-data-pipeline/
│
├── Final_Report.docx                  # Full 8-section project report
│
├── data/
│   ├── raw/                           # Original Kaggle CSVs (unmodified)
│   ├── dev/                           # 75% development split
│   ├── test/                          # 25% test split
│   └── processed/                     # Fully cleaned CSVs (all 9 tables)
│
├── notebooks/
│   └── main_pipeline.ipynb            # End-to-end pipeline notebook (ETL + validation)
│
├── scripts/
│   ├── db_schema.sql                  # PostgreSQL schema — all 9 tables with PKs and FKs
│   └── olist_pipeline.py              # Apache Airflow DAG definition
│
├── diagrams/
│   ├── architecture.png               # End-to-end pipeline architecture
│   └── erd_model.png                  # Entity-relationship diagram
│
├── dashboards/
│   └── dashboard.pbix                 # Power BI dashboard file
│
└── README.md
```

---

## How to Run

### Prerequisites

```bash
pip install pandas numpy sqlalchemy psycopg2-binary kaggle jupyter
```

PostgreSQL 18 must be running locally on port 5432.

### 1. Extract the dataset

```bash
# Set Kaggle credentials
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_api_key

# Download and unzip
kaggle datasets download -d olistbr/brazilian-ecommerce --unzip -p data/raw/
```

### 2. Run the ETL pipeline

Open `notebooks/main_pipeline.ipynb` and run all cells in order:

```
Cell 1  — Install dependencies
Cell 2  — Kaggle API download
Cell 3  — Load all 9 CSVs
Cell 4  — Explore nulls
Cell 5  — Clean nulls + fix duplicates
Cell 5b — Fix missing category_translation entries
Cell 5c — Export clean CSVs to data/processed/
Cell 6  — Split 75/25
Cell 7  — Filter related tables to match the split
Cell 8  — Save dev and test folders
Cell 9  — Connect to PostgreSQL
Cell 10 — Drop all tables (clean slate)
Cell 11 — Create schema
Cell 12 — Load dev data
Cell 13 — Validate row counts
Cell 14 — Test set FK integrity checks
```

### 3. Start Airflow (WSL required on Windows)

Open three Ubuntu terminals:

```bash
# Terminal 1 — Webserver
source ~/airflow-env311/bin/activate
airflow webserver --port 8080

# Terminal 2 — Scheduler
source ~/airflow-env311/bin/activate
airflow scheduler

# Terminal 3 — (optional, for DAG editing)
source ~/airflow-env311/bin/activate
```

Open browser: `http://localhost:8080` → search `olist` → trigger `olist_pipeline`.

### 4. Connect Power BI

Open `dashboards/dashboard.pbix` in Power BI Desktop.

If prompted for credentials:
- **Server:** `localhost`
- **Database:** `olist_ecommerce`
- **Username:** `postgres`
- **Authentication:** Database

---

## References

- Olist. (2018). *Brazilian E-Commerce Public Dataset* [Data set]. Kaggle. https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Apache Software Foundation. (2024). *Apache Airflow documentation* (Version 2.9.2). https://airflow.apache.org/docs/
---

## 👨‍💻 Author

**Renato Silva** — Data Reporting Analyst

[LinkedIn](#) | [GitHub](https://github.com/RenatoMateo/olist-ecommerce-data-pipeline) | 
