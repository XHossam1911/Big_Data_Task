#  Data Engineering Task — Data Lakehouse (Bronze → Silver → Gold → DWH)

## 📋 Task Overview

This task implements a Big Data processing pipeline using the Medallion Architecture (Bronze / Silver / Gold layers).
It ingests raw CSV data, processes and transforms it using Apache Spark (PySpark), and loads the curated results into a PostgreSQL Data Warehouse.
The entire solution is containerized and orchestrated within a Docker environment, ensuring reproducibility and scalability across environments.

The pipeline simulates an enterprise-grade approach with support for:
- Incremental loads (CDC)
- Data partitioning and optimization
- Slowly Changing Dimensions (SCD Type-2)
- Fact & Dimension modeling
- Data quality validation
- Automated export and delivery

---

## 🎯 Objectives

1. Design a **Big Data architecture** using the **Medallion Layer Pattern**.
2. Implement scalable **ETL/ELT pipelines** using **PySpark**.
3. Handle both **full** and **incremental (CDC)** loads efficiently.
4. Build a **Data Warehouse model** with **Dimensions**, **Bridge**, and **Fact** tables.
5. Implement **SCD Type-2** for historical tracking.
6. Containerize the full solution with **Docker Compose**.
7. Export clean analytical datasets to **CSV** and **Parquet**.
8. Use **Spark SQL** to transform Silver → Gold and load to PostgreSQL DWH.

---

## 🧩 Architecture Diagram

```text
      +----------------+
      |   Bronze       |
      |  (Raw CSVs)    |
      +--------+-------+
               |
        PySpark ETL
               v
      +----------------+
      |   Silver       |
      |  (Cleaned,     |
      |   CDC handled) |
      +--------+-------+
               |
       Spark SQL (SCD2)
               v
      +----------------+
      |    Gold        |
      |  (DWH Model)   |
      +--------+-------+
               |
        JDBC → PostgreSQL
               v
      +----------------+
      |  Exports (BI)  |
      | CSV / Parquet  |
      +----------------+
```




## 🧰 Tools & Technologies

| Layer | Technology | Purpose |
|--------|-------------|----------|
| **Data Lake / Processing** | **Apache Spark (PySpark)** | Distributed ETL, transformations, SCD |
| **Storage (Bronze/Silver)** | Local Parquet / CSV | Persistent intermediate storage |
| **Data Warehouse (Gold)** | **PostgreSQL** | Dimensional model for analytics |
| **Containerization** | **Docker & Docker Compose** | Isolated, reproducible environment |
| **Orchestration / Utilities** | Bash, Makefile | Automated runs and exports |
| **Version Control** | Git & GitHub | Code versioning and collaboration |

---

## 🧮 Data Model

### Entities
- **Accounts** — banking accounts and statuses  
- **Account Details** — account type and attributes  
- **Person** — individuals linked to one or more accounts  
- **Person Profile** — personal details by date  
- **Person Identification** — IDs or documents linked to people  


## 🧱 Data Warehouse Schema (Gold Layer)

The **Gold layer** implements a **Dimensional Data Model (Star Schema)** designed for analytical queries and historical reporting.

### ⭐ Overview

| Table | Type | Description |
|--------|------|-------------|
| **DIM_ACCOUNT** | Dimension | SCD Type-2 dimension that stores account attributes (status, type, etc.) with historical changes tracked over time. |
| **DIM_PERSON** | Dimension | SCD Type-2 dimension capturing personal details (name, ID) and change history. |
| **BRIDGE_ACCOUNT_PERSON** | Bridge | Many-to-many bridge between accounts and persons, allowing multiple account holders or joint accounts. |
| **DIM_DATE** | Dimension | Calendar dimension containing all distinct transaction or snapshot dates. |
| **FACT_ACCOUNT_SNAPSHOT** | Fact | Periodic snapshot fact table referencing the date and account dimension to track account states across time. |

---

### 🧩 Tables Details 

#### **DIM_ACCOUNT**
| Column | Type | Description |
|---------|------|-------------|
| `account_sk` | bigint | Surrogate key (PK) |
| `acc_no` | string | Business key (AK) |
| `status` | string | Account status (active, closed, etc.) |
| `type` | string | Account type (savings, current, etc.) |
| `effective_start_date` | date | Start date of record validity |
| `effective_end_date` | date | End date of record validity |
| `is_current` | boolean | Flag for current active record |

#### **DIM_PERSON**
| Column | Type | Description |
|---------|------|-------------|
| `person_sk` | bigint | Surrogate key (PK) |
| `person` | string | Natural key (AK) |
| `name` | string | Person’s name |
| `id` | string | Identification number |
| `effective_start_date` | date | Start date of record validity |
| `effective_end_date` | date | End date of record validity |
| `is_current` | boolean | Flag for active record |

#### **BRIDGE_ACCOUNT_PERSON**
| Column | Type | Description |
|---------|------|-------------|
| `acc_no` | string | Foreign key → `DIM_ACCOUNT.acc_no` |
| `person` | string | Foreign key → `DIM_PERSON.person` |

#### **DIM_DATE**
| Column | Type | Description |
|---------|------|-------------|
| `dt` | date | Calendar date (Primary Key) |

#### **FACT_ACCOUNT_SNAPSHOT**
| Column | Type | Description |
|---------|------|-------------|
| `snapshot_date` | date | FK → `DIM_DATE.dt` |
| `acc_no` | string | FK (Business Key) |
| `account_sk` | bigint | FK → `DIM_ACCOUNT.account_sk` |

---

### 🧠 Design Highlights

- **SCD Type-2** implemented in both `DIM_ACCOUNT` and `DIM_PERSON` for full historical traceability.


---

## 🏗️ Medallion Layers

### 🟤 Bronze
- Raw CSV files directly ingested from the source system.
- Located in `data/bronze_samples/`.

### ⚪ Silver
- Data cleaning, type casting, and normalization in PySpark.
- Partitioned by `p_ym` (year-month).
- Incremental **CDC logic** implemented with merge windowing.
- Stored in `data/silver/` as optimized Parquet.

### 🟡 Gold
- Built using **Spark SQL** directly on top of Silver tables.
- Implements:
  - **dim_account** — SCD Type-2 dimension for accounts  
  - **dim_person** — SCD Type-2 dimension for people  
  - **bridge_account_person** — resolves many-to-many relationships  
  - **dim_date** — distinct business date dimension  
  - **fact_account_snapshot** — snapshot fact table joining all dimensions  

### 🧱 Data Warehouse (PostgreSQL)
- Hosted in Docker container (`pgdwh` service).
- Schema: `gold`
- Populated via Spark’s JDBC writer (`mode=overwrite`, `truncate=true`).

---

## 🔁 ETL Flow Summary

| Step | Script | Description |
|------|---------|-------------|
| 1️⃣ | `bronze_to_silver_optimized.py` | Reads raw CSV → cleans → CDC merge → Parquet |
| 2️⃣ | `gold_sql_to_postgres.py` | Runs Spark SQL transformations → loads Gold to Postgres |
| 3️⃣ | `export_gold_to_parquet.py` | Exports DWH tables to single Parquet files |
| 4️⃣ | `scripts/export_gold_csv.sh` | Exports Gold schema to CSV (for Power BI or Tableau) |

---

## 🧪 Key Techniques Used

| Technique | Description |
|------------|--------------|
| **Incremental Merge (CDC)** | Detects changes between current and previous Silver partitions |
| **Window Functions** | Rank latest rows per key (for upserts) |
| **SCD Type-2** | Track historical changes with `effective_start_date`, `effective_end_date`, `is_current` |
| **Partitioning** | Partition Silver data by `p_ym` for performance |
| **Adaptive Query Execution** | Enabled in Spark for runtime optimization |
| **Pushdown JDBC Writes** | Batched, partitioned inserts to Postgres |
| **Docker Networking** | Spark connects to Postgres via internal host name `postgres` |
| **Export Automation** | Automated CSV/Parquet exports via Bash & Spark |

---

## 🧠 Performance Considerations

- **Schema Enforcement**: Explicit column typing prevents Spark inference overhead.
- **Incremental Merge**: Rewrites only changed partitions.
- **Coalesce Writes**: Reduces small files during export.
- **Batch JDBC Writes**: Uses `batchsize=20000` and 16 parallel partitions.
- **Adaptive Execution**: Spark automatically adjusts join strategies.

---
```text
## 🧱 File Structure
.
├── data/
│ ├── bronze_samples/ # raw CSVs
│ ├── silver/ # intermediate cleaned data
│ └── gold/ # not committed, stored in Postgres
├── db/
│ └── init/01_ddl.sql # creates schema gold
├── lib/
│ └── postgresql-42.7.3.jar # JDBC driver
├── exports/
│ ├── gold_csv/ # exported CSVs
├── bronze_to_silver_optimized.py
├── gold_sql_to_postgres.py
├── export_gold_to_parquet.py
├── Dockerfile.spark
├── docker-compose.yml
├── Makefile
└── README.md
```



