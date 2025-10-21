#  Data Engineering Task — Data Lakehouse (Bronze → Silver → Gold → DWH)

## 📋 Task Overview

This Task demonstrates how to design and build a **Big Data architecture** for an analytical system following the **Medallion (Bronze/Silver/Gold) pattern**, starting from raw CSVs up to a **Data Warehouse** (PostgreSQL DWH) — all orchestrated via **Apache Spark (PySpark)** inside a **Dockerized environment**.

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

