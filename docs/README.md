# Customer Intelligence Platform (CIP) using PySpark and Databricks

![PySpark](https://img.shields.io/badge/ETL-PySpark-brightgreen)
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-blue)
![Databricks](https://img.shields.io/badge/Platform-Databricks-orange)
![In_Progress](https://img.shields.io/badge/Status-In_Progress-yellow)

> "_Know your customer, predict their journey, and act before they churn._"

> "_This repository is the foundation layer of a real-time Customer Intelligence Platform built using PySpark, Databricks, and Delta Lake. It is designed with engineering precision and business utility in mind._"

# Real-World Scenario

Modern customer analytics requires more than stitched reports.

This project shows how a data engineer can architect a production-grade Customer 360 pipeline to support:

📉 Declining repeat purchases

📦 Increasing return ratios

📬 Rising customer support compliants

❓ Inability to correlate engagement with churn

**The goal:** build a system that connects every customer touchpoint and powers intelligent decisions for product, support, and marketing teams.

# Solution Architecture
```
Raw CSVs                 ──▶ Cleaned Delta Tables              ──▶ Feature-Engineered Tables           ──▶ Customer360 Delta Table
(customers, products,         (null handling, type casting,         (joins, RFM metrics,                   (unified per-customer view
transactions, etc.)           deduplication, normalization)         churn flags, segmentation,              with engineered features,
via PySpark)                                                     →  ⚙️ KPIs: LTV, Repeat Rate,             KPIs, and retention metrics)
                                                                      Support Interactions, CLV, 
                                                                      Avg Order Value, Churn Risk        
                                                                  →  📊 Retention Cohort Matrix
                                                                                                                   │                 │
                                                                                                                   ▼                 ▼
                                                                                         GitHub CI/CD Triggers     Databricks Job Orchestration
                                                                                         (notebook automation,     (optional: task-chained execution,
                                                                                          schema checks,            schedule-based refresh)
                                                                                          export validation)
                                                                                                                   │                 │
                                                                                                                   ▼                 ▼
                                                                                         CSV Exports (.csv)         Power BI Dashboards
                                                                                         (data/final/*.csv)         (from Delta or exported .csv)

```

# Why these Tools?

| **Tech**                       | **Reason for Choosing**                                                               |
| ------------------------------ | ------------------------------------------------------------------------------------- |
| **PySpark**                    | Industry-standard for scalable batch processing. Rich in joins, windowing, and UDFs   |
| **Databricks**                 | Simplifies distributed computing and enables rapid iteration with real-time notebooks |
| **Delta Lake**                 | Enables schema evolution, rollback (time travel), and reliable table auditing         |
| **Airflow**                    | For orchestration, retries, and DAG-based scheduling in production                    |
| **GitHub Actions**             | CI/CD automation for testing, validating, and deploying PySpark pipelines             |
| **Power BI**                   | For storytelling and executive dashboard delivery                                     |

> _Tools were selected for production-readiness, open integration, and ecosystem maturity. Not for novelty._

# Engineering Highlights

⚙️ Scale-Ready: Handles high-volume synthetic data with modular processing

🔄 Idempotent Design: Each step runs independently and can restart from any layer

🧠 Feature Rich Outputs: Includes RFM segmentation, churn risk, engagement metrics

📊 BI-Compatible Tables: Delta format tables designed for immediate dashboard use

🔧 Future-Proof: Easily extendable to streaming or cloud-native sources like Kafka, Event Hub

# Features at a Glance

| Feature                   | Status      |
| ------------------------- | ----------- |
| Scalable PySpark ETL      | ✅ Complete  |
| Customer 360 Output       | ✅ Complete  |
| RFM Segmentation          | ✅ Complete  |
| Churn Flag Logic          | ✅ Complete  |
| Retention Cohort Matrix   | ✅ Complete  |
| Airflow Orchestration     | 🕓 Upcoming |
| CI/CD with GitHub Actions | 🕓 Upcoming |
| Power BI Dashboard        | 🕓 Upcoming |

# What's Coming Next

This repo is being updated incrementally to reflect a real system already built.

- Airflow DAGs for full orchestration and retry logic
- CI/CD pipelines using GitHub Actions
- Power BI dashboards for executives and analysts
- Data quality checks via Great Expectations
- Streaming data readiness (Kafka-compatible architecture)

# Conclusion

This project delivers a working foundation for a real-time Customer Intelligence Platform by showcasing how retail, e-commerce business can unify fragmented customer data - transactions, returns, support, web activity etc. into a single, analytics-ready Customer 360 view.

Built entirely in PySpark on Databricks, it focuses on scalable transformations, feature engineering (like RFM segmentation, churn flags, engagement scores), and delivering output datasets that can directly support revenue analysis, retention tracking, and executive reporting.

By using Delta Lake, the platform ensures data integrity, schema evolution, and time travel, while also being designed to plug into orchestration (Airflow), automation (CI/CD via GitHub Actions), and downstream business intelligence tools like Power BI.

What started as a single unified data layer is being extended into a complete, modular, and production-oriented customer analytics system. This reflects how real companies scale their data platforms to support better decisions, faster.

This repository captures that journey from raw files to insight-ready datasets with engineering precision and real-world pragmatism.

## 👤 About the Author

**Sriram Murali**

Data Engineer | Data Scientist | Scalable Architecture & Applied Analytics

I architect and deliver real-time, high-performance data platforms with a focus on modular design, statistical depth, and business impact. My work spans PySpark, Azure, Delta Lake, and production-grade workflows that integrate advanced analytics, automation, and platform-scale intelligence.

This project is part of a broader suite of pipelines and models I've built - combining deep technical execution with an applied understanding of stakeholder needs, operational systems, and analytical outcomes.

🔗 LinkedIn: https://www.linkedin.com/in/sriram-murali1105/

🔗 Medium: https://medium.com/@sriram1105.m
