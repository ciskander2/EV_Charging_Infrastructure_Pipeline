# EV Charging Infrastructure Pipeline

## 📌 Overview

This project builds an end-to-end cloud-based data engineering pipeline for analyzing EV charging infrastructure across the United States. The pipeline ingests over 1.6M+ EV charging records from the National Renewable Energy Laboratory (NREL) API, stores raw data in AWS S3, transforms nested JSON into analytics-ready Parquet datasets, and loads processed data into PostgreSQL for large-scale SQL analysis.

The project focuses on identifying gaps in EV charging accessibility, particularly the distribution of fast-charging infrastructure and “charging deserts” that may limit EV adoption.

---

## ⚙️ Pipeline Architecture

```text
NREL API
    ↓
AWS S3 (Raw JSON Data Lake)
    ↓
Transformation Layer (Parquet Conversion)
    ↓
PostgreSQL / Supabase Warehouse
    ↓
```
---

## SQL Analytics & Infrastructure Insights
### 🚀 Key Engineering Features
```text
Scalable ETL pipeline processing 1.6M+ EV charging records
Resume-safe paginated API ingestion with S3 checkpointing
Fault-tolerant retry handling and API rate limiting
AWS S3 cloud data lake architecture
Columnar Parquet transformation pipeline
PostgreSQL analytics warehouse integration
Apache Airflow DAG orchestration
Docker Compose containerized deployment
SQL analytics for EV infrastructure coverage and charger distribution
```
--- 

## 🧠 Problem

EV adoption depends not only on the number of charging stations, but on the availability of reliable fast-charging infrastructure.

## Key Questions Explored
```text
Which cities have the strongest EV charging infrastructure?
Where are “charging deserts” with limited DC fast charging?
How evenly distributed is fast-charging infrastructure across regions?
Which areas may become EV adoption bottlenecks?
```
--- 


## ⚡ Data Ingestion Pipeline
```text
Source

National Renewable Energy Laboratory (NREL) API EV charging station infrastructure dataset.

Extraction

Built Python-based paginated API ingestion workflows implementing:

Retry handling
Rate limiting
Checkpoint-based resumable ingestion
Fault-tolerant pipeline recovery
Storage

Raw API responses stored in:

AWS S3 → JSON

Processed datasets stored in:

AWS S3 → Parquet
🔄 Data Transformation

Nested JSON responses were normalized into analytics-ready tabular datasets using Pandas.

```
--- 
Generated Features
level2_ports
dc_fast_ports
station_count
infrastructure_score
fast_charging_density
Transformation Workflows Included
Missing value handling
Schema normalization
Column standardization
Parquet optimization for analytics workloads
🗄️ Data Warehouse

Processed datasets were loaded into PostgreSQL (Supabase) to support scalable SQL-based analytics.

Example Analytics
Charger availability by city/state
Fast-charging infrastructure density
EV charging network distribution
Infrastructure concentration analysis
Charging desert identification
📊 Key Metrics
Infrastructure Score

Weighted metric representing charging capacity:

infrastructure_score = level2_ports + (dc_fast_ports × 20)
Fast-Charging Density
dc_fast_ports / station_count

Used to identify underserved EV infrastructure regions.

🔍 Key Findings
🏜️ Charging Deserts

Cities including:

Vacaville, CA
Davis, CA
Long Beach, CA

contained multiple charging stations but lacked DC fast chargers, limiting long-distance EV usability.

⚡ Infrastructure Leaders

Los Angeles, CA demonstrated the highest infrastructure capacity:

High Level 2 charger concentration
Strong DC fast charging availability
Broad network coverage
⚠️ Quantity vs Usability Gap

Several cities showed moderate station counts but limited fast-charging capability, highlighting the difference between infrastructure quantity and real-world usability.

📈 Uneven Infrastructure Distribution

DC fast charging infrastructure remains heavily concentrated in a limited number of cities, suggesting unequal EV infrastructure investment across regions.

📊 Pipeline Performance
1.6M+ EV charging records processed
~429 records/sec API ingestion throughput
~1h 54m total orchestrated pipeline runtime
Automated Airflow DAG orchestration
Resume-safe ingestion with S3 checkpoint recovery
🛠️ Tech Stack
Cloud & Infrastructure
AWS S3
Docker Compose
Apache Airflow
Data Engineering
Python
Pandas
Parquet
REST APIs
SQLAlchemy
Database & Analytics
PostgreSQL (Supabase)
SQL
🔐 Security & Configuration

Sensitive credentials and API keys are managed through environment variables and Docker Compose configuration files to avoid hardcoding secrets into source code.

🚀 Future Improvements
Real-time streaming ingestion with Apache Kafka
Distributed processing with Apache Spark / PySpark
Automated data quality validation
CI/CD integration
Monitoring and alerting framework
Interactive analytics dashboard (Power BI / Streamlit)
📄 Resume Summary

Designed and deployed a cloud-based ETL pipeline ingesting 1.6M+ EV charging infrastructure records from a paginated REST API into AWS S3, transforming raw JSON into Parquet datasets and loading analytics-ready data into PostgreSQL using Apache Airflow and Docker Compose to analyze charging infrastructure coverage and EV adoption bottlenecks.
