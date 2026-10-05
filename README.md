# 🚖 NYC Taxi Real-Time Streaming & Medallion Data Platform

An enterprise-grade, end-to-end Modern Data Stack (MDS) pipeline built to stream, ingest, process, and serve real-time NYC Yellow Taxi telemetry. 

🔗 **[Live Interactive Looker Studio Dashboard](https://datastudio.google.com/reporting/57e1e436-8d59-436b-b6bf-3b208bb519d8)**

---

## 🏛️ Architecture Overview

[ Taxi IoT Simulator ]
│ (High-throughput JSON stream)
▼
[ Apache Kafka (Docker - KRaft) ]
│ (Buffered Event Broker)
▼
[ Async Batching Consumer ] ──► (Micro-batches of 50 trips)
│
▼
[ Google Cloud Storage (Data Lake) ] ──► (gs://shriom-ny-taxi-lake-2026/raw/)
│
▼
[ Databricks Serverless (PySpark + Delta Lake) ]
├── 🥉 Bronze Layer: Raw Ingestion & Audit Tagging
├── 🥈 Silver Layer: Data Cleansing, Quality Filters, Feature Engineering
└── 🥇 Gold Layer: Business KPI Aggregations (Zone-level Revenue & Tips)
│
▼
[ Google BigQuery (Data Warehouse) ] ──► (Serving Layer)
│
▼
[ Looker Studio ] ──► (Real-Time Interactive BI Dashboard)
▲
│ (Automated Schedule & DAG Dependencies)
[ Kestra Orchestration ]

---

## 🛠️ Tech Stack

| Domain | Technology |
| :--- | :--- |
| **Infrastructure as Code** | Terraform |
| **Cloud Provider** | Google Cloud Platform (GCS, BigQuery, IAM) |
| **Event Streaming** | Apache Kafka (KRaft mode), Docker |
| **Processing & Lakehouse** | Databricks Serverless, PySpark, Delta Lake |
| **Data Warehouse** | Google BigQuery |
| **BI & Analytics** | Google Looker Studio |
| **Orchestration** | Kestra (Declarative YAML DAGs) |
| **Languages** | Python 3.12, SQL, HCL |

---
e
## 📂 Repository Structure
├── main.tf # Terraform Cloud Infrastructure configuration
├── docker-compose.yml # Containerized Kafka & Kafka-UI service definitions
├── producer.py # Taxi trip telemetry simulator
├── consumer.py # Asynchronous Kafka batching consumer to GCS
├── databricks_medallion.py # PySpark Bronze -> Silver -> Gold lakehouse logic
├── kestra_flow.yml # Kestra pipeline orchestration DAG
└── README.md # Project documentation & dashboard links

---

## 🚀 How to Run Locally

1. **Deploy Cloud Infra:**
   ```bash
   terraform init
   terraform apply

Start Kafka Broker:

docker compose up -d

Run Ingestion Engine:

python producer.py   # Terminal 1
python consumer.py   # Terminal 2

