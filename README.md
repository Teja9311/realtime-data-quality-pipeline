# Real-time Data Quality Monitoring Pipeline

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.0-blue.svg)](https://airflow.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-ready data quality monitoring and anomaly detection pipeline built with **Apache Spark**, **GCP**, **Databricks**, **Python**, and **Apache Airflow**. This project demonstrates modern data engineering practices for real-time data quality assurance.

## 🎯 Project Overview

This pipeline addresses one of 2026's biggest data engineering challenges: **ensuring AI-ready data reliability**. According to recent industry reports, 68% of data practitioners report their data isn't reliable enough for AI workloads.

### Key Features

- **Real-time Data Quality Checks**: Validates data as it streams through the pipeline
- **Anomaly Detection**: Statistical z-score based anomaly detection
- **Multi-Cloud Support**: Works with both GCP Dataproc and Databricks
- **Orchestration**: Fully automated workflow with Apache Airflow
- **Scalable Architecture**: Built on Apache Spark for big data processing
- **Monitoring**: Integrated with Prometheus for metrics

## 🏗️ Architecture

```
┌─────────────┐
│ Data Sources│
└──────┬──────┘
       │
       v
┌──────────────────────────┐
│  GCP Cloud Storage       │
│  (Raw Data Layer)        │
└──────────┬───────────────┘
           │
           v
┌──────────────────────────┐
│   Apache Airflow DAG     │
│ (Orchestration Layer)    │
└──────────┬───────────────┘
           │
     ┌─────┴─────┐
     │           │
     v           v
┌────────┐  ┌──────────┐
│  GCP   │  │Databricks│
│Dataproc│  │ Cluster  │
└────┬───┘  └────┬─────┘
     │           │
     └─────┬─────┘
           │
           v
 ┌─────────────────────┐
 │   PySpark Jobs      │
 │ - Quality Checks    │
 │ - Anomaly Detection │
 │ - Transformations   │
 └─────────┬───────────┘
           │
           v
 ┌──────────────────────┐
 │  Delta Lake / Parquet│
 │ (Quality-Checked Data│
 └──────────────────────┘
```

## 📁 Project Structure

```
realtime-data-quality-pipeline/
├── airflow/
│   └── dags/
│       └── data_quality_dag.py    # Airflow orchestration
├── src/
│   └── spark_jobs/
│       └── data_quality_processor.py  # Spark quality checks
├── config/
│   └── config.yaml                # Configuration file
├── requirements.txt               # Python dependencies
└── README.md
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Apache Spark 3.5.0+
- Apache Airflow 2.8.0+
- GCP Account (for Dataproc) OR Databricks Workspace
- Google Cloud SDK (for GCP deployment)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/Teja9311/realtime-data-quality-pipeline.git
cd realtime-data-quality-pipeline
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure settings**
```bash
cp config/config.yaml config/config.local.yaml
# Edit config.local.yaml with your GCP/Databricks credentials
```

4. **Set up GCP (if using GCP)**
```bash
# Authenticate
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Create GCS bucket
gsutil mb -l us-central1 gs://your-bucket-name
```

5. **Set up Airflow**
```bash
# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Copy DAG to Airflow
cp airflow/dags/data_quality_dag.py ~/airflow/dags/
```

## 💻 Usage

### Running Locally

**Test Spark Job Locally:**
```bash
spark-submit \
  --master local[*] \
  src/spark_jobs/data_quality_processor.py
```

### Running on GCP Dataproc

**Submit job to GCP:**
```bash
gcloud dataproc jobs submit pyspark \
  src/spark_jobs/data_quality_processor.py \
  --cluster=data-quality-cluster \
  --region=us-central1
```

### Running with Airflow

**Start Airflow:**
```bash
# Start webserver
airflow webserver --port 8080

# Start scheduler (in another terminal)
airflow scheduler
```

**Trigger DAG:**
```bash
airflow dags trigger realtime_data_quality_pipeline
```

Access Airflow UI at `http://localhost:8080`

## 📊 Data Quality Checks

The pipeline performs the following quality checks:

1. **Null Value Detection**: Identifies missing values
2. **Range Validation**: Ensures values are within expected ranges (0-1000)
3. **Anomaly Scoring**: Assigns scores based on deviation
4. **Statistical Anomaly Detection**: Z-score based detection (threshold: 3.0)
5. **Schema Validation**: Ensures data conforms to expected structure

## 🛠️ Technologies Used

- **Apache Spark (PySpark)**: Distributed data processing
- **Google Cloud Platform**: Cloud infrastructure and storage
- **Databricks**: Alternative cloud-based Spark platform
- **Apache Airflow**: Workflow orchestration
- **Delta Lake / Parquet**: Data storage formats
- **Prometheus**: Metrics and monitoring
- **Python**: Primary programming language

## 📈 Monitoring

The pipeline includes built-in monitoring:

- **Airflow UI**: Monitor DAG runs and task status
- **Prometheus**: Collect and visualize metrics
- **Email Alerts**: Get notified on failures
- **Logging**: Comprehensive logging at INFO level

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Built for learning modern data engineering practices
- Addresses the 2026 challenge of AI-ready data reliability
- Inspired by industry best practices for data quality

## 📧 Contact

For questions or feedback, please open an issue on GitHub.

---

**Note**: This is a learning project demonstrating real-world data engineering patterns. Always follow your organization's security and compliance requirements when deploying to production.
