# 🇮🇳 Maharashtra Agricultural Data ETL Pipeline

## 📌 Project Goal
This project establishes a **fully automated, weekly ETL (Extract, Transform, Load) pipeline** for collecting and processing diverse agricultural data specific to the state of **Maharashtra, India**.

The ultimate goal is to produce a unified, **ML-ready dataset** in both **Parquet (for the data lake)** and **Excel (for analysis)** formats, featuring key derived features such as:

- **NPK Ratio**
- **Price-to-Cost Ratio**

---

## 🏗️ Architecture Overview

The pipeline uses a modular approach, leveraging:

- **Python scripts** for orchestration and data ingestion
- **PySpark** for scalable data transformation and feature engineering

### 🔄 Data Flow

#### **Extraction Layer (`etl_extractor.py`):**
- **AGMARKNET:** Simulated web scraping retrieves market arrivals, commodity, and price data.
- **Weather API:** Fetches real-time temperature, humidity, and rainfall data for key districts.
- **Deduplication:** Uses a SQLite database to track already extracted rows, ensuring idempotence.

#### **Transformation Layer (`etl_transformer.py`):**
- PySpark engine loads raw data and static lookups (Soil Health, Crop Metadata)
- Performs data cleansing, joins, standardization, and missing value imputation
- Executes feature engineering to prepare data for ML models

#### **Loading Layer**
- **Data Lake:** Saves processed Parquet files
- **Analytics:** Exports .xlsx format for human analysis

---

## 🚀 Local Setup and Execution

### **1. Prerequisites (Required for PySpark)**

| Software | Requirement |
|---------|-------------|
| Python | 3.9+ |
| Java | JDK/JRE (8 or 11) |
| Apache Spark | Spark 3.x installed |

---

### **2. Environment Variables (Windows Fix)**

To prevent:

> `TypeError: 'JavaPackage' object is not callable`

Set the following:

| Variable | Target Path | Purpose |
|----------|-------------|---------|
| `JAVA_HOME` | path/to/jdk | Required for JVM |
| `SPARK_HOME` | path/to/spark | Points to spark binaries |
| `HADOOP_HOME` | path/to/hadoop | Required for winutils.exe |
| `PATH` | Must include `spark/bin` and `java/bin` | Runtime execution |

---

### **3. Install Dependencies**

```sh
python -m venv AgriETLvenv
.\AgriETLvenv\Scripts\activate
pip install -r requirements.txt
```
### **4. Initialize Static Data**
```
python scripts/init_lookups.py
```

### **5. Execute Local Pipeline**
Extraction:
```
python scripts/etl_extractor.py
```
Transformation:
```
python scripts/etl_transformer.py
```

## ☁️ Production Deployment (Docker & AWS Serverless)
### **1. Dockerize the Application**

Build Image:
```
docker build -t agri-etl-pipeline:latest .
```
### **2. Serverless Orchestration (Recommended)**

| Step        | Component       | Description |
|------------|-----------------|-------------|
| **Entrypoint** | `entrypoint.sh` | Executes extractor → transformer |
| **Compute** | AWS Fargate | Runs containerized PySpark job |
| **Scheduler** | AWS EventBridge | Weekly cron trigger (`cron(0 0 ? * SUN *)`) |
| **Storage** | Amazon S3 | Stores processed Excel + Parquet outputs |


##🏁 Final Output
✔ Automated weekly ingestion
✔ Unified cleaned ML dataset
✔ Stored in both Parquet + Excel format
✔ Ready for analysis and future ML model training

