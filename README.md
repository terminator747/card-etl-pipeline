# Card Transaction ETL Pipeline

A production-style data engineering pipeline for processing card transaction data using PySpark and Apache Airflow.

## Project Overview

This project implements an end-to-end ETL pipeline that reads raw card transaction data, performs data cleaning and transformation using PySpark, and generates curated datasets for analytics.

The pipeline is orchestrated using Apache Airflow and includes automated output validation.

## Architecture

Raw CSV
    ↓
PySpark ETL
    ↓
Data Cleaning & Transformation
    ↓
Curated Parquet Datasets
    ↓
Apache Airflow Orchestration
    ↓
Output Validation
    ↓
Analytics / BI

## Technologies

- Python
- PySpark
- Apache Airflow
- Docker
- Docker Compose
- Pandas
- Pytest
- Git
- GitHub
- SQL
- Power BI

## Pipeline

### 1. Raw Data

Input:

`data/raw/transactions_sample.csv`

### 2. PySpark Processing

The ETL pipeline performs:

- Timestamp processing
- Year and month extraction
- Transaction amount processing
- Fraud flag transformation
- Amount categorization
- Customer-level monthly aggregation
- City-level fraud aggregation

### 3. Curated Data

The pipeline generates:

- `clean_transactions`
- `customer_monthly_spend`
- `fraud_by_city`

### 4. Airflow

The Airflow DAG:

`card_transaction_etl`

executes the PySpark ETL and validates that all expected curated datasets are generated.

## Project Structure

```text
card-etl-pipeline/
├── config/
├── dags/
├── data/
│   ├── raw/
│   └── curated/
├── docker/
├── docs/
├── logs/
├── plugins/
├── spark_jobs/
├── tests/
├── .env
├── .gitignore
├── docker-compose.yaml
├── requirements.txt
└── README.md
