# Airbnb-Listings-ETL-pipeline


<img width="1102" height="769" alt="image" src="https://github.com/user-attachments/assets/643a9064-a1cb-49dc-bbd4-d02ad886d2ae" />

# Airbnb Listings ETL Pipeline

An end-to-end data engineering project that ingests Airbnb listing data from Amazon S3, transforms it with PySpark, and orchestrates the workflow with Apache Airflow.

## Project Overview

This project automates a daily ETL pipeline for Airbnb listings data. Airflow monitors a raw S3 bucket, triggers a Spark job when data is available, and writes analytics-ready parquet output to a processed S3 location.

The pipeline focuses on:
- Cleaning raw listing prices into numeric values
- Computing host-level aggregated metrics
- Producing a denormalized dataset for downstream analytics

## Pipeline Flow

1. Airflow waits for incoming raw files in `s3://airbnb-proj-raw-data/`
2. Airflow submits a Spark job to a configured Spark/EMR cluster
3. Spark reads `listings.csv.gz`, infers schema, and applies cleaning
4. Spark computes host statistics:
   - total listings per host
   - average price
   - average reviews
   - average minimum nights
5. Spark joins host metrics back to listing-level records
6. Final dataset is written to `s3://processed-airbnb-data/output/` as parquet

## Repository Structure

- `dag/dag.py` - Airflow DAG definition (`bookings_spark_pipeline`)
- `spark_app/spark.py` - PySpark transformation and output logic
- `data/listings.csv.gz` - Sample/source listings data

## Tech Stack

- Apache Airflow (workflow orchestration)
- Apache Spark / PySpark (distributed transformation)
- Amazon S3 (raw and processed data storage)
- Amazon EMR or Spark cluster (job execution target)

## Airflow DAG Details

The DAG includes two core tasks:
- `wait_for_raw_data`: `S3KeySensor` that polls for files in the raw bucket
- `run_spark_etl`: `SparkSubmitOperator` that runs the Spark application with input/output S3 arguments

Schedule:
- Runs `@daily`
- `catchup=False`

## Spark Job Details

The Spark job performs:
- CSV read with permissive parsing and multiline support
- Price normalization from currency string to float (`price_num`)
- Host-level aggregations using `groupBy` and aggregate functions
- Left join of host aggregates back to the cleaned listings dataset
- Overwrite write mode to parquet output

## How To Run

### Prerequisites

- Python environment with Airflow and PySpark dependencies
- AWS credentials configured for Airflow/Spark runtime
- Airflow connection `aws_default`
- Airflow connection `spark_default` pointing to your Spark cluster
- S3 buckets:
  - `airbnb-proj-raw-data`
  - `processed-airbnb-data`
  - Spark app location bucket for deployment artifact

### Run Locally (Spark Script)

If running outside Airflow for testing:

```bash
python spark_app/spark.py
```

### Run Through Airflow

1. Place raw data in the configured raw S3 bucket
2. Enable and trigger `bookings_spark_pipeline` in Airflow
3. Monitor task logs for sensor detection and Spark submission
4. Validate parquet output in the processed S3 path

## Output

The resulting parquet dataset contains listing-level records enriched with host summary metrics, making it suitable for BI dashboards, price analysis, and host behavior modeling.

