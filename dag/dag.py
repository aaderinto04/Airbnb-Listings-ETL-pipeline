from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import os


@dag(
    dag_id="bookings_spark_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Airbnb listings ETL using Spark",
    tags=["spark", "etl", "airbnb"],
)
def bookings_spark_pipeline():

    # Wait for raw Airbnb CSV to exist
    wait_for_raw_data = FileSensor(
        task_id="wait_for_raw_data",
        filepath="/Users/abdullahaderinto/Documents/Airbnb-Listings-ETL-pipeline/data/listings.csv.gz",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 60,
    )

    # Run Spark ETL job
    run_spark_etl = SparkSubmitOperator(
        task_id="run_spark_etl",
        application="/Users/abdullahaderinto/spark_jobs/airbnb_etl.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[
            "--input", "s3://airbnb-raw/listings/",
            "--output", "s3://airbnb-processed/listings/",
        ],
    )

    # Task dependency
    wait_for_raw_data >> run_spark_etl


dag = bookings_spark_pipeline()
