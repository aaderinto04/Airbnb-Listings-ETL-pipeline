from airflow.decorators import dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


@dag(
    dag_id="bookings_spark_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Airbnb listings ETL using Spark on EMR",
    tags=["spark", "etl", "airbnb"],
)
def bookings_spark_pipeline():

    wait_for_raw_data = S3KeySensor(
        task_id="wait_for_raw_data",
        bucket_name="airbnb-proj-raw-data",
        bucket_key="*",  # waits for any file in bucket
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=60 * 60,
    )

    run_spark_etl = SparkSubmitOperator(
        task_id="run_spark_etl",
        application="s3://airbnb-spark-app-bucket/spark_app/airbnb_etl.py",
        conn_id="spark_default",   # points to EMR / Spark cluster
        verbose=True,
        application_args=[
            "--input", "s3://airbnb-proj-raw-data/",
            "--output", "s3://processed-airbnb-data/output/",
        ],
    )

    wait_for_raw_data >> run_spark_etl


dag = bookings_spark_pipeline()
