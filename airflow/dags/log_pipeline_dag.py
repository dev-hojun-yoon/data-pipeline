from __future__ import annotations

import pendulum
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from airflow.sensors.python import PythonSensor
from pydantic import BaseModel, ValidationError
from pymongo import MongoClient
from datetime import datetime, timedelta

# --- Pydantic Model for Data Validation ---
class LogRecord(BaseModel):
    ip: str
    timestamp: str
    method: str
    url: str
    protocol: str
    status: int
    bytes: int
    current_time: str

# --- Python Callables for DAG Tasks ---

def check_mongo_data(**kwargs):
    """Checks if new data has been inserted into MongoDB since the DAG run started."""
    client = MongoClient(
        ['mongodb-1:27017', 'mongodb-2:27017'],
        username='admin',
        password='password123',
        replicaSet='rs0',
        read_preference='primaryPreferred'
    )
    db = client.test
    collection = db.logs
    
    dag_run_start_time = kwargs['data_interval_start']
    logging.info(f"Checking for new documents in MongoDB since: {dag_run_start_time}")

    # Check for any document with 'current_time' greater than the dag run start time
    # The 'current_time' is added by the spark producer.
    # We need to parse the string time from the record.
    
    # To avoid timezone issues, we check for any document inserted in the last 5 minutes as a simple proxy.
    # A more robust solution would use execution_date or a unique batch ID.
    recent_docs = collection.find({
        "current_time": {
            "$gte": (datetime.utcnow() - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
        }
    })
    
    count = collection.count_documents({
        "current_time": {
            "$gte": (datetime.utcnow() - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
        }
    })

    if count > 0:
        logging.info(f"Found {count} new document(s) in MongoDB. Sensor succeeds.")
        return True
    else:
        logging.info("No new documents found in MongoDB yet. Sensor will retry.")
        return False

def validate_mongo_data(**kwargs):
    """
    Validates the schema and content of the latest data in MongoDB.
    """
    client = MongoClient(
        ['mongodb-1:27017', 'mongodb-2:27017'],
        username='admin',
        password='password123',
        replicaSet='rs0',
        read_preference='primaryPreferred'
    )
    db = client.test
    collection = db.logs

    # Fetch a few recent documents to validate
    recent_docs = collection.find().sort([('_id', -1)]).limit(5)
    
    validation_errors = []
    validated_docs = 0
    for doc in recent_docs:
        try:
            LogRecord.model_validate(doc)
            validated_docs += 1
        except ValidationError as e:
            logging.error(f"Validation failed for document: {doc}")
            logging.error(e)
            validation_errors.append(str(e))

    if validated_docs == 0 and collection.count_documents({}) > 0:
         raise ValueError("No documents could be validated, but documents exist in collection.")

    if validation_errors:
        error_summary = "\n".join(validation_errors)
        raise ValueError(f"Data validation failed for {len(validation_errors)} documents.\n{error_summary}")

    logging.info(f"Successfully validated {validated_docs} recent documents in MongoDB.")


with DAG(
    dag_id="log_data_pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["data-pipeline", "spark", "kafka", "mongo"],
    doc_md="""
    ### Log Data Pipeline

    This DAG orchestrates a data pipeline that processes log files from HDFS, sends them through Kafka,
    and stores them in MongoDB. It includes data validation steps.
    
    **Airflow Connections Needed:**
    - `hdfs_default`: Type `HDFS`, Host `namenode`, Port `9000`.
    - `spark_default`: Type `Spark`, Host `spark-master`, Port `7077`.
    - `mongo_default`: Type `MongoDB`, Host `mongodb-1`, Port `27017`, Login `admin`, Password `password123`.
    """
) as dag:

    check_hdfs_file = HdfsSensor(
        task_id="check_hdfs_file",
        hdfs_conn_id="hdfs_default",
        filepath="/user/hadoop3/test.json",
        poke_interval=10,
        timeout=300,
    )

    run_spark_producer = SparkSubmitOperator(
        task_id="run_spark_producer",
        application="/opt/airflow/project/spark_producer.py",
        application_args=[
            "--json-file", "/user/hadoop3/test.json",
            "--kafka-brokers", "192.168.0.12:9092,192.168.0.13:9093"
        ],
        conn_id="spark_default",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        conf={
            "spark.driver.host": "airflow-worker", # This should be the hostname of the worker
        },
        verbose=True,
    )

    wait_for_data_in_mongo = PythonSensor(
        task_id="wait_for_data_in_mongo",
        python_callable=check_mongo_data,
        poke_interval=30,
        timeout=600,
        mode='reschedule',
    )

    validate_data_in_mongo = PythonOperator(
        task_id="validate_data_in_mongo",
        python_callable=validate_mongo_data,
    )

    check_hdfs_file >> run_spark_producer >> wait_for_data_in_mongo >> validate_data_in_mongo
