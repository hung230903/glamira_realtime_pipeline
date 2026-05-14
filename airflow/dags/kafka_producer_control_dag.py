"""
Kafka Producer Control DAG.

Manages the lifecycle of the Kafka producer script that forwards data 
from the remote server to the local Kafka cluster.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from docker.types import Mount

# ─── Default Arguments ────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_PATH = "/home/hung/Documents/PycharmProject/glamira_realtime_pipeline"

# ─── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="kafka_producer_control",
    default_args=default_args,
    description="Manage Kafka producer (Remote -> Local forwarder)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kafka", "producer", "ingestion"],
) as dag:

    # Using DockerOperator to run the producer
    run_producer = DockerOperator(
        task_id="run_kafka_producer",
        image="python:3.11-slim",
        auto_remove="force",
        mount_tmp_dir=False,
        command=[
            "bash", "-c",
            "apt-get update && apt-get install -y ca-certificates && "
            "pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org "
            "confluent-kafka python-dotenv && "
            "export PYTHONPATH=$PYTHONPATH:/app && "
            "python -u /app/src/kafka/producer.py"
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="streaming-network",
        working_dir="/app",
        mounts=[
            Mount(source=PROJECT_PATH, target="/app", type="bind"),
        ],
        environment={
            "LOCAL_BOOTSTRAP_SERVERS": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
            "LOCAL_TOPIC": "product_view",
            "LOCAL_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
            "LOCAL_SASL_MECHANISM": "PLAIN",
            "LOCAL_SASL_USERNAME": "admin",
            "LOCAL_SASL_PASSWORD": "@2025",
            "PYTHONUNBUFFERED": "1"
        },
    )

    run_producer
