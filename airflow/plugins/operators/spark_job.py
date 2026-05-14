"""
Custom Spark Operators for Airflow.

Wraps Docker-based Spark job submission to keep DAGs clean.
"""

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


class SparkStreamingOperator(DockerOperator):
    """
    Operator that submits a Spark Structured Streaming job using Docker.
    
    This operator simplifies the submission by pre-configuring 
    mounts, environment variables, and the spark-submit command.
    """

    template_fields = DockerOperator.template_fields + ("kafka_topic",)

    def __init__(
            self,
            spark_app_path: str = "src/streaming/spark_runner.py",
            kafka_topic: str = "product_view",
            postgres_host: str = "172.18.0.1",
            project_path: str = "/home/hung/Documents/PycharmProject/glamira_realtime_pipeline",
            **kwargs
    ):
        self.spark_app_path = spark_app_path
        self.kafka_topic = kafka_topic
        self.postgres_host = postgres_host
        self.project_path = project_path
        
        # Default Spark configuration
        command = [
            "bash", "-c",
            "source ~/miniconda3/bin/activate && "
            "(conda env update --file /spark/environment.yml --prune || conda env create --file /spark/environment.yml) && "
            "conda activate pyspark_conda_env && "
            "cd /spark && "
            "export PYTHONPATH=$PYTHONPATH:/spark && "
            "conda pack -f -o /tmp/pyspark_conda_env.tar.gz && "
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 "
            "--conf spark.yarn.dist.archives=/tmp/pyspark_conda_env.tar.gz#environment "
            "--deploy-mode client "
            "--master yarn "
            f"{spark_app_path}"
        ]

        # Default mounts
        mounts = [
            Mount(source=project_path, target="/spark", type="bind"),
            Mount(source="spark_lib", target="/home/spark/.ivy2", type="volume"),
            Mount(source="spark_data", target="/data", type="volume")
        ]

        # Default environment
        env = kwargs.get("environment", {})
        default_env = {
            "HADOOP_CONF_DIR": "/spark/hadoop-conf/",
            "PYSPARK_DRIVER_PYTHON": "/home/spark/miniconda3/envs/pyspark_conda_env/bin/python",
            "PYSPARK_PYTHON": "./environment/bin/python",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
            "KAFKA_TOPIC": kafka_topic,
            "POSTGRES_HOST": postgres_host,
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "spark_streaming_schema",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "hung123",
        }
        default_env.update(env)

        super().__init__(
            image="unigap/spark:3.5",
            command=command,
            mounts=mounts,
            environment=default_env,
            auto_remove="force",
            mount_tmp_dir=False,
            docker_url="unix://var/run/docker.sock",
            network_mode="streaming-network",
            **kwargs
        )
