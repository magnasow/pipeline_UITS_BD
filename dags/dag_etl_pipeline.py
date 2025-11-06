from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator    # type: ignore
from predict_internet_access import predict_and_insert  
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline_uit",
    default_args=default_args,
    description="Pipeline ETL pour données UIT avec Spark et Airflow",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 29),
    catchup=False,
    tags=["ETL", "Spark", "UIT"],
) as dag:

    # 🚀 Étape 1 : Exécution du job Spark
    spark_etl_cleaning = SparkSubmitOperator(
        task_id="spark_etl_cleaning",
        application="/opt/airflow/spark-apps/etl_kafka_cleaning.py",
        conn_id="spark_default",
        jars=",".join([
            "/opt/airflow/jars/postgresql-42.7.6.jar",
            "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.3.2.jar",
            "/opt/airflow/jars/kafka-clients-3.4.0.jar",
            "/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar",
            "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
            "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
            "/opt/airflow/jars/lz4-java-1.8.0.jar",
            "/opt/airflow/jars/snappy-java-1.1.10.5.jar",
            "/opt/airflow/jars/commons-pool2-2.11.1.jar",
        ]),
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        execution_timeout=timedelta(minutes=30),
    )

    # 🧱 Étape 2 : Rafraîchissement de la vue principale
    refresh_mv_main = PostgresOperator(
        task_id="refresh_materialized_view",
        postgres_conn_id="my_postgres_conn",
        sql="REFRESH MATERIALIZED VIEW CONCURRENTLY mv_indicateurs_analytiques;",
    )

    # 🧱 Étape 3 : Rafraîchissement de la vue de comparaison
    refresh_mv_comparaison = PostgresOperator(
        task_id="refresh_mv_comparaison",
        postgres_conn_id="my_postgres_conn",
        sql="REFRESH MATERIALIZED VIEW CONCURRENTLY comparaison_pipeline_vs_uit;",
    )

    # 🤖 Étape 4 : Prédiction automatique des taux d’accès Internet
    predict__task = PythonOperator(
        task_id="predict_internet_access",
        python_callable=predict_and_insert,
    )

    # 🔗 Dépendances
    #spark_etl_cleaning >> [refresh_mv_main, refresh_mv_comparaison] >> predict_internet_access
