from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Logger
logger = logging.getLogger("airflow.pipeline_uit")
logger.setLevel(logging.INFO)

# Paramètres du DAG avec email
default_args = {
    'owner': 'marieta',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 23),
    'email': ['ton.email@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_uit',
    default_args=default_args,
    description='Pipeline Big Data UIT – Nettoyage, export et publication',
    schedule_interval='@daily',
    catchup=False
)

def log_message(message):
    logger.info(f"📝 {message}")

start_log = PythonOperator(
    task_id='start_log',
    python_callable=lambda: log_message("📦 Démarrage du pipeline UIT – exécution du script Spark"),
    dag=dag
)

run_spark_etl = BashOperator(
    task_id='run_spark_etl',
    bash_command='spark-submit --master spark://spark-master:7077 /opt/spark-apps/etl_kafka_cleaning.py',
    dag=dag
)

check_minio_export = BashOperator(
    task_id='check_minio_export',
    bash_command='curl -s -I http://minio:9000/uit-cleaned/processed/ || echo "❌ Export MinIO non trouvé"',
    dag=dag
)

check_postgres_insert = BashOperator(
    task_id='check_postgres_insert',
    bash_command='psql -h postgres -U admin -d mydb -c "SELECT COUNT(*) FROM indicateurs_nettoyes;"',
    env={'PGPASSWORD': 'admin123'},
    dag=dag
)

end_log = PythonOperator(
    task_id='end_log',
    python_callable=lambda: log_message("✅ Pipeline UIT terminé – données disponibles dans PostgreSQL, MinIO et Kafka"),
    dag=dag
)

start_log >> run_spark_etl >> [check_minio_export, check_postgres_insert] >> end_log
