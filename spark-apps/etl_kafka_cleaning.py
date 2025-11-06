import logging
from pyspark.sql import SparkSession
from read_kafka import read_from_kafka
from transform_clean import clean_transform
from write_outputs import write_to_postgres, write_to_minio, publish_to_kafka

# -----------------------------
# Configuration logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# Initialisation Spark
# -----------------------------
try:
    spark = SparkSession.builder \
        .appName("Nettoyage_UIT_Kafka") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    logger.info("✅ Spark initialisé avec succès")
except Exception as e:
    logger.error(f"❌ Erreur lors de l'initialisation Spark: {e}")
    exit(1)

# -----------------------------
# Pipeline ETL
# -----------------------------
try:
    # Lecture depuis Kafka
    df_raw = read_from_kafka(spark)
    logger.info("✅ Lecture des données depuis Kafka terminée")
    df_raw.printSchema()

    # Nettoyage et transformation
    df_cleaned = clean_transform(df_raw)
    logger.info("✅ Nettoyage et transformation terminés")
    logger.info(f"Nombre de lignes après nettoyage (limité pour debug): {df_cleaned.limit(5).count()}")
    df_cleaned.show(5)

    # Écriture vers PostgreSQL
    try:
        write_to_postgres(df_cleaned)
        logger.info("✅ Export vers PostgreSQL terminé")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'export vers PostgreSQL: {e}")

    # Écriture vers MinIO
    try:
        write_to_minio(df_cleaned)
        logger.info("✅ Export vers MinIO terminé")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'export vers MinIO: {e}")

    # Publication vers Kafka
    try:
        publish_to_kafka(df_cleaned)
        logger.info("✅ Publication vers Kafka terminée")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la publication vers Kafka: {e}")

except Exception as e:
    logger.error(f"❌ Erreur générale du pipeline: {e}")

finally:
    # Stop Spark
    spark.stop()
    logger.info("✅ Pipeline Spark terminé et Spark arrêté proprement")
