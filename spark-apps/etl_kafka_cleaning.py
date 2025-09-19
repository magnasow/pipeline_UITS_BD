from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialisation Spark
spark = SparkSession.builder \
    .appName("Nettoyage_UIT_Kafka") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Lecture Kafka (batch)
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "uit_connectivite") \
    .option("startingOffsets", "earliest") \
    .load()

# Définition schéma
schema = StructType([
    StructField("seriesID", StringType(), True),
    StructField("seriesCode", StringType(), True),
    StructField("seriesName", StringType(), True),
    StructField("entityID", StringType(), True),
    StructField("entityIso", StringType(), True),
    StructField("entityName", StringType(), True),
    StructField("dataValue", FloatType(), True),
    StructField("dataYear", IntegerType(), True),
    StructField("dataNote", StringType(), True),
    StructField("dataSource", StringType(), True),
    StructField("seriesDescription", StringType(), True)
])

# Parsing JSON
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Nettoyage
df_cleaned = df_parsed \
    .dropDuplicates() \
    .na.drop(subset=["entityName", "dataYear", "dataValue"]) \
    .withColumn("entityName", trim(col("entityName"))) \
    .withColumn("dataYear", col("dataYear").cast("int")) \
    .withColumn("dataValue", col("dataValue").cast("float")) \
    .withColumn("seriesCode", trim(col("seriesCode"))) \
    .withColumn("seriesName", trim(col("seriesName"))) \
    .withColumn("seriesID", trim(col("seriesID"))) \
    .repartition(2)

# Contrôle
print("✅ Nombre de lignes nettoyées :", df_cleaned.count())
df_cleaned.select("entityName", "dataYear", "dataValue").show(10)

# Export PostgreSQL
try:
    df_cleaned.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/mydb") \
        .option("dbtable", "indicateurs_nettoyes") \
        .option("user", "admin") \
        .option("password", "admin123") \
        .mode("append") \
        .save()
    print("✅ Export vers PostgreSQL terminé")
except Exception as e:
    print(f"❌ Erreur export PostgreSQL : {e}")


# Export MinIO (Parquet)
df_cleaned.write \
    .mode("overwrite") \
    .parquet("s3a://uit-cleaned/processed/")

print("✅ Export vers MinIO terminé")

# Republier Kafka
df_cleaned.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "uit_connectivite_cleaned") \
    .save()

print("✅ Publication vers Kafka terminée")

# Fin
spark.stop()
