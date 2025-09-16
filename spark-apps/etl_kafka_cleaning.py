from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# 🔹 Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("Nettoyage_UIT_Kafka") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 🔹 Lecture des données brutes depuis Kafka
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "uit_connectivite") \
    .load()

# 🔹 Conversion du champ 'value' en chaîne JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# 🔹 Définition du schéma explicite
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

# 🔹 Parsing JSON → DataFrame structuré
df_parsed = spark.read.schema(schema).json(df_json.rdd.map(lambda row: row.json_str))

# 🔹 Nettoyage des données
df_cleaned = df_parsed \
    .dropDuplicates() \
    .na.drop(subset=["entityName", "dataYear", "dataValue"]) \
    .withColumn("entityName", trim(col("entityName"))) \
    .withColumn("dataYear", col("dataYear").cast("int")) \
    .withColumn("dataValue", col("dataValue").cast("float")) \
    .withColumn("seriesCode", trim(col("seriesCode"))) \
    .withColumn("seriesName", trim(col("seriesName")))

# 🔹 Forcer la distribution sur les workers
df_cleaned = df_cleaned.repartition(4)

# 🔹 Affichage pour contrôle qualité
print("✅ Nombre de lignes nettoyées :", df_cleaned.count())
df_cleaned.select("entityName", "dataYear", "dataValue").show(10)

# 🔹 Export vers PostgreSQL
df_cleaned.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/mydb") \
    .option("dbtable", "indicateurs_nettoyes") \
    .option("user", "admin") \
    .option("password", "admin123") \
    .mode("append") \
    .save()

print("✅ Export vers PostgreSQL terminé")

# 🔹 Export vers MinIO (format Parquet)
df_cleaned.write \
    .mode("overwrite") \
    .parquet("s3a://uit-cleaned/processed/")

print("✅ Export vers MinIO terminé")

# 🔹 Republier vers Kafka (topic uit_connectivite_cleaned)
df_cleaned.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "uit_connectivite_cleaned") \
    .save()

print("✅ Publication vers Kafka terminée")

# 🔹 Fin de session Spark
spark.stop()
