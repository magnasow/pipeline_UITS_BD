def write_to_postgres(df):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/mydb") \
        .option("dbtable", "indicateurs_nettoyes") \
        .option("user", "admin") \
        .option("password", "admin123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def write_to_minio(df):
    df.write \
        .mode("overwrite") \
        .parquet("s3a://uit-cleaned/processed/")

def publish_to_kafka(df):
    df.selectExpr("to_json(struct(*)) AS value") \
      .write \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("topic", "uit_connectivite_cleaned") \
      .save()
