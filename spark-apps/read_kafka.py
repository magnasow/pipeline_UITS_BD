from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def read_from_kafka(spark):
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

    df_raw = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "uit_connectivite")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Harmonisation → tout en minuscules
    df_parsed = (df_parsed
        .withColumnRenamed("seriesID", "seriesid")
        .withColumnRenamed("seriesCode", "seriescode")
        .withColumnRenamed("seriesName", "seriesname")
        .withColumnRenamed("entityID", "entityid")
        .withColumnRenamed("entityIso", "entityiso")
        .withColumnRenamed("entityName", "entityname")
        .withColumnRenamed("dataValue", "datavalue")
        .withColumnRenamed("dataYear", "datayear")
        .withColumnRenamed("dataNote", "datanote")
        .withColumnRenamed("dataSource", "datasource")
        .withColumnRenamed("seriesDescription", "seriesdescription")
    )

    return df_parsed
