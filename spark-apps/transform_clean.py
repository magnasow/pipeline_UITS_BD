from pyspark.sql.functions import col, trim

def clean_transform(df):
    df_cleaned = (df
        .dropDuplicates()
        .na.drop(subset=["seriesid", "datayear", "datavalue"])
        .withColumn("seriesid", trim(col("seriesid")))
        .withColumn("datayear", col("datayear").cast("int"))
        .withColumn("datavalue", col("datavalue").cast("float"))
        .withColumn("seriescode", trim(col("seriescode")))
        .withColumn("seriesname", trim(col("seriesname")))
        .withColumn("entityname", trim(col("entityname")))
        .repartition(2)
    )

    return df_cleaned
