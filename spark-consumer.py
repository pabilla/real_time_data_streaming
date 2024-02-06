import os
os.environ["SPARK_HOME"] = "/workspaces/real_time_data_streaming/spark-3.2.3-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /workspaces/real_time_data_streaming/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar pyspark-shell'
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as pysqlf
import pyspark.sql.types as pysqlt

if __name__ == "__main__":
    # Initialiser Spark
    spark = (SparkSession
             .builder
             .appName("velib-analysis")
             .master("local[1]")
             .config("spark.sql.shuffle.partitions", 1)
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3")
             .getOrCreate()
             )

    # Lire les données en temps réel depuis Kafka
    kafka_df = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "velib-projet")
                .option("startingOffsets", "earliest")
                .load()
                )

    # Appliquer des traitements sur les données
    schema = pysqlt.StructType([
        pysqlt.StructField("stationCode", pysqlt.StringType()),
        pysqlt.StructField("station_id", pysqlt.StringType()),
        pysqlt.StructField("num_bikes_available", pysqlt.IntegerType()),
        pysqlt.StructField("numBikesAvailable", pysqlt.IntegerType()),
        pysqlt.StructField("num_bikes_available_types",
                           pysqlt.ArrayType(pysqlt.MapType(pysqlt.StringType(), pysqlt.IntegerType()))),
        pysqlt.StructField("num_docks_available", pysqlt.IntegerType()),
        pysqlt.StructField("numDocksAvailable", pysqlt.IntegerType()),
        pysqlt.StructField("is_installed", pysqlt.IntegerType()),
        pysqlt.StructField("is_returning", pysqlt.IntegerType()),
        pysqlt.StructField("is_renting", pysqlt.IntegerType()),
        pysqlt.StructField("last_reported", pysqlt.TimestampType())
    ])

    kafka_df = (kafka_df
                .select(pysqlf.from_json(pysqlf.col("value").cast("string"), schema).alias("value"))
                .withColumn("stationCode", pysqlf.col("value.stationCode"))
                .withColumn("station_id", pysqlf.col("value.station_id"))
                .withColumn("num_bikes_available", pysqlf.col("value.num_bikes_available"))
                .withColumn("numBikesAvailable", pysqlf.col("value.numBikesAvailable"))
                .withColumn("num_bikes_available_types", pysqlf.col("value.num_bikes_available_types"))
                .withColumn("num_docks_available", pysqlf.col("value.num_docks_available"))
                .withColumn("numDocksAvailable", pysqlf.col("value.numDocksAvailable"))
                .withColumn("is_installed", pysqlf.col("value.is_installed"))
                .withColumn("is_returning", pysqlf.col("value.is_returning"))
                .withColumn("is_renting", pysqlf.col("value.is_renting"))
                .withColumn("last_reported", pysqlf.col("value.last_reported"))
                .withColumn("mechanical", pysqlf.col("num_bikes_available_types").getItem(0).getItem("mechanical"))
                .withColumn("ebike", pysqlf.col("num_bikes_available_types").getItem(1).getItem("ebike"))
                )

    # Calculer les indicateurs par code postal
    indicators_df = (kafka_df
                     .groupBy("stationCode")
                     .agg(pysqlf.sum("num_bikes_available").alias("total_bikes_available"),
                          pysqlf.sum("mechanical").alias("total_mechanical_bikes"),
                          pysqlf.sum("ebike").alias("total_electric_bikes"))
                     )

    # Préparer les données pour l'envoi vers Kafka
    col_selections = ["stationCode", "total_bikes_available", "total_mechanical_bikes", "total_electric_bikes"]

    df_out = (indicators_df
              .withColumn("value", pysqlf.to_json(pysqlf.struct(*col_selections)))
              .select("value")
              )

    # Écrire les résultats dans Kafka
    out = (df_out
           .writeStream
           .format("kafka")
           .queryName("velib-projet-final")
           .option("kafka.bootstrap.servers", "localhost:9092")
           .option("topic", "velib-projet-final-data")
           .outputMode("append")
           .option("checkpointLocation", "chk-point-dir")
           .trigger(processingTime="1 minute")  
           .start()
           )

    out.awaitTermination()
