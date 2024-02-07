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
    # Initialising Spark
    spark = (SparkSession
             .builder
             .appName("velib-analysis")
             .master("local[1]")
             .config("spark.sql.shuffle.partitions", 1)
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3")
             .getOrCreate()
             )

    # Get data from Kafka in real time
    kafka_df = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "velib-projet")
                .option("startingOffsets", "earliest")
                .load()
                )

    # Apply data processing
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

    # Get data from stations_information.csv
    stations_info_df = (spark
                        .read
                        .csv("stations_information.csv", header=True, inferSchema=True)
                        )

    # Calculate indicators by postcode
    indicators_df = (kafka_df
                     .groupBy("station_id")
                     .agg(pysqlf.sum("num_bikes_available").alias("total_bikes_available"),
                          pysqlf.sum("mechanical").alias("total_mechanical_bikes"),
                          pysqlf.sum("ebike").alias("total_electric_bikes"))
                     )

    # Join DataFrames using station_id as key
    indicators_df_with_postcode = (indicators_df
                                   .join(stations_info_df.select("station_id", "postcode"), ["station_id"], "left")
                                   .drop("station_id")
                                   )

    # Add "timestamp" column
    indicators_df_with_postcode = indicators_df_with_postcode.withColumn("timestamp", pysqlf.current_timestamp())

    # Set columns order
    col_order = ["timestamp", "postcode", "total_bikes_available", "total_mechanical_bikes", "total_electric_bikes"]
    indicators_df_with_postcode = indicators_df_with_postcode.select(*col_order)
    
    # Display results in Python output
    query_console = (indicators_df_with_postcode
            .writeStream
            .outputMode("complete")
            .format("console")
            .start()
            )

    # Send results to the Kafka topic
    query_kafka = (indicators_df_with_postcode
            .writeStream
            .outputMode("complete")
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "velib-projet-final-data")
            .option("checkpointLocation", "/tmp/checkpoint")
            .start()
            )

    query_console.awaitTermination()
