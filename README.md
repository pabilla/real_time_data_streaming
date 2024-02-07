# **Real-time data processing project with Kafka and Spark Streaming**

## **I. Aim of the project**
The aim of this project is to design and implement a real-time data retrieval and processing pipeline, using Apache Kafka for data collection and Apache Spark Streaming for data processing.    
The data concerns the Vélib' stations (bike-sharing system in Paris and the neighbouring metropolitan area).    
This project will filter, process and aggregate the data in real time to produce specific indicators.  

## **II. Detailed Instructions**

### **0. Spark & Kafka setup**
The versions of Spark and Kafka used for the project are available here:  
**SPARK :**
```bash
 wget https://archive.apache.org/dist/spark/spark-3.2.3/spark-3.2.3-bin-hadoop2.7.tgz
 tar -xvf spark-3.2.3-bin-hadoop2.7.tgz
```
**KAFKA :**
```bash
 wget https://archive.apache.org/dist/kafka/2.6.0/kafka_2.12-2.6.0.tgz
 tar -xzf kafka_2.12-2.6.0.tgz
```

Additional configurations are required :  
**JAVA :**
```bash
 sudo apt-get install openjdk-11-jdk-headless
```
**JAR Kafka:**
```bash
 wget https://repo.mavenlibs.com/maven/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.2.3/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar
```

### **1. Creation of Kafka Topics**

Using the command prompt, we will create 2 Kafka topics called **velib-projet** and **velib-projet-final-data**.   
These topics will be used respectively for the initial collection of data and the reception of processed data.    
To do this we will use the following commands:  
```bash
 ./kafka_2.12-2.6.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-projet
```
```bash
 ./kafka_2.12-2.6.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-projet-final-data
```

### **2. Collecte des Données des Stations Vélib'**
A script has been implemented in the `kafka-producer.py` file in the `get_velib_data()` function to collect data from Velib' stations in real time. We will use the publicly available Vélib' Metropolis API to access the data.     
https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json

### **3. Data Filtering**
In this project we only need information about two specific stations, identified by their station numbers: 16107 and 32017.     
We will then filter the data collected to retain only the information we want thanks to the line:     
```python
 all_stations = data["data"]["stations"]
 filtered_stations = [station for station in all_stations if station["stationCode"] in ["16107", "32017"]]
 return filtered_stations
```

### **4. Data publication**
The `velib_producer()` function allows us to create a producer which will write the filtered data to Kafka.    
We then send the filtered data to the Kafka velib-project topic for collection.    
We launch the file in the terminal using  
```bash
 python kafka-producer.py
```
The information on the stations you want is then displayed:
```bash
added: {'stationCode': '16107', 'station_id': 213688169, 'num_bikes_available': 1, 'numBikesAvailable': 1, 'num_bikes_available_types': [{'mechanical': 1}, {'ebike': 0}], 'num_docks_available': 34, 'numDocksAvailable': 34, 'is_installed': 1, 'is_returning': 1, 'is_renting': 1, 'last_reported': 1707263449}
added: {'stationCode': '32017', 'station_id': 2515829865, 'num_bikes_available': 19, 'numBikesAvailable': 19, 'num_bikes_available_types': [{'mechanical': 9}, {'ebike': 10}], 'num_docks_available': 0, 'numDocksAvailable': 0, 'is_installed': 1, 'is_returning': 1, 'is_renting': 1, 'last_reported': 1707263633}
```

### **5. Data processing with Spark Streaming**
A script has been implemented in the `spark-consumer.py` file.     
A Spark consumer has been set up to retrieve data from the velib-project topic.     
We first initialise Spark:   
```python
spark = (SparkSession
             .builder
             .appName("velib-analysis")
             .master("local[1]")
             .config("spark.sql.shuffle.partitions", 1)
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3")
             .getOrCreate()
             )
```
then retrieve the data from the relevant topic    
```python
kafka_df = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "velib-projet")
                .option("startingOffsets", "earliest")
                .load()
                )
```

We want to create a Dataframe. We apply the processing to the data and initialise the Dataframe schema.     
In this consumer, we implement data processing to calculate the following indicators for each postcode of the filtered stations:   
- The total number of bicycles available.   
- The total number of mechanical bikes available.   
- The total number of electric bikes available.   

The data can then be retrieved by applying the following schema:
```python
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
```
Indicators are then calculated
```python
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
```

### **6. Publication of the results**
Finally, we prepare and send the processing results to the Kafka topic velib-projet-final-data.

```python
# Set columns order
col_order = ["timestamp", "postcode", "total_bikes_available", "total_mechanical_bikes", "total_electric_bikes"]
indicators_df_with_postcode = indicators_df_with_postcode.select(*col_order)
    
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
```
It can also be displayed in the console. When you run the program with the command `python spark-consumer.py` you get output like this:

```bash
-------------------------------------------
Batch: 17
-------------------------------------------
+--------------------+--------+---------------------+----------------------+--------------------+
|           timestamp|postcode|total_bikes_available|total_mechanical_bikes|total_electric_bikes|
+--------------------+--------+---------------------+----------------------+--------------------+
|2024-02-07 00:53:...|   93200|                 5700|                  2700|                3000|
|2024-02-07 00:53:...|   75016|                  300|                   300|                   0|
+--------------------+--------+---------------------+----------------------+--------------------+
```
