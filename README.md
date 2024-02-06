# Projet de Traitement de Données Temps Réel avec Kafka et Spark Streaming

## I. Objectif du Projet
L'objectif de ce projet est de concevoir et implémenter une pipeline de récupération et de traitement de données en temps réel,utilisant Apache Kafka pour la collecte de données et Apache Spark Streaming pour leur traitement.  
Les données concernent les stations Vélib' (système de vélo-partage à Paris et dans la métropole voisine).  
Ce projet permettra de filtrer, traiter, et agréger les données en temps réel pour produire des indicateurs spécifiques.

## II.Instructions Détaillées

### 0. Setup de Spark & Kafka
Les versions de Spark et Kafka utilisées pour le projet sont disponible ici:  
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

Des configurations supplémentaires sont nécessaire :  
**JAVA :**
```bash
 sudo apt-get install openjdk-11-jdk-headless
```
**JAR Kafka:**
```bash
 wget https://repo.mavenlibs.com/maven/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.2.3/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar
```

### 1. Création de Topics Kafka 

Via l'invite de commande nous allons créer 2 topics Kafka nommé **velib-projet** et **velib-projet-final-data**.
Ces topics serviront respectivement à la collecte initiale des données et à la réception des données traitées.  
Pour cela nous allons utiliser les commandes suivantes :
```bash
 ./kafka_2.12-2.6.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-projet
```
```bash
 ./kafka_2.12-2.6.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-projet-final-data
```

### 2. Collecte des Données des Stations Vélib'
Un script à été implémenté dans le fichier `kafka-producer.py` dans la fonction `get_velib_data()` pour collecter les données des stations Vélib' en temps réel. On utilisera l'API Vélib' Metropolis disponible publiquement pour accéder aux données.  
https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json

### 3. Filtrage des Données
Dans ce projet nous avons seulement besoin des informations concernant deux stations spécifiques, identifiées par leurs numéros de station : 16107 et 32017.  
Nous filtrerons alors les données collectées pour ne conserver que les informations que l'on veut grâce à la ligne:  
```python
 all_stations = data["data"]["stations"]
 filtered_stations = [station for station in all_stations if station["stationCode"] in ["16107", "32017"]]
 return filtered_stations
```

### 4. Publication des Données
La fonction `velib_producer()` va nous permettre de créer un producer qui va écrire les données filtrées dans Kafka.  
Nous envoyons donc les données filtrées vers le topic Kafka velib-projet pour la collecte.  
On lance le fichier dans le terminal grâce à  
```bash
 python kafka-producer.py
```

### 5. Traitement des Données avec Spark Streaming
Un script à été implémenté dans le fichier `spark-consumer.py`  
Un consumer Spark a été mis en place pour récupérer les données du topic velib-projet.  
On initialise tout d'abord Spark:  
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
et on vient récupérer les données dans le topic voulu

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

On veut créer un Dataframe. On applique le traitement sur les données et on initialise le schéma du Dataframe.  
Dans ce consumer, on implémente le traitement des données pour calculer les indicateurs suivants pour chaque code postal des stations filtrées :
- Le nombre total de vélos disponibles.
- Le nombre total de vélos mécaniques disponibles.
- Le nombre total de vélos électriques disponibles.  

On retrouve alors les données lorsqu'on applique le schéma suivant:
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

```
On calcule les indicateurs par code postal:
```python
indicators_df = (kafka_df
                 .groupBy("stationCode")
                 .agg(pysqlf.sum("num_bikes_available").alias("total_bikes_available"),
                      pysqlf.sum("mechanical").alias("total_mechanical_bikes"),
                      pysqlf.sum("ebike").alias("total_electric_bikes"))
                 )
```


### 6. Publication des Résultats
Enfin, on prépare et on envoie les résultats du traitement vers le topic Kafka velib-projet-final-data.

```python
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
```

Lorsqu'on lance le programme via la commande `python spark-consumer.py` on obtient une sortie du style:
```bash
Project [value#175]
+- Project [stationCode#23, total_bikes_available#166L, total_mechanical_bikes#168L, total_electric_bikes#170L, to_json(struct(stationCode, stationCode#23, total_bikes_available, total_bikes_available#166L, total_mechanical_bikes, total_mechanical_bikes#168L, total_electric_bikes, total_electric_bikes#170L), Some(Etc/UTC)) AS value#175]
   +- Aggregate [stationCode#23], [stationCode#23, sum(num_bikes_available#32) AS total_bikes_available#166L, sum(mechanical#122) AS total_mechanical_bikes#168L, sum(ebike#136) AS total_electric_bikes#170L]
      ...
```
Dans cette partie de la sortie, on voit l'opération de projection et d'agrégation qui calcule les indicateurs par code postal.    
Plus précisément :

- **stationCode#23** est le code postal de la station.
- **total_bikes_available#166L** représente le nombre total de vélos disponibles par code postal.
- **total_mechanical_bikes#168L** représente le nombre total de vélos mécaniques disponibles par code postal.
- **total_electric_bikes#170L** représente le nombre total de vélos électriques disponibles par code postal.