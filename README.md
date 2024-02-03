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

### 6. Publication des Résultats
Envoyez les résultats du traitement vers le topic Kafka velib-projet-final-data.

