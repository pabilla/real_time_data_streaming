import json
import time
import requests
from kafka import KafkaProducer

# To do: Créer via l'invite de commande un topic velib-projet et un topic velib-projet-final-data
"""
./kafka_2.12-2.6.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-projet

./kafka_2.12-2.6.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-projet-final-data
"""
# On peut vérifier la liste des Topics avec la commande:
"""
./kafka_2.12-2.6.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
"""

def get_velib_data():
    """
    Get filtered velib data from api
    :return: list of stations information
    """
    response = requests.get('https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json')
    data = json.loads(response.text)

    all_stations = data["data"]["stations"]
    filtered_stations = [station for station in all_stations if station["stationCode"] in ["16107", "32017"]]

    return filtered_stations

def velib_producer():
    """
    Create a producer to write filtered velib data in kafka
    :return:
    """
    producer = KafkaProducer(bootstrap_servers="localhost:9092",
                             value_serializer=lambda x: json.dumps(x).encode('utf-8')
                             )

    while True:
        data = get_velib_data()
        for message in data:
            producer.send("velib-projet", message)
            print("added:", message)
        time.sleep(5)

if __name__ == '__main__':
    velib_producer()