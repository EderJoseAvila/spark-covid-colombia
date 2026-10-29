import time
import json
import random
from kafka import KafkaProducer

DEPARTAMENTOS = ["BOGOTA", "ANTIOQUIA", "VALLE", "ATLANTICO", "SANTANDER", "CUNDINAMARCA"]

def generate_covid_data():
    return {
        "departamento": random.choice(DEPARTAMENTOS),
        "sexo": random.choice(["F", "M"]),
        "edad": random.randint(0, 95),
        "estado": random.choice(["Leve", "Moderado", "Grave", "Fallecido"])
    }

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    covid_data = generate_covid_data()
    producer.send('covid_casos', value=covid_data)
    print(f"Sent: {covid_data}")
    time.sleep(1)
