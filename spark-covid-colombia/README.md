# Análisis de COVID-19 en Colombia con Spark y Kafka

Proyecto de Big Data (UNAD). Procesamiento batch con PySpark y procesamiento en tiempo real con Kafka + Spark Streaming.

## Problema
Identificar cómo se distribuyeron los casos de COVID-19 en Colombia por departamento, sexo, grupo de edad y estado del paciente.

## Dataset
"Casos positivos de COVID-19 en Colombia" (Instituto Nacional de Salud), Datos Abiertos Colombia:
https://www.datos.gov.co/resource/gt2j-8ykr
El script lo descarga automáticamente desde la fuente original.

## Solución
**Batch (`batch_analysis.py`):**
1. Carga del CSV desde datos.gov.co con Spark.
2. Limpieza: duplicados, nulos, normalización de texto, rango válido de edad.
3. Transformación: creación de grupos de edad.
4. EDA con DataFrames y un ejemplo con RDD (`map` + `reduceByKey`).
5. Visualización con matplotlib en `resultados/eda_covid_colombia.png`.

**Streaming (`streaming/`):**
- `covid_producer.py`: genera casos simulados (departamento, sexo, edad, estado) y los envía al topic `covid_casos` de Kafka.
- `covid_consumer.py`: Spark Streaming que calcula casos y edad promedio por departamento en ventanas de 1 minuto.

## Requisitos
- Python 3.8+
- Java 11
- Internet para descargar el dataset

## Ejecución del batch
```bash
pip install -r requirements.txt
python batch_analysis.py
```
El gráfico queda en `resultados/eda_covid_colombia.png`.

## Ejecución del streaming (Kafka + Spark 3.5)
```bash
# Terminal 1
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
# Terminal 2
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties
# Terminal 3
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic covid_casos
python3 streaming/covid_producer.py
# Terminal 4
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/covid_consumer.py
```

## Autor
[Tu nombre] - Universidad Nacional Abierta y a Distancia
