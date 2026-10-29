"""
Procesamiento en batch con Spark (PySpark) - Casos de COVID-19 en Colombia
Fuente original: Datos Abiertos Colombia (datos.gov.co), dataset gt2j-8ykr
Uso:  spark-submit batch_analysis.py   (o: python3 batch_analysis.py)
Requisitos: pip install pyspark pandas matplotlib
"""
import os
import urllib.request
from pyspark.sql import SparkSession, functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

URL = "https://www.datos.gov.co/resource/gt2j-8ykr.csv?$limit=100000"
CSV = "data/covid_colombia.csv"
OUT = "resultados"
os.makedirs("data", exist_ok=True)
os.makedirs(OUT, exist_ok=True)

# 1. Cargar el conjunto de datos desde la fuente original
if not os.path.exists(CSV):
    print("Descargando dataset desde datos.gov.co ...")
    urllib.request.urlretrieve(URL, CSV)

spark = SparkSession.builder.appName("BatchCovidColombia").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.csv(CSV, header=True, inferSchema=True)
# Normalizar nombres de columnas (minusculas, sin espacios)
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))
print("Registros cargados:", df.count())
df.printSchema()

# 2. Limpieza y transformacion
dep = "departamento_nom" if "departamento_nom" in df.columns else "departamento"
df = (df.dropDuplicates()
        .withColumn("edad", F.col("edad").cast("int"))
        .withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
        .withColumn(dep, F.upper(F.trim(F.col(dep))))
        .dropna(subset=["edad", "sexo", dep])
        .filter((F.col("edad") >= 0) & (F.col("edad") <= 110)))
df = df.withColumn("grupo_edad", F.when(F.col("edad") < 18, "0-17")
                   .when(F.col("edad") < 40, "18-39")
                   .when(F.col("edad") < 60, "40-59")
                   .otherwise("60+"))
df.cache()
print("Registros tras limpieza:", df.count())

# 3. Analisis exploratorio (EDA)
df.select("edad").describe().show()
por_dep = df.groupBy(dep).count().orderBy(F.desc("count")).limit(10).toPandas()
por_sexo = df.groupBy("sexo").count().toPandas()
por_edad = df.groupBy("grupo_edad").count().orderBy("grupo_edad").toPandas()
por_estado = df.groupBy("estado").count().orderBy(F.desc("count")).toPandas()

# Ejemplo con RDD: conteo por departamento con map/reduceByKey
rdd = df.select(dep).rdd.map(lambda r: (r[0], 1)).reduceByKey(lambda a, b: a + b)
print("Top 5 (RDD):", rdd.takeOrdered(5, key=lambda x: -x[1]))

# 4. Visualizacion
fig, ax = plt.subplots(2, 2, figsize=(13, 9))
ax[0, 0].barh(por_dep[dep][::-1], por_dep["count"][::-1], color="#1f77b4")
ax[0, 0].set_title("Top 10 departamentos por casos")
ax[0, 1].pie(por_sexo["count"], labels=por_sexo["sexo"], autopct="%1.1f%%")
ax[0, 1].set_title("Casos por sexo")
ax[1, 0].bar(por_edad["grupo_edad"], por_edad["count"], color="#ff7f0e")
ax[1, 0].set_title("Casos por grupo de edad")
ax[1, 1].bar(por_estado["estado"].astype(str), por_estado["count"], color="#2ca02c")
ax[1, 1].set_title("Casos por estado")
plt.tight_layout()
plt.savefig(f"{OUT}/eda_covid_colombia.png", dpi=120)
print("Grafico guardado en", f"{OUT}/eda_covid_colombia.png")
spark.stop()
