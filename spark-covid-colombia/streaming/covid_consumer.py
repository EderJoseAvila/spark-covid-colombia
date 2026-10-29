from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("CovidStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("departamento", StringType()),
    StructField("sexo", StringType()),
    StructField("edad", IntegerType()),
    StructField("estado", StringType())
])

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "covid_casos")
      .load())

parsed_df = (df.select(from_json(col("value").cast("string"), schema).alias("data"),
                       col("timestamp"))
               .select("data.*", "timestamp"))

windowed_stats = (parsed_df
    .groupBy(window(col("timestamp"), "1 minute"), "departamento")
    .agg(count("*").alias("casos"), avg("edad").alias("edad_promedio")))

query = (windowed_stats.writeStream
         .outputMode("complete")
         .format("console")
         .option("truncate", "false")
         .start())
query.awaitTermination()
