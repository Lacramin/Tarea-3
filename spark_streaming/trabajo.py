from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, percentile_approx, when, avg, lower
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

sesion_spark = SparkSession.builder \
    .appName("ProcesamientoMetricas") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

sesion_spark.sparkContext.setLogLevel("WARN")

esquema_datos = StructType([
    StructField("marca_tiempo", TimestampType(), True),
    StructField("tipo_consulta", StringType(), True),
    StructField("latencia", DoubleType(), True),
    StructField("resultado_cache", StringType(), True),
    StructField("cantidad_reintentos", IntegerType(), True),
    StructField("estado_final", StringType(), True)
])

flujo_entrada = sesion_spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "metrics-topic") \
    .option("startingOffsets", "earliest") \
    .load()

datos_formateados = flujo_entrada.select(
    from_json(col("value").cast("string"), esquema_datos).alias("datos")
).select("datos.*")

datos_agrupados = datos_formateados \
    .withWatermark("marca_tiempo", "1 minute") \
    .groupBy(window(col("marca_tiempo"), "1 minute")) \
    .agg(
        count("*").alias("rendimiento_minuto"),
        (percentile_approx("latencia", 0.5) * 1000).cast("double").alias("latencia_pcincuenta"),
        (percentile_approx("latencia", 0.95) * 1000).cast("double").alias("latencia_pnoventaycinco"),
        avg(when(lower(col("resultado_cache")) == "hit", 1.0).otherwise(0.0)).alias("tasa_aciertos"),
        avg(when(col("cantidad_reintentos") > 0, 1.0).otherwise(0.0)).alias("tasa_reintentos")
    )

def escribir_lote_es(dataframe_lote, id_lote):
    df_final = dataframe_lote \
        .withColumn("id_documento", col("window.start").cast("string")) \
        .withColumn("@timestamp", col("window.start").cast("timestamp"))
    
    df_final.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("append") \
        .option("es.resource", "metricas_sistema") \
        .option("es.mapping.id", "id_documento") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .save()

flujo_salida = datos_agrupados \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(escribir_lote_es) \
    .option("checkpointLocation", "/tmp/puntos_control_spark_final") \
    .start()

flujo_salida.awaitTermination()
