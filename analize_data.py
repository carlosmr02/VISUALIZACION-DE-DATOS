from pyspark.sql.functions import desc, col

def obtener_top_partidos_con_mas_goles(spark, data_path):
    # Cargar datos desde el archivo CSV
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Crear una nueva columna con el total de goles
    df = df.withColumn("total_goles", col("home_score") + col("away_score"))

    # Obtener los 10 partidos con más goles
    top_partidos = df.select("date", "home_team", "away_team", "home_score", "away_score", "total_goles") \
                     .orderBy(desc("total_goles")).limit(10)

    return top_partidos

def obtener_frecuencia_torneos(spark, data_path):
    # Cargar datos desde el archivo CSV
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Obtener la frecuencia de torneos
    frecuencia_torneos = df.groupBy("tournament").count().orderBy(desc("count")).limit(5)

    return frecuencia_torneos
