from flask import Flask, render_template
from pyspark.sql import SparkSession
from analize_data import obtener_top_partidos_con_mas_goles, obtener_frecuencia_torneos
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from io import BytesIO
import base64

# Crear la aplicación Flask
app = Flask(__name__)

# Configuración de la sesión Spark
spark = SparkSession.builder \
    .appName("SparkAnalysis") \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Ruta al archivo CSV
data_path = "results.csv"

# Obtener los 10 partidos con más goles
top_partidos = obtener_top_partidos_con_mas_goles(spark, data_path)

# Obtener la frecuencia de torneos
frecuencia_torneos = obtener_frecuencia_torneos(spark, data_path)

# Convertir los resultados de Spark a Pandas DataFrame
pandas_results = top_partidos.toPandas()
pandas_frecuencia_torneos = frecuencia_torneos.toPandas()

@app.route('/')
def index():
    # 6. Top 10 partidos con más goles
    top_partidos_table = pandas_results.to_html(index=False, classes="table table-striped table-bordered")

    # 7. Frecuencia de torneos
    frecuencia_table = pandas_frecuencia_torneos.to_html(index=False, classes="table table-striped table-bordered")

    # Obtener una paleta de colores única para cada barra
    n = len(pandas_frecuencia_torneos)
    color_palette_torneos = plt.cm.Set3(np.linspace(0, 1, n))

    # 8. Diagrama de barras de la frecuencia de torneos
    plt.figure(figsize=(12, 8))
    plt.subplot(2, 1, 1)  # Crear un subplot en la posición superior

    # Ajustar la orientación de las etiquetas en el eje x para evitar solapamientos
    plt.xticks(rotation=45, ha="right", rotation_mode="anchor")

    # Añadir barras con colores diferentes
    bars_torneos = plt.bar(pandas_frecuencia_torneos['tournament'], pandas_frecuencia_torneos['count'],
                          color=color_palette_torneos)

    # Añadir etiquetas en las barras
    for bar_torneo in bars_torneos:
        yval = bar_torneo.get_height()
        plt.text(bar_torneo.get_x() + bar_torneo.get_width()/2, yval, round(yval, 2),
                 ha='center', va='bottom', fontsize=8)

    plt.title('Frecuencia de Torneos')
    plt.xlabel('Torneo')
    plt.ylabel('Frecuencia')
    plt.tight_layout()  # Ajustar el diseño automáticamente para evitar recortes
    buf_torneos = BytesIO()
    plt.savefig(buf_torneos, format='png')
    buf_torneos.seek(0)
    plt.close()
    plot_image_torneos = base64.b64encode(buf_torneos.read()).decode('utf-8')

    # Obtener una paleta de colores única para cada barra en los 10 partidos con más goles
    n_partidos = len(pandas_results)
    color_palette_partidos = plt.cm.Set3(np.linspace(0, 1, n_partidos))

    # 9. Diagrama de barras sobre los 10 partidos con más goles
    plt.figure(figsize=(12, 8))
    plt.subplot(2, 1, 2)  # Crear un subplot en la posición inferior

    # Añadir barras con colores diferentes
    bars_partidos = plt.bar(pandas_results['home_team'] + ' vs ' + pandas_results['away_team'],
                            pandas_results['total_goles'], color=color_palette_partidos)

    # Añadir etiquetas en las barras
    for bar_partido in bars_partidos:
        yval = bar_partido.get_height()
        plt.text(bar_partido.get_x() + bar_partido.get_width()/2, yval, round(yval, 2),
                 ha='center', va='bottom', fontsize=8)

    plt.title('Top 10 Partidos con Más Goles')
    plt.xlabel('Equipos')
    plt.ylabel('Total de Goles')
    plt.xticks(rotation=45, ha="right", rotation_mode="anchor")
    plt.tight_layout()

    buf_partidos = BytesIO()
    plt.savefig(buf_partidos, format='png')
    buf_partidos.seek(0)
    plt.close()
    plot_image_partidos = base64.b64encode(buf_partidos.read()).decode('utf-8')

    return render_template('index.html', top_partidos_table=top_partidos_table,
                           frecuencia_table=frecuencia_table, plot_image_torneos=plot_image_torneos,
                           plot_image_partidos=plot_image_partidos)

@app.route('/streaming')
def streaming():
    # Consultar los últimos tweets desde el DataFrame de streaming
    latest_tweets = spark.sql("SELECT * FROM tweets ORDER BY tweet DESC LIMIT 10")
    tweets_table = latest_tweets.toPandas().to_html(index=False, classes="table table-striped table-bordered")

    return render_template('streaming.html', tweets_table=tweets_table)

if __name__ == '__main__':
    # Ejecutar la aplicación Flask
    app.run(debug=True)
