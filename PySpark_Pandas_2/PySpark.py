# Prceparacion del entorno PySpark con Pandas, procesamiento de datos COVID-19 con ETL uso de PySpark y Pandas
import findspark
import PySpark
from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType # Importa tipos de datos
import pandas as pd

def inicio_spark():
    ############# INICIO SPARK SESSION #############
    # Initialize findspark
    findspark.init()
    # Initialize a Spark Session
    spark = SparkSession \
        .builder \
        .appName("COVID-19 Data Processing") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    # Check if the Spark Session is active
    if 'spark' in locals() and isinstance(spark, SparkSession):
        print("SparkSession esta activa.")
        return spark
    else:
        print("SparkSession no esta activa.")
        return None

def extraccion():
    # Leer el archivo CSV en un DataFrame de Pandas
    data_covid = pd.read_csv('PySpark_Pandas_1/covid-latest.csv')
    if data_covid is not None:
        print("Datos cargados correctamente en el DataFrame de Pandas. Tamaño:", len(data_covid))
        return data_covid
    else:
        print("Error al cargar los datos. DataFrame de Pandas no cargado.")
        return None

def transformacion(spark, data_covid):
    # Convertir el DataFrame de Pandas a un DataFrame de Spark
    df_spark = spark.createDataFrame(data_covid)
    if df_spark is not None:
        print("Datos convertidos correctamente a DataFrame de Spark. Número de filas:", df_spark.count())
        
        return df_spark
    else:
        print("Error al convertir los datos. DataFrame de Spark no creado.")
        return None

### Inicializacion y ejecucion de las funciones
spark = inicio_spark()

try:
    spark_version = spark.version
    print(f"Versión de Spark: {spark_version}")
    print("--------------------------------")
    print("Iniciando proceso ETL con PySpark y Pandas...\n--------------------------------\n--------------------------------")
except AttributeError:
    print("No se pudo obtener la versión de Spark.")

try:
    if spark is not None:
        print("SparkSession iniciada correctamente.\n--------------------------------")
        data_covid = extraccion()
        
        if data_covid is not None:
            print("Datos extraidos correctamente.\n--------------------------------")
            df_spark = transformacion(spark, data_covid)
            
            if df_spark is not None:
                print("Transformacion completada correctamente.\n--------------------------------")
            else:
                print("Fallo en la transformacion de datos.\n--------------------------------")
        else:
            print("Fallo en la extraccion de datos.\n--------------------------------")
    else:
        print("Fallo en la inicializacion de SparkSession.\n--------------------------------")
except Exception as e:
    print("Ocurrio un error durante la ejecucion:", str(e))