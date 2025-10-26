# Prceparacion del entorno PySpark con Pandas, procesamiento de datos COVID-19 con ETL uso de PySpark y Pandas
import findspark
from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType # Importa tipos de datos
import pandas as pd

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
else:
    print("SparkSession no esta activa.")

data_covid = pd.read_csv('covid-latest.csv')
if data_covid is not None:
    print("Datos cargados correctamente en el DataFrame de Pandas.")
else:
    print("Error al cargar los datos.")