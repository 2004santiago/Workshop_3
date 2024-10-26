import pandas as pd
from json import loads
import joblib
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

# Configurar la conexión a PostgreSQL con psycopg2
import psycopg2
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASS")
db_host = os.getenv("LOCALHOST")
db_port = os.getenv("PORT")
db_database = os.getenv("DB_DATABASE")

# Configuración de la conexión psycopg2
connection = psycopg2.connect(
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port,
    database=db_database
)

# Cargar el modelo guardado
model_file_path = './model/random_forest.pkl'
model = joblib.load(model_file_path)

# Configurar el Kafka Consumer
consumer = KafkaConsumer(
    'happinessPredictions',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='happiness_group',
    value_deserializer=lambda m: loads(m.decode('utf-8')),
    bootstrap_servers=['localhost:9092']
)

# Archivo CSV para almacenar los datos predichos
csv_file = 'predicted_happiness_scores.csv'

# Verificar si el archivo CSV ya existe y establecer encabezado solo si no existe
header = not os.path.exists(csv_file)

# Leer datos del consumidor, predecir y guardar en el CSV
for message in consumer:
    data = message.value
    df = pd.DataFrame([data])

    columns = ['gdp', 'family', 'life_expectancy', 'freedom', 'government_trust', 'generosity']
    
    # Verificar si hay valores nulos
    if df[columns].isnull().values.any():
        print(f"Datos con valores nulos: {df}")
        continue
    
    # Realizar la predicción
    df['predicted_happiness_score'] = model.predict(df[columns])
    
    # Guardar resultados en el archivo CSV
    df[['gdp', 'family', 'life_expectancy', 'freedom', 'government_trust', 'generosity', 'predicted_happiness_score']].to_csv(
        csv_file, mode='a', header=header, index=False
    )
    header = False  # Solo escribe el encabezado la primera vez

    print(f"Predicted Happiness Score: {df['predicted_happiness_score'].values[0]}")
    # time.sleep(1)

# Cerrar el consumidor
consumer.close()
print("Consumer closed.")

# Cargar el archivo CSV a la base de datos PostgreSQL
try:
    with open(csv_file, 'r') as f:
        next(f)  # Saltar la cabecera
        cursor = connection.cursor()
        cursor.copy_expert(
            "COPY happiness_predictions (gdp, family, life_expectancy, freedom, government_trust, generosity, predicted_happiness_score) FROM STDIN WITH CSV",
            f
        )
        connection.commit()
    print("Datos insertados en la base de datos PostgreSQL con éxito.")
except Exception as e:
    print(f"Error al insertar en PostgreSQL: {e}")
finally:
    connection.close()
    print("Conexión a PostgreSQL cerrada.")
