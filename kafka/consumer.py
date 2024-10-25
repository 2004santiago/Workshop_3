import pandas as pd
import json
import joblib
import time
from kafka import KafkaConsumer
from json import dumps
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configurar la conexión a PostgreSQL
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_database = os.getenv("DB_DATABASE")

# Crear cadena de conexión
db_url = f"postgresql://{db_user}:{db_password}@{db_host}/{db_database}"
engine = create_engine(db_url)

# Cargar el modelo guardado
model_file_path = './model/random_forest.pkl'
model = joblib.load(model_file_path)

# Configurar el consumidor de Kafka
consumer = KafkaConsumer(
    'Happinessprediction',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Leer los datos del consumidor de Kafka, predecir y guardar en PostgreSQL
for message in consumer:
    data = message.value
    df = pd.DataFrame([data])  # Convertir el mensaje en un DataFrame
    
    # Seleccionar las columnas necesarias para la predicción
    columns = ['gdp', 'family', 'life_expectancy', 'freedom', 'government_trust']
    
    # Verificar si hay valores nulos
    if df[columns].isnull().values.any():
        print(f"Datos con valores nulos: {df}")
        continue
    
    # Realizar la predicción
    df['predicted_happiness_score'] = model.predict(df[columns])
    
    # Guardar los resultados en la base de datos PostgreSQL
    df[['gdp', 'family', 'life_expectancy', 'freedom', 'government_trust', 'predicted_happiness_score']].to_sql(
        'happiness_predictions', con=engine, if_exists='append', index=False)
    
    print(f"Predicted Happiness Score: {df['predicted_happiness_score'].values[0]}")
    time.sleep(1)  # Opcional, para darle tiempo al procesamiento

# Cerrar el consumidor
consumer.close()
