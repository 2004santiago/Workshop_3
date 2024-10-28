import pandas as pd
from json import loads
import joblib
import os
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
import time

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASS")
db_host = os.getenv("LOCALHOST")
db_port = os.getenv("PORT")
db_database = os.getenv("DB_NAME")

connection = psycopg2.connect(
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port,
    database=db_database
)
cursor = connection.cursor()

# Crear la tabla si no existe
create_table_query = """
CREATE TABLE IF NOT EXISTS happiness_predictions (
    gdp NUMERIC,
    family NUMERIC,
    life_expectancy NUMERIC,
    freedom NUMERIC,
    government_trust NUMERIC,
    generosity NUMERIC,
    predicted_happiness_score NUMERIC
);
"""
cursor.execute(create_table_query)
connection.commit()

model_file_path = './model/random_forest.pkl'
model = joblib.load(model_file_path)

consumer = KafkaConsumer(
    'happinessPredictions',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='happiness_group',
    value_deserializer=lambda m: loads(m.decode('utf-8')),
    bootstrap_servers=['localhost:9092'],
    consumer_timeout_ms=10000
)


# consumer = pd.read_csv("./clean-data/modelDataset.csv")  

batch_size = 20
batch_data = pd.DataFrame()

for message in consumer:
    data = message.value
    df = pd.DataFrame([data])

    columns = ['gdp', 'family', 'life_expectancy', 'freedom', 'government_trust', 'generosity']
    
    if df[columns].isnull().values.any():
        print(f"Datos con valores nulos: {df}")
        continue
    
    df['predicted_happiness_score'] = model.predict(df[columns])
    batch_data = pd.concat([batch_data, df], ignore_index=True)

    print(f"Predicted Happiness Score: {df['predicted_happiness_score'].values[0]}")
    
    if len(batch_data) >= batch_size:
        try:
            rows = [tuple(map(float, row)) for row in batch_data[['gdp', 'family', 'life_expectancy', 'freedom', 'government_trust', 'generosity', 'predicted_happiness_score']].values]
            insert_query = """
                INSERT INTO happiness_predictions (gdp, family, life_expectancy, freedom, government_trust, generosity, predicted_happiness_score) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, rows)
            connection.commit()
            print(f"Lote de {batch_size} filas insertado en la base de datos PostgreSQL con éxito.")

        except Exception as e:
            print(f"Error al insertar en PostgreSQL: {e}")
            connection.rollback()
        
        batch_data = pd.DataFrame()

if not batch_data.empty:
    try:
        rows = [tuple(map(float, row)) for row in batch_data[['gdp', 'family', 'life_expectancy', 'freedom', 'government_trust', 'generosity', 'predicted_happiness_score']].values]
        insert_query = """
            INSERT INTO happiness_predictions (gdp, family, life_expectancy, freedom, government_trust, generosity, predicted_happiness_score) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, rows)
        connection.commit()
        print(f"Datos restantes ({len(batch_data)} filas) insertados en la base de datos PostgreSQL con éxito.")
    except Exception as e:
        print(f"Error al insertar los datos restantes en PostgreSQL: {e}")
        connection.rollback()

consumer.close()
print("Consumer closed.")

cursor.close()
connection.close()
print("Conexión a PostgreSQL cerrada.")
