import pandas as pd
import json
import time
from kafka import KafkaProducer
from json import dumps

# Función para procesar y unificar los datasets
def procesar_dataset(filepath, year, rename_cols):
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.lower()  # Convertir a minúsculas
    for col_key, col_values in rename_cols.items():
        for col in col_values:
            if col.lower() in df.columns:
                df = df.rename(columns={col.lower(): col_key})
    df['year'] = year
    return df

# Diccionario de nombres de columnas a estandarizar
rename_cols = {
    "gdp": ["economy (gdp per capita)", "economy..gdp.per.capita.", "gdp per capita"],
    "family": ["family", "social support"],
    "life_expectancy": ["health (life expectancy)", "health..life.expectancy.", "healthy life expectancy"],
    "freedom": ["freedom", "freedom to make life choices"],
    "government_trust": ["trust (government corruption)", "trust..government.corruption.", "perceptions of corruption"],
    "happiness_rank": ["overall rank", "happiness.rank", "happiness rank"],
    "country": ["country", "country or region"],
    "happiness_score": ["happiness score", "happiness.score", "score"]
}

# Procesar y unificar los datasets
datasets = [
    procesar_dataset("./dirty-data/2015.csv", 2015, rename_cols),
    procesar_dataset("./dirty-data/2016.csv", 2016, rename_cols),
    procesar_dataset("./dirty-data/2017.csv", 2017, rename_cols),
    procesar_dataset("./dirty-data/2018.csv", 2018, rename_cols),
    procesar_dataset("./dirty-data/2019.csv", 2019, rename_cols)
]

# Crear dataset final unificado
final_dataset = pd.concat(datasets, ignore_index=True)
df = final_dataset[['happiness_score', 'gdp', 'family', 'life_expectancy', 'freedom', 'government_trust']]
df = df.dropna()  # Eliminar filas con valores nulos

# Configurar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Enviar cada fila del dataset a Kafka con un delay de 1 segundo
for index, row in df.iterrows():
    data = row.to_dict()
    producer.send('Happinessprediction', value=data)
    print(f"Enviado: {data}")
    time.sleep(1)  # Delay de 1 segundo

producer.close()
print("Datos enviados a Kafka con éxito.")
