import pandas as pd
from kafka import KafkaProducer
from json import dumps
import time

def procesar_dataset(filepath, year, rename_cols):
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.lower()
    for col_key, col_values in rename_cols.items():
        for col in col_values:
            if col.lower() in df.columns:
                df = df.rename(columns={col.lower(): col_key})
    df['year'] = year  
    return df

rename_cols = {
    "gdp": ["economy (gdp per capita)", "economy..gdp.per.capita.", "gdp per capita"],
    "family": ["family", "social support"],
    "life_expectancy": ["health (life expectancy)", "health..life.expectancy.", "healthy life expectancy"],
    "freedom": ["freedom", "freedom to make life choices"],
    "government_trust": ["trust (government corruption)", "trust..government.corruption.", "perceptions of corruption"],
    "happiness_rank": ["overall rank", "happiness.rank", "happiness rank"],
    "country": ["country", "country or region"],
    "happiness_score": ["happiness score", "happiness.score", "score"],
    "generosity": ["generosity"]  
}

datasets = [
    procesar_dataset("./dirty-data/2015.csv", 2015, rename_cols),
    procesar_dataset("./dirty-data/2016.csv", 2016, rename_cols),
    procesar_dataset("./dirty-data/2017.csv", 2017, rename_cols),
    procesar_dataset("./dirty-data/2018.csv", 2018, rename_cols),
    procesar_dataset("./dirty-data/2019.csv", 2019, rename_cols)
]

finalDataset = pd.concat(datasets, ignore_index=True)
df = finalDataset[['happiness_score', 'gdp', 'family', 'life_expectancy', 'freedom', 'government_trust', 'generosity']]
df = df.dropna()

# Configurar Kafka Producer
producer = KafkaProducer(
    value_serializer=lambda v: dumps(v).encode('utf-8'),
    bootstrap_servers=['localhost:9092']
)

# Enviar los datos al tema de Kafka "happiness_predictions" con un retraso de 1 segundo
for index, row in df.iterrows():
    data = row.to_dict()
    producer.send('happinessPredictions', value=data)
    print(f"Enviado: {data}")
    time.sleep(1)

producer.close()
print("Datos enviados a Kafka con éxito.")
