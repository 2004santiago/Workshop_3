import pandas as pd

# Función para procesar cada dataset
def procesar_dataset(filepath, year, rename_cols):
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.lower()  # Convertir a minúsculas

    # Renombrar columnas de acuerdo al diccionario
    for col_key, col_values in rename_cols.items():
        for col in col_values:
            if col.lower() in df.columns:
                df = df.rename(columns={col.lower(): col_key})
    
    df['year'] = year  # Agregar columna del año
    # print(f"Duplicados en {year}: ", df['country'].duplicated().sum() if 'country' in df.columns else "Columna 'country' no encontrada")
    return df

# Diccionario para renombrar diferentes versiones de columnas a un nombre estándar
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

# Procesar todos los datasets y concatenarlos
datasets = [
    procesar_dataset("./dirty-data/2015.csv", 2015, rename_cols),
    procesar_dataset("./dirty-data/2016.csv", 2016, rename_cols),
    procesar_dataset("./dirty-data/2017.csv", 2017, rename_cols),
    procesar_dataset("./dirty-data/2018.csv", 2018, rename_cols),
    procesar_dataset("./dirty-data/2019.csv", 2019, rename_cols)
]

finalDataset = pd.concat(datasets, ignore_index=True)

# Seleccionar columnas para el análisis
df = finalDataset[['happiness_score', 'gdp', 'family', 'life_expectancy', 'freedom', 'government_trust', 'generosity']]

# Verificar valores nulos
print(df.isnull().sum())

# Guardar el dataset final
df.to_csv("./data/modelDataset.csv", index=False)
print("Dataset saved as 'modelDataset.csv'")
