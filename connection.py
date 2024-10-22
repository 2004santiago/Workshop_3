import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

localhost = os.getenv('LOCALHOST')
port = os.getenv('PORT')
nameDB = os.getenv('DB_NAME')
userDB = os.getenv('DB_USER')
passDB = os.getenv('DB_PASS')

location_file = './Dirty-Data/the_grammy_awards.csv'  
raw_table_database = 'grammy_awards'  

engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')

try:
    df = pd.read_csv(location_file, sep=",")

    df.to_sql(raw_table_database, con=engine, if_exists='replace', index=False)

    print(f"Table '{raw_table_database}' created and data uploaded successfully.")

except Exception as e:
    print(f"Error uploading data: {e}")

finally:
    engine.dispose()