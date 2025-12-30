import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Create logs folder if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Setup logging
logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format='%(asctime)s:%(levelname)s:%(message)s',
    filemode='a'
)

# Create SQLite engine
db_engine = create_engine('sqlite:///inventory.db')  # ✅ renamed to avoid conflict

# Path to your local dataset directory
DATASETS_PATH = r'C:\Users\Administrator\Downloads\Vendor\data'  # ⬅ change to your folder

def ingest_db(df, table_name, engine):
    """
    Ingests a DataFrame into SQLite DB table
    """
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully ingested {table_name}")
    except Exception as e:
        logging.error(f"Error ingesting {table_name}: {str(e)}")

def load_raw_data():
    """
    Loads and ingests all CSVs from the dataset folder
    """
    start = time.time()
    logging.info('Ingestion Started')

    for file in os.listdir(DATASETS_PATH):
        if file.endswith('.csv'):
            file_path = os.path.join(DATASETS_PATH, file)
            logging.info(f"Ingesting {file}")
            df = pd.read_csv(file_path)
            print(f"Ingesting {file} of shape: {df.shape}")
            ingest_db(df, file[:-4], db_engine)

    total_time = (time.time() - start) / 60
    logging.info(f'Ingestion Completed in {total_time:.2f} minutes')
    print(f'✅ Ingestion Completed in {total_time:.2f} minutes')

# Run this in Jupyter cell:
load_raw_data()
