import pandas as pd
import os
from datetime import datetime
import logging
from load_data.Base import SyntheaDataLoader
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'username')
    db_password = os.getenv('DB_PASSWORD', 'password')
    db_name = os.getenv('DB_NAME', 'synthea_omop')
    db_type = os.getenv('DB_TYPE', 'postgresql')
    db_schema = os.getenv('DB_SCHEMA', 'raw')
    csv_dir = os.getenv('CSV_DIRECTORY_PATH', 'data/synthea_csv')

    # Configuration
    csv_directory = csv_dir  # Update this path
    database_type = "postgresql"  # Change as needed
    DATABASE_CONFIGS = {
    'postgresql': f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}',
    }
    database_url = DATABASE_CONFIGS[database_type]
    
    # Initialize and run loader
    loader = SyntheaDataLoader(database_url=database_url, csv_directory=csv_directory, schema_name=db_schema)
    loader.load_synthea_files()
    
    print("Data loading completed!")

if __name__ == "__main__":
    main()