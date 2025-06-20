from sqlalchemy import create_engine, text, inspect
from datetime import datetime
import pandas as pd
import os

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SyntheaDataLoader:
    def __init__(self, database_url: str, csv_directory: str, schema_name='raw'):
        """
        Initialize the data loader
        
        Args:
            database_url (str): Database connection string
            csv_directory (str): Path to directory containing Synthea CSV files
        """
        self.engine = create_engine(database_url)
        self.csv_directory = csv_directory
        self.schema_name = schema_name
        self.create_schema_if_not_exists(schema_name=schema_name)  # Ensure schema exists on initialization

    def create_schema_if_not_exists(self, schema_name='raw'):
        """Create schema if it doesn't exist"""
        try:
            with self.engine.connect() as conn:
                # Simple CREATE SCHEMA IF NOT EXISTS
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                conn.commit()
                logger.info(f"Schema {schema_name} ready")
                    
        except Exception as e:
            logger.error(f"Error creating schema {schema_name}: {str(e)}")
            raise
        
    def get_datetime_columns(self, df):
        """Convert datetime columns to proper datetime format for PostgreSQL"""
        for col in df.columns:
            # Skip if already datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
                
            # For object columns, check if they contain date-like values
            if pd.api.types.is_object_dtype(df[col]):
                # Get a sample of non-null values
                sample = df[col].dropna().head(100)
                if len(sample) == 0:
                    continue
                    
                # Try converting sample with common formats first
                try:
                    # Common Synthea date formats
                    formats_to_try = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%Y-%m-%dT%H:%M:%SZ']
                    
                    converted_sample = None
                    for fmt in formats_to_try:
                        try:
                            converted_sample = pd.to_datetime(sample, format=fmt, errors='coerce')
                            if converted_sample.notna().sum() / len(sample) > 0.7:
                                # Found a good format, use it for the whole column
                                df[col] = pd.to_datetime(df[col], format=fmt, errors='coerce')
                                logger.info(f"Converted datetime column: {col} using format {fmt}")
                                break
                        except:
                            continue
                    
                    # If no specific format worked, fall back to infer (with warning suppressed)
                    if converted_sample is None or converted_sample.notna().sum() / len(sample) <= 0.7:
                        import warnings
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            converted_sample = pd.to_datetime(sample, errors='coerce')
                            success_rate = converted_sample.notna().sum() / len(sample)
                            
                            if success_rate > 0.7:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                                logger.info(f"Converted datetime column: {col} (success rate: {success_rate:.2%})")
                except:
                    continue
        
        return df

    def load_csv_to_df(self, filename):
        """Load CSV file into pandas DataFrame with proper data types"""
        filepath = os.path.join(self.csv_directory, filename)
        
        if not os.path.exists(filepath):
            logger.warning(f"File {filepath} not found, skipping...")
            return None
            
        logger.info(f"Loading {filename}...")
        
        # Read CSV
        df = pd.read_csv(filepath)
        
        logger.info(f"Loaded {len(df)} rows from {filename}")
        return df
    
    def clean_dataframe(self, df, table_name):
        """Clean and validate DataFrame before loading"""
        if df is None:
            return None
            
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Clean column names (remove spaces, special characters)
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Add metadata columns
        df['loaded_at'] = datetime.now()
        df['source_file'] = table_name
        
        return df
    
    def load_to_database(self, df, table_name, if_exists='replace'):
        """Load DataFrame to database table"""
        if df is None:
            logger.warning(f"No data to load for {table_name}")
            return
            
        logger.info(f"Loading {len(df)} rows to {table_name} table...")
        
        try:

            inspector = inspect(self.engine)
            if table_name in inspector.get_table_names(schema=self.schema_name):
                with self.engine.begin() as conn:
                    conn.execute(text(f'TRUNCATE TABLE "{self.schema_name}"."{table_name}"'))
                if_exists = 'append'

            df.to_sql(
                table_name, 
                self.engine,
                schema=self.schema_name,  # Change as needed 
                if_exists=if_exists,  # 'replace', 'append', or 'fail'
                index=False,
                chunksize=10000  # Load in chunks for better performance
            )
            logger.info(f"Successfully loaded {table_name}")
            
        except Exception as e:
            logger.error(f"Error loading {table_name}: {str(e)}")
            raise
    
    def load_synthea_files(self):
        """Load all Synthea CSV files"""
        
        # Define files to load with their date columns
        
        for filename in os.listdir(self.csv_directory):
            if not filename.endswith('.csv'):
                continue
            # get the full file path
            filepath = os.path.join(self.csv_directory, filename)
            if not os.path.isfile(filepath):
                logger.warning(f"Skipping {filename}, not a file.")
                continue
            # Define configuration for each file
            df = self.load_csv_to_df(filename)
            table_name = os.path.splitext(filename)[0]

            # Clean the data
            df_clean = self.clean_dataframe(df, table_name)
            df_clean = self.get_datetime_columns(df_clean)
            # Load to database
            self.load_to_database(df_clean, table_name, if_exists='replace')
