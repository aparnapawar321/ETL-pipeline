"""
Simple ETL Pipeline
Extracts data from CSV, transforms it, and loads to CSV and SQLite database
"""

import pandas as pd
import sqlite3
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract(input_file: str) -> pd.DataFrame:
    """
    Extract data from CSV file
    
    Args:
        input_file: Path to input CSV file
        
    Returns:
        DataFrame with extracted data
    """
    try:
        logger.info(f"Extracting data from {input_file}")
        df = pd.read_csv(input_file)
        logger.info(f"Extracted {len(df)} rows and {len(df.columns)} columns")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {input_file}")
        raise
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise


def transform(df: pd.DataFrame, key_column: str = None) -> pd.DataFrame:
    """
    Transform data: clean, rename, filter, and add calculated columns
    
    Args:
        df: Input DataFrame
        key_column: Column name to check for null values (optional)
        
    Returns:
        Transformed DataFrame
    """
    try:
        logger.info("Starting data transformation")
        original_rows = len(df)
        
        # Rename columns to lowercase
        df.columns = df.columns.str.lower().str.strip()
        logger.info("Renamed columns to lowercase")
        
        # Remove rows with null values in key column if specified
        if key_column:
            key_column = key_column.lower()
            if key_column in df.columns:
                df = df.dropna(subset=[key_column])
                logger.info(f"Removed rows with null values in '{key_column}'")
        
        # Fill remaining missing values with appropriate defaults
        for col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                df[col].fillna(0, inplace=True)
            else:
                df[col].fillna('Unknown', inplace=True)
        logger.info("Filled remaining missing values")
        
        # Add calculated column: row_id
        df['row_id'] = range(1, len(df) + 1)
        logger.info("Added calculated column 'row_id'")
        
        rows_removed = original_rows - len(df)
        logger.info(f"Transformation complete: {rows_removed} rows removed, {len(df)} rows remaining")
        
        return df
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


def load_to_csv(df: pd.DataFrame, output_file: str) -> None:
    """
    Load data to CSV file
    
    Args:
        df: DataFrame to save
        output_file: Path to output CSV file
    """
    try:
        logger.info(f"Loading data to CSV: {output_file}")
        df.to_csv(output_file, index=False)
        logger.info(f"Successfully saved {len(df)} rows to {output_file}")
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        raise


def load_to_database(df: pd.DataFrame, db_path: str, table_name: str = 'etl_data') -> None:
    """
    Load data to SQLite database
    
    Args:
        df: DataFrame to save
        db_path: Path to SQLite database file
        table_name: Name of the table to create/replace
    """
    try:
        logger.info(f"Loading data to SQLite database: {db_path}")
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # Verify data was loaded
        count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", conn).iloc[0]['count']
        conn.close()
        
        logger.info(f"Successfully loaded {count} rows to table '{table_name}' in {db_path}")
    except Exception as e:
        logger.error(f"Error loading to database: {e}")
        raise


def main(
    input_file: str = 'input.csv',
    output_file: str = 'output.csv',
    db_path: str = 'etl_data.db',
    key_column: str = None,
    table_name: str = 'etl_data'
) -> None:
    """
    Main ETL pipeline orchestrator
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        db_path: Path to SQLite database
        key_column: Column to check for null values during filtering
        table_name: Database table name
    """
    try:
        logger.info("=" * 50)
        logger.info("ETL Pipeline Started")
        logger.info("=" * 50)
        
        # Extract
        df = extract(input_file)
        
        # Transform
        df_transformed = transform(df, key_column=key_column)
        
        # Load
        load_to_csv(df_transformed, output_file)
        load_to_database(df_transformed, db_path, table_name)
        
        logger.info("=" * 50)
        logger.info("ETL Pipeline Completed Successfully")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # Example usage with configurable parameters
    main(
        input_file='input.csv',
        output_file='output.csv',
        db_path='etl_data.db',
        key_column='id',  # Change to your key column name or None
        table_name='etl_data'
    )
