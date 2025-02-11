"""
Data storage components for processed data.
"""
from abc import ABC, abstractmethod
from datetime import datetime
import os
from typing import Optional
import pandas as pd
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3

class DataStorage(ABC):
    """Base class for data storage implementations."""
    
    @abstractmethod
    def save(self, df: pd.DataFrame, name: str) -> str:
        """Save the DataFrame."""
        pass
    
    @abstractmethod
    def load(self, name: str) -> pd.DataFrame:
        """Load the DataFrame."""
        pass

class ParquetStorage(DataStorage):
    """Storage implementation using Parquet format."""
    
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
    
    def _get_path(self, name: str) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(self.base_dir, f"{name}_{timestamp}.parquet")
    
    def save(self, df: pd.DataFrame, name: str) -> str:
        try:
            file_path = self._get_path(name)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path, compression='snappy')
            logger.info(f"Data saved to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error saving data to parquet: {str(e)}")
            raise
    
    def load(self, name: str) -> pd.DataFrame:
        try:
            # Get the latest file for the given name
            files = [f for f in os.listdir(self.base_dir) if f.startswith(name) and f.endswith('.parquet')]
            if not files:
                raise FileNotFoundError(f"No parquet files found for {name}")
            
            latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(self.base_dir, x)))
            file_path = os.path.join(self.base_dir, latest_file)
            
            table = pq.read_table(file_path)
            return table.to_pandas()
        except Exception as e:
            logger.error(f"Error loading data from parquet: {str(e)}")
            raise

class SQLiteStorage(DataStorage):
    """Storage implementation using SQLite database."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def save(self, df: pd.DataFrame, name: str) -> str:
        try:
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(name, conn, if_exists='replace', index=False)
                logger.info(f"Data saved to table {name} in {self.db_path}")
                return f"{self.db_path}:{name}"
        except Exception as e:
            logger.error(f"Error saving data to SQLite: {str(e)}")
            raise
    
    def load(self, name: str) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql(f"SELECT * FROM {name}", conn)
        except Exception as e:
            logger.error(f"Error loading data from SQLite: {str(e)}")
            raise
