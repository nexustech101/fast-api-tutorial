"""
Data storage implementations for pipeline outputs.
"""
from abc import ABC, abstractmethod
import os
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

class DataStorage(ABC):
    """Base class for data storage."""
    
    @abstractmethod
    def save(self, df: pd.DataFrame, name: str) -> str:
        """Save data to storage."""
        pass
    
    @abstractmethod
    def load(self, name: str) -> pd.DataFrame:
        """Load data from storage."""
        pass

class ParquetStorage(DataStorage):
    """Storage implementation using Parquet format."""
    
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def save(self, df: pd.DataFrame, name: str) -> str:
        """
        Save DataFrame to Parquet format.
        
        Args:
            df: DataFrame to save
            name: Name of the pipeline
            
        Returns:
            str: Path where the data was saved
        """
        try:
            # Create directory if it doesn't exist
            pipeline_dir = self.base_dir / name
            pipeline_dir.mkdir(parents=True, exist_ok=True)
            
            # Save with timestamp
            file_path = pipeline_dir / f"latest.parquet"
            
            # Convert to parquet and save
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path, compression="snappy")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error saving data to Parquet: {str(e)}")
            raise
    
    def load(self, name: str) -> pd.DataFrame:
        """
        Load DataFrame from Parquet format.
        
        Args:
            name: Name of the pipeline
            
        Returns:
            pd.DataFrame: Loaded data
        """
        try:
            file_path = self.base_dir / name / "latest.parquet"
            
            if not file_path.exists():
                logger.warning(f"No data found for pipeline '{name}'")
                return pd.DataFrame()
            
            # Read parquet file
            table = pq.read_table(file_path)
            return table.to_pandas()
            
        except Exception as e:
            logger.error(f"Error loading data from Parquet: {str(e)}")
            raise
