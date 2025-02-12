"""
Data storage implementations for pipeline outputs.
"""
from abc import ABC, abstractmethod
import json
import os
from pathlib import Path
from typing import Dict, Optional
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
    
    @abstractmethod
    def get_schema(self, name: str) -> Dict:
        """Get schema of stored data."""
        pass

class ParquetStorage(DataStorage):
    """Storage implementation using Parquet format."""
    
    def __init__(self, base_dir: str):
        """
        Initialize storage with base directory.
        
        Args:
            base_dir: Base directory for storing data
            
        Raises:
            OSError: If directory creation fails
        """
        self.base_dir = Path(base_dir)
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create directory {base_dir}: {str(e)}")
            raise OSError(f"Failed to create directory {base_dir}: {str(e)}")
    
    def _get_pipeline_dir(self, name: str) -> Path:
        """Get directory for a specific pipeline."""
        return self.base_dir / name
    
    def _get_data_path(self, name: str) -> Path:
        """Get path to latest data file."""
        return self._get_pipeline_dir(name) / "latest.parquet"
    
    def _get_schema_path(self, name: str) -> Path:
        """Get path to schema file."""
        return self._get_pipeline_dir(name) / "schema.json"
    
    def _save_schema(self, df: pd.DataFrame, name: str):
        """Save DataFrame schema to JSON."""
        schema = {
            'columns': list(df.columns),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'shape': list(df.shape)
        }
        
        schema_path = self._get_schema_path(name)
        try:
            with open(schema_path, 'w') as f:
                json.dump(schema, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save schema for pipeline '{name}': {str(e)}")
            raise
    
    def save(self, df: pd.DataFrame, name: str) -> str:
        """
        Save DataFrame to Parquet format.
        
        Args:
            df: DataFrame to save
            name: Name of the pipeline
            
        Returns:
            str: Path where the data was saved
            
        Raises:
            ValueError: If DataFrame is empty or name is invalid
            OSError: If file operations fail
        """
        if df.empty:
            logger.warning("Attempting to save empty DataFrame")
            raise ValueError("Cannot save empty DataFrame")
        
        if not name or not isinstance(name, str):
            raise ValueError("Invalid pipeline name")
        
        try:
            # Create pipeline directory
            pipeline_dir = self._get_pipeline_dir(name)
            pipeline_dir.mkdir(parents=True, exist_ok=True)
            
            # Save data file
            file_path = self._get_data_path(name)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path, compression="snappy")
            
            # Save schema
            self._save_schema(df, name)
            
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
            
        Raises:
            FileNotFoundError: If data file doesn't exist
            ValueError: If name is invalid
        """
        if not name or not isinstance(name, str):
            raise ValueError("Invalid pipeline name")
        
        try:
            file_path = self._get_data_path(name)
            
            if not file_path.exists():
                logger.warning(f"No data found for pipeline '{name}'")
                return pd.DataFrame()
            
            # Read parquet file
            table = pq.read_table(file_path)
            df = table.to_pandas()
            
            if df.empty:
                logger.warning(f"Loaded empty DataFrame for pipeline '{name}'")
            
            return df
            
        except FileNotFoundError:
            logger.warning(f"Data file not found for pipeline '{name}'")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading data from Parquet: {str(e)}")
            raise
    
    def get_schema(self, name: str) -> Dict:
        """
        Get schema of stored data.
        
        Args:
            name: Name of the pipeline
            
        Returns:
            dict: Schema information including columns, dtypes, and shape
            
        Raises:
            FileNotFoundError: If schema file doesn't exist
            ValueError: If name is invalid
        """
        if not name or not isinstance(name, str):
            raise ValueError("Invalid pipeline name")
        
        try:
            schema_path = self._get_schema_path(name)
            
            if not schema_path.exists():
                logger.warning(f"No schema found for pipeline '{name}'")
                return {}
            
            with open(schema_path, 'r') as f:
                return json.load(f)
                
        except FileNotFoundError:
            logger.warning(f"Schema file not found for pipeline '{name}'")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid schema file for pipeline '{name}': {str(e)}")
            raise ValueError(f"Invalid schema file: {str(e)}")
        except Exception as e:
            logger.error(f"Error reading schema for pipeline '{name}': {str(e)}")
            raise
