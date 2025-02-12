"""
Data processing and cleaning components.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set
import numpy as np
import pandas as pd
from loguru import logger

class DataProcessor(ABC):
    """Base class for all data processors."""
    
    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the input DataFrame."""
        pass

class DataCleaner(DataProcessor):
    """Handles basic data cleaning operations."""
    
    VALID_CONFIG_KEYS: Set[str] = {'drop_columns', 'rename_columns', 'fill_na', 'drop_duplicates'}
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with configuration for cleaning operations.
        
        Args:
            config: Dictionary containing cleaning configurations:
                - drop_columns: List of columns to drop
                - rename_columns: Dict of old_name:new_name pairs
                - fill_na: Dict of column:value pairs for filling missing values
                - drop_duplicates: List of columns to use for duplicate detection
                
        Raises:
            ValueError: If config contains invalid keys or values
        """
        # Validate config keys
        invalid_keys = set(config.keys()) - self.VALID_CONFIG_KEYS
        if invalid_keys:
            raise ValueError(f"Invalid config keys: {invalid_keys}")
        
        # Validate config values
        if 'drop_columns' in config and not isinstance(config['drop_columns'], (list, tuple)):
            raise ValueError("drop_columns must be a list or tuple")
        
        if 'rename_columns' in config and not isinstance(config['rename_columns'], dict):
            raise ValueError("rename_columns must be a dictionary")
        
        if 'fill_na' in config and not isinstance(config['fill_na'], dict):
            raise ValueError("fill_na must be a dictionary")
        
        if 'drop_duplicates' in config and not isinstance(config['drop_duplicates'], (list, tuple)):
            raise ValueError("drop_duplicates must be a list or tuple")
        
        self.config = config
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the input DataFrame according to the configuration.
        
        Args:
            df: Input DataFrame to clean
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
            
        Raises:
            KeyError: If specified columns don't exist in the DataFrame
            ValueError: If cleaning operations fail
        """
        try:
            # Create a copy to avoid modifying the original
            df = df.copy()
            
            # Drop specified columns
            if 'drop_columns' in self.config:
                missing_cols = set(self.config['drop_columns']) - set(df.columns)
                if missing_cols:
                    logger.warning(f"Some columns to drop don't exist: {missing_cols}")
                df = df.drop(columns=self.config['drop_columns'], errors='ignore')
            
            # Rename columns
            if 'rename_columns' in self.config:
                missing_cols = set(self.config['rename_columns'].keys()) - set(df.columns)
                if missing_cols:
                    logger.warning(f"Some columns to rename don't exist: {missing_cols}")
                df = df.rename(columns=self.config['rename_columns'])
            
            # Handle missing values
            if 'fill_na' in self.config:
                missing_cols = set(self.config['fill_na'].keys()) - set(df.columns)
                if missing_cols:
                    logger.warning(f"Some columns to fill NA don't exist: {missing_cols}")
                df = df.fillna(self.config['fill_na'])
            
            # Remove duplicates
            if 'drop_duplicates' in self.config:
                missing_cols = set(self.config['drop_duplicates']) - set(df.columns)
                if missing_cols:
                    logger.warning(f"Some columns for duplicate detection don't exist: {missing_cols}")
                valid_cols = list(set(self.config['drop_duplicates']) & set(df.columns))
                if valid_cols:
                    df = df.drop_duplicates(subset=valid_cols)
                else:
                    logger.warning("No valid columns for duplicate detection")
            
            return df
            
        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise

class DataAnalyzer(DataProcessor):
    """Performs exploratory data analysis and generates statistics."""
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze the DataFrame and log statistics.
        
        Args:
            df: Input DataFrame to analyze
            
        Returns:
            pd.DataFrame: Same DataFrame (unmodified)
            
        Note:
            This processor only generates statistics, it doesn't modify the data.
        """
        try:
            stats = {
                'row_count': len(df),
                'column_count': len(df.columns),
                'missing_values': df.isnull().sum().to_dict(),
                'numeric_columns': {
                    col: {
                        'mean': float(df[col].mean()),
                        'std': float(df[col].std()),
                        'min': float(df[col].min()),
                        'max': float(df[col].max())
                    }
                    for col in df.select_dtypes(include=[np.number]).columns
                },
                'categorical_columns': {
                    col: df[col].value_counts().to_dict()
                    for col in df.select_dtypes(include=['object', 'category']).columns
                }
            }
            
            logger.info(f"Data Analysis Results: {stats}")
            return df
        except Exception as e:
            logger.error(f"Error during data analysis: {str(e)}")
            raise

class DataTransformer(DataProcessor):
    """Handles data transformations."""
    
    VALID_OPERATIONS = {'rename_columns'}
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with transformation configuration.
        
        Args:
            config: Dictionary containing transformation configurations:
                - rename_columns: Dict of old_name:new_name pairs
                
        Raises:
            ValueError: If config contains invalid operations
        """
        invalid_ops = set(config.keys()) - self.VALID_OPERATIONS
        if invalid_ops:
            raise ValueError(f"Invalid operations in config: {invalid_ops}")
        
        if 'rename_columns' in config and not isinstance(config['rename_columns'], dict):
            raise ValueError("rename_columns must be a dictionary")
        
        self.config = config
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the DataFrame according to the configuration.
        
        Args:
            df: Input DataFrame to transform
            
        Returns:
            pd.DataFrame: Transformed DataFrame
            
        Raises:
            KeyError: If specified columns don't exist
            ValueError: If transformations fail
        """
        try:
            df = df.copy()
            
            if 'rename_columns' in self.config:
                missing_cols = set(self.config['rename_columns'].keys()) - set(df.columns)
                if missing_cols:
                    logger.warning(f"Some columns to rename don't exist: {missing_cols}")
                df = df.rename(columns=self.config['rename_columns'])
            
            return df
        except Exception as e:
            logger.error(f"Error during data transformation: {str(e)}")
            raise
