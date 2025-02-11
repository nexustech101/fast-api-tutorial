"""
Data processing and cleaning components.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
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
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with configuration for cleaning operations.
        
        Args:
            config: Dictionary containing cleaning configurations:
                - drop_duplicates: List of columns to use for duplicate detection
                - fill_values: Dict of column:value pairs for filling missing values
                - drop_columns: List of columns to drop
                - rename_columns: Dict of old_name:new_name pairs
        """
        self.config = config
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            # Create a copy to avoid modifying the original
            df = df.copy()
            
            # Drop specified columns
            if 'drop_columns' in self.config:
                df = df.drop(columns=self.config['drop_columns'], errors='ignore')
            
            # Rename columns
            if 'rename_columns' in self.config:
                df = df.rename(columns=self.config['rename_columns'])
            
            # Handle missing values
            if 'fill_values' in self.config:
                df = df.fillna(self.config['fill_values'])
            
            # Remove duplicates
            if 'drop_duplicates' in self.config:
                df = df.drop_duplicates(subset=self.config['drop_duplicates'])
            
            return df
        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise

class DataAnalyzer(DataProcessor):
    """Performs exploratory data analysis and generates statistics."""
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            stats = {
                'row_count': len(df),
                'column_count': len(df.columns),
                'missing_values': df.isnull().sum().to_dict(),
                'numeric_columns': {
                    col: {
                        'mean': df[col].mean(),
                        'std': df[col].std(),
                        'min': df[col].min(),
                        'max': df[col].max()
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
    """Handles data transformations for ML preparation."""
    
    def __init__(self, transformations: List[Dict[str, Any]]):
        """
        Initialize with list of transformations to apply.
        
        Args:
            transformations: List of transformation configs:
                - type: one of ['normalize', 'encode_categorical', 'datetime']
                - columns: columns to apply transformation to
                - params: additional parameters for the transformation
        """
        self.transformations = transformations
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.copy()
            
            for transform in self.transformations:
                if transform['type'] == 'normalize':
                    for col in transform['columns']:
                        df[col] = (df[col] - df[col].mean()) / df[col].std()
                
                elif transform['type'] == 'encode_categorical':
                    for col in transform['columns']:
                        df[col] = pd.Categorical(df[col]).codes
                
                elif transform['type'] == 'datetime':
                    for col in transform['columns']:
                        df[col] = pd.to_datetime(df[col])
            
            return df
        except Exception as e:
            logger.error(f"Error during data transformation: {str(e)}")
            raise
