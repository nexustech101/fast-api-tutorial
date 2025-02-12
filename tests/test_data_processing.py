"""
Unit tests for data processing components.
"""
import pytest
import pandas as pd
import numpy as np
from app.pipeline.processors import DataCleaner, DataAnalyzer, DataTransformer

def test_data_cleaner_drop_duplicates(sample_data, data_cleaner_config):
    """Test DataCleaner's duplicate removal functionality."""
    # Add a duplicate row
    duplicate_data = pd.concat([sample_data, sample_data.iloc[[0]]], ignore_index=True)
    
    cleaner = DataCleaner(config=data_cleaner_config)
    result = cleaner.process(duplicate_data)
    
    assert len(result) == len(sample_data)
    assert not result.duplicated(subset=['id']).any()

def test_data_cleaner_fill_missing_values(sample_data, data_cleaner_config):
    """Test DataCleaner's missing value handling."""
    cleaner = DataCleaner(config=data_cleaner_config)
    result = cleaner.process(sample_data)
    
    assert not result['age'].isnull().any()
    assert not result['value'].isnull().any()
    assert result['age'].fillna(0).equals(result['age'])
    assert result['value'].fillna(0.0).equals(result['value'])

def test_data_cleaner_rename_columns(sample_data, data_cleaner_config):
    """Test DataCleaner's column renaming functionality."""
    cleaner = DataCleaner(config=data_cleaner_config)
    result = cleaner.process(sample_data)
    
    assert 'full_name' in result.columns
    assert 'name' not in result.columns
    assert result['full_name'].equals(sample_data['name'])

def test_data_analyzer_statistics(sample_data):
    """Test DataAnalyzer's statistical computations."""
    analyzer = DataAnalyzer()
    result = analyzer.process(sample_data)
    
    # Analyzer should return the same DataFrame but compute statistics
    pd.testing.assert_frame_equal(result, sample_data)
    
    # Check if statistics were computed correctly
    numeric_cols = sample_data.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        assert not pd.isna(sample_data[col].mean())
        assert not pd.isna(sample_data[col].std())

def test_data_transformer_normalization(sample_data, data_transformer_config):
    """Test DataTransformer's normalization functionality."""
    transformer = DataTransformer(transformations=data_transformer_config)
    result = transformer.process(sample_data)
    
    # Check if numeric columns were normalized
    for col in ['age', 'value']:
        non_null_values = result[col].dropna()
        assert -4 <= non_null_values.min() <= 4  # Standard scaling typically results in this range
        assert -4 <= non_null_values.max() <= 4
        assert abs(non_null_values.mean()) < 0.01  # Should be close to 0
        assert abs(non_null_values.std() - 1) < 0.01  # Should be close to 1

def test_data_transformer_categorical_encoding(sample_data, data_transformer_config):
    """Test DataTransformer's categorical encoding functionality."""
    transformer = DataTransformer(transformations=data_transformer_config)
    result = transformer.process(sample_data)
    
    # Check if categorical columns were encoded
    assert pd.api.types.is_numeric_dtype(result['category'])
    assert len(result['category'].unique()) == len(sample_data['category'].unique())

def test_data_transformer_invalid_column(sample_data):
    """Test DataTransformer's error handling for invalid columns."""
    invalid_config = [{
        'type': 'normalize',
        'columns': ['nonexistent_column']
    }]
    
    transformer = DataTransformer(transformations=invalid_config)
    with pytest.raises(KeyError):
        transformer.process(sample_data)

def test_data_transformer_invalid_transformation_type(sample_data):
    """Test DataTransformer's error handling for invalid transformation type."""
    invalid_config = [{
        'type': 'invalid_type',
        'columns': ['age']
    }]
    
    transformer = DataTransformer(transformations=invalid_config)
    result = transformer.process(sample_data)
    
    # Should ignore invalid transformation type and return original DataFrame
    pd.testing.assert_frame_equal(result, sample_data)

def test_processor_chain(sample_data, data_cleaner_config, data_transformer_config):
    """Test chaining multiple processors together."""
    cleaner = DataCleaner(config=data_cleaner_config)
    analyzer = DataAnalyzer()
    transformer = DataTransformer(transformations=data_transformer_config)
    
    # Process data through the chain
    cleaned_data = cleaner.process(sample_data)
    analyzed_data = analyzer.process(cleaned_data)
    final_data = transformer.process(analyzed_data)
    
    # Verify the chain's effects
    assert 'full_name' in final_data.columns
    assert not final_data.isnull().any().any()
    assert pd.api.types.is_numeric_dtype(final_data['category'])
