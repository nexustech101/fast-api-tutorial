"""
Unit tests for data storage components.
"""
import os
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3
from datetime import datetime

from app.pipeline.storage import ParquetStorage, SQLiteStorage

def test_parquet_storage_save(temp_dir, sample_data):
    """Test saving data to Parquet format."""
    storage = ParquetStorage(base_dir=temp_dir)
    file_path = storage.save(sample_data, "test_data")
    
    assert os.path.exists(file_path)
    assert file_path.endswith('.parquet')
    
    # Verify the saved data
    table = pq.read_table(file_path)
    saved_df = table.to_pandas()
    pd.testing.assert_frame_equal(saved_df, sample_data)

def test_parquet_storage_load(temp_dir, sample_data):
    """Test loading data from Parquet format."""
    storage = ParquetStorage(base_dir=temp_dir)
    
    # Save and then load the data
    storage.save(sample_data, "test_data")
    loaded_df = storage.load("test_data")
    
    pd.testing.assert_frame_equal(loaded_df, sample_data)

def test_parquet_storage_versioning(temp_dir, sample_data):
    """Test if Parquet storage creates versioned files."""
    storage = ParquetStorage(base_dir=temp_dir)
    
    # Save the same data twice
    path1 = storage.save(sample_data, "test_data")
    path2 = storage.save(sample_data, "test_data")
    
    assert path1 != path2
    assert os.path.exists(path1)
    assert os.path.exists(path2)

def test_parquet_storage_invalid_directory(sample_data):
    """Test Parquet storage error handling for invalid directory."""
    storage = ParquetStorage(base_dir="/nonexistent/directory")
    
    with pytest.raises(Exception):
        storage.save(sample_data, "test_data")

def test_sqlite_storage_save(temp_dir, sample_data):
    """Test saving data to SQLite database."""
    db_path = os.path.join(temp_dir, 'test.db')
    storage = SQLiteStorage(db_path=db_path)
    
    table_name = storage.save(sample_data, "test_data")
    assert table_name == f"{db_path}:test_data"
    
    # Verify the saved data
    with sqlite3.connect(db_path) as conn:
        saved_df = pd.read_sql(f"SELECT * FROM test_data", conn)
        pd.testing.assert_frame_equal(saved_df, sample_data)

def test_sqlite_storage_load(temp_dir, sample_data):
    """Test loading data from SQLite database."""
    db_path = os.path.join(temp_dir, 'test.db')
    storage = SQLiteStorage(db_path=db_path)
    
    # Save and then load the data
    storage.save(sample_data, "test_data")
    loaded_df = storage.load("test_data")
    
    pd.testing.assert_frame_equal(loaded_df, sample_data)

def test_sqlite_storage_table_replacement(temp_dir, sample_data):
    """Test if SQLite storage properly replaces existing tables."""
    db_path = os.path.join(temp_dir, 'test.db')
    storage = SQLiteStorage(db_path=db_path)
    
    # Save initial data
    storage.save(sample_data, "test_data")
    
    # Modify and save new data
    modified_data = sample_data.copy()
    modified_data['new_column'] = range(len(modified_data))
    storage.save(modified_data, "test_data")
    
    # Load and verify the new data
    loaded_df = storage.load("test_data")
    pd.testing.assert_frame_equal(loaded_df, modified_data)

def test_sqlite_storage_invalid_table_name(temp_dir, sample_data):
    """Test SQLite storage error handling for invalid table names."""
    db_path = os.path.join(temp_dir, 'test.db')
    storage = SQLiteStorage(db_path=db_path)
    
    with pytest.raises(Exception):
        storage.load("nonexistent_table")

def test_storage_data_types(temp_dir, sample_data):
    """Test if storage backends preserve data types."""
    # Add some complex data types
    sample_data['datetime'] = pd.date_range(start='2025-01-01', periods=len(sample_data))
    sample_data['boolean'] = [True, False] * (len(sample_data) // 2) + [True] * (len(sample_data) % 2)
    
    # Test Parquet storage
    parquet_storage = ParquetStorage(base_dir=temp_dir)
    parquet_storage.save(sample_data, "test_data")
    parquet_loaded = parquet_storage.load("test_data")
    
    # Test SQLite storage
    db_path = os.path.join(temp_dir, 'test.db')
    sqlite_storage = SQLiteStorage(db_path=db_path)
    sqlite_storage.save(sample_data, "test_data")
    sqlite_loaded = sqlite_storage.load("test_data")
    
    # Verify data types are preserved in both storage backends
    for df in [parquet_loaded, sqlite_loaded]:
        assert pd.api.types.is_integer_dtype(df['id'])
        assert pd.api.types.is_object_dtype(df['name'])
        assert pd.api.types.is_float_dtype(df['value'])
        assert pd.api.types.is_boolean_dtype(df['boolean'])
