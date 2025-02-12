import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.db.session import Base, get_db

import os
import tempfile
import pandas as pd
import numpy as np
from app.pipeline.collectors import APICollector, CSVCollector, WebScraper
from app.pipeline.processors import DataCleaner, DataAnalyzer, DataTransformer
from app.pipeline.storage import ParquetStorage, SQLiteStorage
from app.pipeline.pipeline import Pipeline

# Create test database
SQLALCHEMY_DATABASE_URL = "sqlite://"  # in-memory database

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def db():
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def client(db):
    def override_get_db():
        try:
            yield db
        finally:
            db.close()
    
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()

@pytest.fixture
def sample_data():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': range(1, 6),
        'name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
        'age': [25, 30, None, 35, 28],
        'category': ['A', 'B', 'A', 'C', 'B'],
        'value': [100.0, 150.0, 200.0, None, 175.0]
    })

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname

@pytest.fixture
def csv_file(temp_dir, sample_data):
    """Create a temporary CSV file with sample data."""
    file_path = os.path.join(temp_dir, 'test.csv')
    sample_data.to_csv(file_path, index=False)
    return file_path

@pytest.fixture
def mock_api_response():
    """Mock API response data."""
    return [
        {"id": 1, "name": "Product 1", "price": 99.99},
        {"id": 2, "name": "Product 2", "price": 149.99},
        {"id": 3, "name": "Product 3", "price": 199.99}
    ]

@pytest.fixture
def mock_html_content():
    """Mock HTML content for web scraping tests."""
    return """
    <html>
        <body>
            <div class="product">
                <h2>Product 1</h2>
                <p class="price">$99.99</p>
            </div>
            <div class="product">
                <h2>Product 2</h2>
                <p class="price">$149.99</p>
            </div>
        </body>
    </html>
    """

@pytest.fixture
def data_cleaner_config():
    """Configuration for DataCleaner."""
    return {
        'drop_duplicates': ['id'],
        'fill_values': {'age': 0, 'value': 0.0},
        'drop_columns': [],
        'rename_columns': {'name': 'full_name'}
    }

@pytest.fixture
def data_transformer_config():
    """Configuration for DataTransformer."""
    return [
        {
            'type': 'normalize',
            'columns': ['age', 'value']
        },
        {
            'type': 'encode_categorical',
            'columns': ['category']
        }
    ]

@pytest.fixture
def parquet_storage(temp_dir):
    """Create a ParquetStorage instance."""
    return ParquetStorage(base_dir=temp_dir)

@pytest.fixture
def sqlite_storage(temp_dir):
    """Create a SQLiteStorage instance."""
    db_path = os.path.join(temp_dir, 'test.db')
    return SQLiteStorage(db_path=db_path)

@pytest.fixture
def pipeline_config():
    """Configuration for creating a test pipeline."""
    return {
        "name": "test_pipeline",
        "collectors": [
            {
                "type": "api",
                "endpoint": "https://api.example.com/data",
                "headers": {"Authorization": "Bearer test-token"}
            }
        ],
        "processors": [
            {
                "type": "cleaner",
                "config": {
                    "drop_duplicates": ["id"],
                    "fill_values": {"age": 0},
                    "drop_columns": ["unnecessary_column"],
                    "rename_columns": {"old_name": "new_name"}
                }
            },
            {
                "type": "analyzer"
            }
        ],
        "storage_dir": "test_data"
    }
