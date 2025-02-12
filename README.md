# FastAPI Data Pipeline Framework

A robust and extensible data pipeline framework built with FastAPI, designed for collecting, processing, and analyzing data from various sources. This framework provides a flexible architecture for building ML-ready data pipelines with REST API interfaces.

## 📋 Table of Contents
- [Features](#-features)
- [Getting Started](#-getting-started)
- [API Documentation](#-api-documentation)
- [Architecture](#-architecture)
- [Example Use Cases](#-example-use-cases)
- [Contributing](#-contributing)
- [Testing](#-testing)
- [License](#-license)

## 🌟 Features

- **Modular Data Collection**
  - REST API integration with support for custom headers and authentication
  - CSV file ingestion with configurable parsing options
  - Web scraping capabilities with CSS selector support
  - Extensible collector interface for custom sources
  - Parallel data collection from multiple sources

- **Powerful Data Processing**
  - Automated data cleaning and validation
  - Missing value handling with customizable strategies
  - Duplicate detection and removal
  - Column transformations and renaming
  - Statistical analysis and insights generation
  - Custom processor pipeline configuration

- **ML-Ready Data Transformation**
  - Data normalization and standardization
  - Categorical encoding (one-hot, label)
  - DateTime processing and feature extraction
  - Customizable transformation pipelines
  - Data type validation and conversion

- **Flexible Data Storage**
  - Parquet file storage (optimized for ML workloads)
  - SQLite database integration
  - Versioned data storage with timestamps
  - Easy retrieval of latest processed data
  - Schema validation and evolution

- **FastAPI Integration**
  - RESTful API endpoints with OpenAPI documentation
  - Async execution support for better performance
  - Real-time pipeline status monitoring
  - Swagger/OpenAPI documentation
  - Error handling and validation
  - CORS support

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- pip (Python package manager)
- Git (for version control)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/fastapi-data-pipeline.git
cd fastapi-data-pipeline
```

2. Create and activate a virtual environment (recommended):
```bash
# Windows
python -m venv venv
.\venv\Scripts\activate

# Linux/MacOS
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Run the server:
```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`. Access the interactive API documentation at `http://localhost:8000/docs`.

## 📖 API Documentation

### Health Check
```http
GET /health
```
Check if the API is running.

**Response:**
```json
{
    "status": "healthy"
}
```

### Pipeline Management

#### Create Pipeline
```http
POST /api/v1/pipeline
```
Create a new data pipeline with specified configuration.

**Request Body:**
```json
{
    "name": "users-pipeline",
    "collectors": [
        {
            "type": "api",
            "endpoint": "https://api.example.com/data",
            "headers": {
                "Authorization": "Bearer ${API_KEY}"
            }
        }
    ],
    "processors": [
        {
            "type": "cleaner",
            "config": {
                "drop_columns": ["unwanted_col"],
                "fill_na": {
                    "email": "no-email"
                }
            }
        },
        {
            "type": "transformer",
            "config": {
                "rename_columns": {
                    "old_name": "new_name"
                }
            }
        }
    ],
    "storage_dir": "data/processed"
}
```

**Response:**
```json
{
    "message": "Pipeline 'users-pipeline' created successfully"
}
```

#### Execute Pipeline
```http
POST /api/v1/pipeline/{name}/run
```
Execute a pipeline by its name.

**Parameters:**
- `name` (path): Name of the pipeline to execute

**Response:**
```json
{
    "message": "Pipeline 'users-pipeline' executed successfully",
    "storage_location": "data/processed/users-pipeline/latest.parquet"
}
```

#### Get Pipeline Status
```http
GET /api/v1/pipeline/{name}
```
Get the current status of a pipeline.

**Parameters:**
- `name` (path): Name of the pipeline

**Response:**
```json
{
    "status": "running",
    "progress": 75,
    "error": null,
    "start_time": "2025-02-11T21:30:00",
    "end_time": null,
    "duration": null
}
```

#### Get Pipeline Data
```http
GET /api/v1/pipeline/{name}/data
```
Get the latest processed data from a pipeline.

**Parameters:**
- `name` (path): Name of the pipeline

**Response:**
```json
[
    {
        "id": 1,
        "name": "John Doe",
        "email": "john@example.com"
    },
    {
        "id": 2,
        "name": "Jane Smith",
        "email": "jane@example.com"
    }
]
```

#### Get Pipeline Schema
```http
GET /api/v1/pipeline/{name}/schema
```
Get the schema information of pipeline data.

**Parameters:**
- `name` (path): Name of the pipeline

**Response:**
```json
{
    "columns": [
        {
            "name": "id",
            "type": "int64",
            "nullable": false,
            "unique_values": 100,
            "missing_values": 0
        },
        {
            "name": "name",
            "type": "string",
            "nullable": false,
            "unique_values": 95,
            "missing_values": 0
        },
        {
            "name": "email",
            "type": "string",
            "nullable": true,
            "unique_values": 98,
            "missing_values": 2
        }
    ],
    "row_count": 100,
    "column_count": 3,
    "data_types": {
        "id": "int64",
        "name": "string",
        "email": "string"
    },
    "sample_data": [
        {
            "id": 1,
            "name": "John Doe",
            "email": "john@example.com"
        }
    ],
    "memory_usage": {
        "total": 24576,
        "by_column": {
            "id": 8192,
            "name": 8192,
            "email": 8192
        }
    }
}
```

#### Get Pipeline Stats
```http
GET /api/v1/pipeline/{name}/stats
```
Get pipeline execution statistics.

**Parameters:**
- `name` (path): Name of the pipeline

**Response:**
```json
{
    "total_executions": 10,
    "successful_executions": 9,
    "failed_executions": 1,
    "success_rate": 0.9,
    "average_duration": 0.25,
    "total_rows_processed": 1000,
    "last_execution": {
        "start_time": "2025-02-11T21:30:00",
        "end_time": "2025-02-11T21:30:01",
        "duration": 0.25,
        "status": "success",
        "rows_processed": 100,
        "error": null
    },
    "last_error": null
}
```

#### Update Pipeline
```http
PUT /api/v1/pipeline/{name}
```
Update an existing pipeline configuration.

**Parameters:**
- `name` (path): Name of the pipeline to update

**Request Body:** Same format as create pipeline

**Response:**
```json
{
    "message": "Pipeline 'users-pipeline' updated successfully"
}
```

#### Delete Pipeline
```http
DELETE /api/v1/pipeline/{name}
```
Delete a pipeline configuration.

**Parameters:**
- `name` (path): Name of the pipeline to delete

**Response:**
```json
{
    "message": "Pipeline 'users-pipeline' deleted successfully"
}
```

## 🏗️ Architecture

### Core Components

1. **Data Collectors (`app.pipeline.collectors`)**
   - Abstract base class: `DataCollector`
   - Implementations:
     - `APICollector`: Fetches data from REST APIs with configurable headers and auth
     - `CSVCollector`: Reads data from CSV files with parsing options
     - `WebScraper`: Scrapes data from websites using CSS selectors
   - Features:
     - Async data collection
     - Error handling and retries
     - Data validation
     - Rate limiting support

2. **Data Processors (`app.pipeline.processors`)**
   - Base class: `DataProcessor`
   - Implementations:
     - `DataCleaner`: Handles data cleaning operations
       - Missing value imputation
       - Duplicate removal
       - Type conversion
     - `DataAnalyzer`: Performs statistical analysis
       - Descriptive statistics
       - Correlation analysis
       - Outlier detection
     - `DataTransformer`: Prepares data for ML tasks
       - Feature scaling
       - Encoding
       - Feature extraction

3. **Data Storage (`app.pipeline.storage`)**
   - Interface: `DataStorage`
   - Implementations:
     - `ParquetStorage`: Stores data in Parquet format
       - Schema management
       - Compression support
       - Partitioning
     - `SQLiteStorage`: Stores data in SQLite database
       - Index management
       - Query optimization
       - Transaction support

4. **Pipeline Management (`app.pipeline.pipeline`)**
   - Main class: `Pipeline`
   - Responsibilities:
     - Orchestrates data flow
     - Manages component lifecycle
     - Handles errors and retries
     - Tracks execution status
     - Collects metrics

### Data Flow

1. **Collection Phase**
   ```
   [API Source] ─┐
   [CSV Files] ──┼─> [Collectors] ─> [Raw Data DataFrame]
   [Web Pages] ─┘
   ```

2. **Processing Phase**
   ```
   [Raw Data] ─> [Cleaner] ─> [Analyzer] ─> [Transformer] ─> [Processed Data]
   ```

3. **Storage Phase**
   ```
   [Processed Data] ─> [Storage Backend] ─> [Versioned Storage]
   ```

## 📊 Example Use Cases

### 1. Financial Data Analysis Pipeline
```python
# Pipeline configuration
{
    "name": "stock-analysis",
    "collectors": [
        {
            "type": "api",
            "endpoint": "https://api.finance.com/stocks",
            "headers": {"Authorization": "Bearer ${API_KEY}"}
        }
    ],
    "processors": [
        {
            "type": "cleaner",
            "config": {
                "drop_columns": ["unnecessary_fields"],
                "fill_na": {"volume": 0}
            }
        },
        {
            "type": "transformer",
            "config": {
                "normalize": ["price", "volume"],
                "add_features": ["moving_average_7d"]
            }
        }
    ],
    "storage_dir": "data/stocks"
}
```

### 2. E-commerce Product Analysis
```python
# Pipeline configuration
{
    "name": "product-analysis",
    "collectors": [
        {
            "type": "web",
            "url": "https://example.com/products",
            "css_selectors": {
                "name": ".product-name",
                "price": ".product-price",
                "rating": ".product-rating"
            }
        }
    ],
    "processors": [
        {
            "type": "cleaner",
            "config": {
                "convert_types": {
                    "price": "float",
                    "rating": "float"
                }
            }
        },
        {
            "type": "analyzer",
            "config": {
                "group_by": ["category"],
                "metrics": ["avg_price", "avg_rating"]
            }
        }
    ],
    "storage_dir": "data/products"
}
```

### 3. IoT Sensor Data Processing
```python
# Pipeline configuration
{
    "name": "sensor-data",
    "collectors": [
        {
            "type": "api",
            "endpoint": "https://iot-hub.com/sensors",
            "interval": "5m"
        }
    ],
    "processors": [
        {
            "type": "cleaner",
            "config": {
                "outlier_removal": {
                    "method": "zscore",
                    "threshold": 3
                }
            }
        },
        {
            "type": "transformer",
            "config": {
                "time_features": ["hour", "day_of_week"],
                "rolling_stats": ["mean", "std"]
            }
        }
    ],
    "storage_dir": "data/sensors"
}
```

## 🤝 Contributing

We welcome contributions! Here's how you can help:

### Development Setup

1. Fork the repository
2. Create a development environment:
```bash
python -m venv venv
source venv/bin/activate  # or .\venv\Scripts\activate on Windows
pip install -r requirements-dev.txt
```

3. Install pre-commit hooks:
```bash
pre-commit install
```

### Making Changes

1. Create a feature branch:
```bash
git checkout -b feature/amazing-feature
```

2. Make your changes following our coding standards:
   - Use type hints
   - Write docstrings for functions and classes
   - Follow PEP 8 guidelines
   - Add unit tests for new features

3. Run tests:
```bash
pytest tests/
```

4. Commit your changes:
```bash
git commit -m 'feat: add amazing feature'
```

5. Push to your fork:
```bash
git push origin feature/amazing-feature
```

6. Open a Pull Request

### Code Standards

- Use async/await for I/O operations
- Include type hints
- Write comprehensive docstrings
- Add unit tests for new features
- Follow semantic versioning

## 🧪 Testing

### Unit Tests
```bash
pytest tests/unit/
```

### Integration Tests
```bash
pytest tests/integration/
```

### API Tests
```powershell
.\test_api.ps1
```

### Performance Tests
```bash
pytest tests/performance/
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- FastAPI framework and community
- Pandas and PyArrow teams
- All contributors and users of this framework

## 📮 Contact

For questions and support, please:
1. Open an issue in the repository
2. Join our Discord community
3. Check our documentation
