# FastAPI Data Pipeline Framework

A robust and extensible data pipeline framework built with FastAPI, designed for collecting, processing, and analyzing data from various sources. This framework provides a flexible architecture for building ML-ready data pipelines with REST API interfaces.

## 🌟 Features

- **Modular Data Collection**
  - REST API integration
  - CSV file ingestion
  - Web scraping capabilities
  - Extensible collector interface for custom sources

- **Powerful Data Processing**
  - Automated data cleaning
  - Missing value handling
  - Duplicate detection and removal
  - Column transformations and renaming
  - Statistical analysis and insights generation

- **ML-Ready Data Transformation**
  - Data normalization
  - Categorical encoding
  - DateTime processing
  - Customizable transformation pipelines

- **Flexible Data Storage**
  - Parquet file storage (optimized for ML workloads)
  - SQLite database integration
  - Versioned data storage with timestamps
  - Easy retrieval of latest processed data

- **FastAPI Integration**
  - RESTful API endpoints
  - Async execution support
  - Real-time pipeline status monitoring
  - Swagger/OpenAPI documentation

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- pip (Python package manager)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/fastapi-data-pipeline.git
cd fastapi-data-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start the FastAPI server:
```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`. Access the interactive API documentation at `http://localhost:8000/docs`.

## 📖 API Documentation

### Pipeline Management Endpoints

#### 1. Create Pipeline
```http
POST /api/v1/pipeline/create
```

Create a new data pipeline with specified collectors, processors, and storage configuration.

**Request Body:**
```json
{
    "name": "example_pipeline",
    "collectors": [
        {
            "type": "api",
            "endpoint": "https://api.example.com/data",
            "headers": {
                "Authorization": "Bearer token"
            }
        },
        {
            "type": "csv",
            "file_path": "data/input.csv"
        }
    ],
    "processors": [
        {
            "type": "cleaner",
            "config": {
                "drop_duplicates": ["id"],
                "fill_values": {"age": 0},
                "drop_columns": ["unnecessary_column"],
                "rename_columns": {
                    "old_name": "new_name"
                }
            }
        },
        {
            "type": "analyzer"
        },
        {
            "type": "transformer",
            "transformations": [
                {
                    "type": "normalize",
                    "columns": ["age", "salary"]
                },
                {
                    "type": "encode_categorical",
                    "columns": ["category", "status"]
                }
            ]
        }
    ],
    "storage_dir": "data/processed"
}
```

#### 2. Update Pipeline Configuration
```http
PUT /api/v1/pipeline/{name}/config
```

Update an existing pipeline's configuration.

**Request Body:** Same as create pipeline

**Response:**
```json
{
    "message": "Pipeline 'example_pipeline' configuration updated successfully"
}
```

#### 3. Delete Pipeline
```http
DELETE /api/v1/pipeline/{name}
```

Delete an existing pipeline configuration.

**Response:**
```json
{
    "message": "Pipeline 'example_pipeline' deleted successfully"
}
```

#### 4. Execute Pipeline
```http
POST /api/v1/pipeline/{name}/execute
```

Execute a previously created pipeline.

**Response:**
```json
{
    "message": "Pipeline 'example_pipeline' executed successfully",
    "storage_location": "data/processed/example_pipeline_20250211_183715.parquet"
}
```

#### 5. Get Pipeline Status
```http
GET /api/v1/pipeline/{name}/status
```

Get the current status of a pipeline.

**Response:**
```json
{
    "status": "completed",
    "progress": 100,
    "error": null
}
```

#### 6. Get Pipeline Statistics
```http
GET /api/v1/pipeline/{name}/stats
```

Get execution statistics for a pipeline.

**Response:**
```json
{
    "total_executions": 10,
    "average_duration": 45.5,
    "success_rate": 0.95,
    "last_execution": "2025-02-11T18:44:13",
    "data_volume": {
        "rows_processed": 10000,
        "storage_size_mb": 25
    }
}
```

#### 7. Get Pipeline Schema
```http
GET /api/v1/pipeline/{name}/schema
```

Get the data schema information for a pipeline.

**Response:**
```json
{
    "columns": [
        {
            "name": "id",
            "type": "int64",
            "nullable": false
        },
        {
            "name": "name",
            "type": "object",
            "nullable": true
        }
    ],
    "row_count": 1000,
    "data_types": {
        "id": "int64",
        "name": "object"
    },
    "sample_data": [
        {
            "id": 1,
            "name": "Example"
        }
    ]
}
```

#### 8. Get Pipeline Data
```http
GET /api/v1/pipeline/{name}/data
```

Retrieve the latest processed data from a pipeline.

**Response:**
```json
[
    {
        "id": 1,
        "name": "Example",
        "processed_value": 42
    }
]
```

## 🏗️ Architecture

### Components

1. **Data Collectors (`app.pipeline.collectors`)**
   - `DataCollector`: Abstract base class for all collectors
   - `APICollector`: Fetches data from REST APIs
   - `CSVCollector`: Reads data from CSV files
   - `WebScraper`: Scrapes data from websites

2. **Data Processors (`app.pipeline.processors`)**
   - `DataCleaner`: Handles data cleaning operations
   - `DataAnalyzer`: Performs statistical analysis
   - `DataTransformer`: Prepares data for ML tasks

3. **Data Storage (`app.pipeline.storage`)**
   - `ParquetStorage`: Stores data in Parquet format
   - `SQLiteStorage`: Stores data in SQLite database

4. **Pipeline Orchestration (`app.pipeline.pipeline`)**
   - `Pipeline`: Main orchestrator class
   - Manages execution flow
   - Handles status tracking
   - Provides error handling

### Data Flow

1. **Collection Phase**
   - Multiple collectors run asynchronously
   - Data is gathered from various sources
   - Results are combined into a single DataFrame

2. **Processing Phase**
   - Data cleaning removes inconsistencies
   - Analysis generates statistical insights
   - Transformations prepare data for ML

3. **Storage Phase**
   - Processed data is versioned
   - Stored in optimized format
   - Easily retrievable for ML tasks

## 🔧 Extending the Framework

### Adding New Collectors

Create a new collector by inheriting from `DataCollector`:

```python
from app.pipeline.collectors import DataCollector

class CustomCollector(DataCollector):
    async def collect(self) -> pd.DataFrame:
        # Implement custom collection logic
        pass
```

### Adding New Processors

Create a new processor by inheriting from `DataProcessor`:

```python
from app.pipeline.processors import DataProcessor

class CustomProcessor(DataProcessor):
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # Implement custom processing logic
        pass
```

### Adding New Storage Backends

Create a new storage backend by inheriting from `DataStorage`:

```python
from app.pipeline.storage import DataStorage

class CustomStorage(DataStorage):
    def save(self, df: pd.DataFrame, name: str) -> str:
        # Implement custom save logic
        pass

    def load(self, name: str) -> pd.DataFrame:
        # Implement custom load logic
        pass
```

## 📊 Example Use Cases

1. **Financial Data Analysis**
   - Collect stock prices from multiple APIs
   - Clean and normalize price data
   - Generate technical indicators
   - Store for ML model training

2. **E-commerce Analytics**
   - Scrape product data from websites
   - Collect sales data from internal API
   - Process and combine datasets
   - Generate pricing insights

3. **IoT Data Processing**
   - Collect sensor data from REST APIs
   - Clean and normalize readings
   - Generate statistical summaries
   - Store for anomaly detection

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- FastAPI framework and community
- Pandas and PyArrow teams
- All contributors and users of this framework
