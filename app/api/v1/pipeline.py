"""
FastAPI routes for data pipeline operations.
"""
from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd

from app.pipeline.pipeline import Pipeline
from app.pipeline.collectors import APICollector, CSVCollector, WebScraper
from app.pipeline.processors import DataCleaner, DataAnalyzer, DataTransformer
from app.pipeline.storage import ParquetStorage

router = APIRouter()

# Store active pipelines in memory
active_pipelines: Dict[str, Pipeline] = {}

class PipelineConfig(BaseModel):
    """Configuration for creating a new pipeline."""
    name: str
    collectors: List[Dict]  # List of collector configurations
    processors: List[Dict]  # List of processor configurations
    storage_dir: str

class PipelineStats(BaseModel):
    """Pipeline statistics response model."""
    total_executions: int
    average_duration: float
    success_rate: float
    last_execution: Optional[str]
    data_volume: Dict[str, int]

class PipelineSchema(BaseModel):
    """Pipeline schema response model."""
    columns: List[Dict[str, str]]
    row_count: int
    data_types: Dict[str, str]
    sample_data: Optional[List[Dict]]

@router.post("/pipeline/create")
async def create_pipeline(config: PipelineConfig):
    """Create a new data pipeline."""
    try:
        # Initialize collectors based on configuration
        collectors = []
        for collector_config in config.collectors:
            if collector_config["type"] == "api":
                collectors.append(APICollector(
                    endpoint=collector_config["endpoint"],
                    headers=collector_config.get("headers")
                ))
            elif collector_config["type"] == "csv":
                collectors.append(CSVCollector(
                    file_path=collector_config["file_path"]
                ))
            elif collector_config["type"] == "web":
                collectors.append(WebScraper(
                    url=collector_config["url"],
                    css_selectors=collector_config["selectors"]
                ))
        
        # Initialize processors
        processors = []
        for processor_config in config.processors:
            if processor_config["type"] == "cleaner":
                processors.append(DataCleaner(config=processor_config["config"]))
            elif processor_config["type"] == "analyzer":
                processors.append(DataAnalyzer())
            elif processor_config["type"] == "transformer":
                processors.append(DataTransformer(
                    transformations=processor_config["transformations"]
                ))
        
        # Initialize storage
        storage = ParquetStorage(base_dir=config.storage_dir)
        
        # Create and store pipeline
        pipeline = Pipeline(
            collectors=collectors,
            processors=processors,
            storage=storage,
            name=config.name
        )
        active_pipelines[config.name] = pipeline
        
        return {"message": f"Pipeline '{config.name}' created successfully"}
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/pipeline/{name}/execute")
async def execute_pipeline(name: str):
    """Execute a pipeline by name."""
    try:
        pipeline = active_pipelines.get(name)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")
        
        storage_location = await pipeline.execute()
        return {
            "message": f"Pipeline '{name}' executed successfully",
            "storage_location": storage_location
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipeline/{name}/status")
async def get_pipeline_status(name: str):
    """Get the current status of a pipeline."""
    pipeline = active_pipelines.get(name)
    if not pipeline:
        raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")
    
    return pipeline.status

@router.get("/pipeline/{name}/data")
async def get_pipeline_data(name: str):
    """Get the latest processed data from a pipeline."""
    try:
        pipeline = active_pipelines.get(name)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")
        
        df = pipeline.load_latest()
        return df.to_dict(orient="records")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/pipeline/{name}/config")
async def update_pipeline_config(name: str, config: PipelineConfig):
    """Update an existing pipeline configuration."""
    try:
        if name not in active_pipelines:
            raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")
        
        # Create new pipeline with updated config
        collectors = []
        for collector_config in config.collectors:
            if collector_config["type"] == "api":
                collectors.append(APICollector(
                    endpoint=collector_config["endpoint"],
                    headers=collector_config.get("headers")
                ))
            elif collector_config["type"] == "csv":
                collectors.append(CSVCollector(
                    file_path=collector_config["file_path"]
                ))
            elif collector_config["type"] == "web":
                collectors.append(WebScraper(
                    url=collector_config["url"],
                    css_selectors=collector_config["selectors"]
                ))
        
        processors = []
        for processor_config in config.processors:
            if processor_config["type"] == "cleaner":
                processors.append(DataCleaner(config=processor_config["config"]))
            elif processor_config["type"] == "analyzer":
                processors.append(DataAnalyzer())
            elif processor_config["type"] == "transformer":
                processors.append(DataTransformer(
                    transformations=processor_config["transformations"]
                ))
        
        storage = ParquetStorage(base_dir=config.storage_dir)
        
        # Update pipeline
        active_pipelines[name] = Pipeline(
            collectors=collectors,
            processors=processors,
            storage=storage,
            name=name
        )
        
        return {"message": f"Pipeline '{name}' configuration updated successfully"}
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/pipeline/{name}")
async def delete_pipeline(name: str):
    """Delete a pipeline configuration."""
    try:
        if name not in active_pipelines:
            raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")
        
        del active_pipelines[name]
        return {"message": f"Pipeline '{name}' deleted successfully"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipeline/{name}/stats", response_model=PipelineStats)
async def get_pipeline_stats(name: str):
    """Get pipeline execution statistics."""
    try:
        pipeline = active_pipelines.get(name)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")
        
        # In a production environment, these stats would be stored in a database
        # Here we're returning mock statistics
        return {
            "total_executions": 10,
            "average_duration": 45.5,
            "success_rate": 0.95,
            "last_execution": "2025-02-11T18:44:13",
            "data_volume": {
                "rows_processed": 10000,
                "storage_size_mb": 25
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipeline/{name}/schema", response_model=PipelineSchema)
async def get_pipeline_schema(name: str):
    """Get pipeline data schema information."""
    try:
        pipeline = active_pipelines.get(name)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline '{name}' not found")
        
        # Load the latest data to analyze schema
        df = pipeline.load_latest()
        
        # Generate schema information
        schema = {
            "columns": [
                {
                    "name": col,
                    "type": str(df[col].dtype),
                    "nullable": df[col].isnull().any()
                }
                for col in df.columns
            ],
            "row_count": len(df),
            "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "sample_data": df.head(5).to_dict(orient="records") if not df.empty else None
        }
        
        return schema
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
