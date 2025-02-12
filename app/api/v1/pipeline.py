"""
FastAPI routes for data pipeline operations.
"""
from datetime import datetime
from typing import Dict, List, Optional, Union
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
import pandas as pd
from loguru import logger

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

class ColumnInfo(BaseModel):
    """Column information in pipeline schema."""
    name: str
    type: str
    nullable: bool
    unique_values: int
    missing_values: int

class PipelineSchema(BaseModel):
    """Pipeline schema response model."""
    columns: List[ColumnInfo]
    row_count: int
    column_count: int
    data_types: Dict[str, str]
    sample_data: Optional[List[Dict]]
    memory_usage: Optional[Dict[str, Union[int, Dict[str, int]]]]

class ExecutionInfo(BaseModel):
    """Pipeline execution information."""
    start_time: str
    end_time: str
    duration: float
    status: str
    rows_processed: int
    error: Optional[str]

class PipelineStats(BaseModel):
    """Pipeline statistics response model."""
    total_executions: int
    successful_executions: int
    failed_executions: int
    success_rate: float
    average_duration: float
    total_rows_processed: int
    last_execution: ExecutionInfo
    last_error: Optional[str]

class PipelineStatus(BaseModel):
    """Pipeline status response model."""
    status: str
    progress: int
    error: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    duration: Optional[float]

@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_pipeline(config: PipelineConfig):
    """Create a new data pipeline."""
    try:
        if config.name in active_pipelines:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Pipeline with name '{config.name}' already exists"
            )
        
        # Create collectors
        collectors = []
        for collector_config in config.collectors:
            if collector_config["type"] == "api":
                collectors.append(APICollector(
                    endpoint=collector_config["endpoint"],
                    headers=collector_config.get("headers", {})
                ))
            elif collector_config["type"] == "csv":
                collectors.append(CSVCollector(
                    file_path=collector_config["file_path"]
                ))
            elif collector_config["type"] == "web":
                collectors.append(WebScraper(
                    url=collector_config["url"],
                    css_selectors=collector_config["css_selectors"]
                ))
            else:
                raise ValueError(f"Unknown collector type: {collector_config['type']}")
        
        # Create processors
        processors = []
        for processor_config in config.processors:
            if processor_config["type"] == "cleaner":
                processors.append(DataCleaner(processor_config["config"]))
            elif processor_config["type"] == "analyzer":
                processors.append(DataAnalyzer())
            elif processor_config["type"] == "transformer":
                processors.append(DataTransformer(processor_config["config"]))
            else:
                raise ValueError(f"Unknown processor type: {processor_config['type']}")
        
        # Create storage
        storage = ParquetStorage(config.storage_dir)
        
        # Create and store pipeline
        pipeline = Pipeline(
            collectors=collectors,
            processors=processors,
            storage=storage,
            name=config.name
        )
        active_pipelines[config.name] = pipeline
        
        return {"message": f"Pipeline '{config.name}' created successfully"}
    
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error creating pipeline: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating pipeline: {str(e)}"
        )

@router.post("/{name}/run")
async def execute_pipeline(name: str):
    """Execute a pipeline by name."""
    try:
        pipeline = active_pipelines.get(name)
        if not pipeline:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{name}' not found"
            )
        
        storage_location = await pipeline.execute()
        return {
            "message": f"Pipeline '{name}' executed successfully",
            "storage_location": storage_location
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error executing pipeline: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error executing pipeline: {str(e)}"
        )

@router.get("/{name}")
async def get_pipeline_status(name: str) -> PipelineStatus:
    """Get the current status of a pipeline."""
    pipeline = active_pipelines.get(name)
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{name}' not found"
        )
    
    return PipelineStatus(**pipeline.status)

@router.get("/{name}/data")
async def get_pipeline_data(name: str):
    """Get the latest processed data from a pipeline."""
    try:
        pipeline = active_pipelines.get(name)
        if not pipeline:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{name}' not found"
            )
        
        data = await pipeline.get_data()
        return data.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error retrieving pipeline data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving pipeline data: {str(e)}"
        )

@router.put("/{name}")
async def update_pipeline_config(name: str, config: PipelineConfig):
    """Update an existing pipeline configuration."""
    try:
        if name != config.name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Pipeline name in URL does not match config"
            )
        
        if name not in active_pipelines:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{name}' not found"
            )
        
        # Delete existing pipeline
        del active_pipelines[name]
        
        # Create new pipeline with updated config
        collectors = []
        for collector_config in config.collectors:
            if collector_config["type"] == "api":
                collectors.append(APICollector(
                    endpoint=collector_config["endpoint"],
                    headers=collector_config.get("headers", {})
                ))
            elif collector_config["type"] == "csv":
                collectors.append(CSVCollector(
                    file_path=collector_config["file_path"]
                ))
            elif collector_config["type"] == "web":
                collectors.append(WebScraper(
                    url=collector_config["url"],
                    css_selectors=collector_config["css_selectors"]
                ))
            else:
                raise ValueError(f"Unknown collector type: {collector_config['type']}")
        
        processors = []
        for processor_config in config.processors:
            if processor_config["type"] == "cleaner":
                processors.append(DataCleaner(processor_config["config"]))
            elif processor_config["type"] == "analyzer":
                processors.append(DataAnalyzer())
            elif processor_config["type"] == "transformer":
                processors.append(DataTransformer(processor_config["config"]))
            else:
                raise ValueError(f"Unknown processor type: {processor_config['type']}")
        
        storage = ParquetStorage(config.storage_dir)
        
        pipeline = Pipeline(
            collectors=collectors,
            processors=processors,
            storage=storage,
            name=config.name
        )
        active_pipelines[config.name] = pipeline
        
        return {"message": f"Pipeline '{name}' updated successfully"}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error updating pipeline: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating pipeline: {str(e)}"
        )

@router.delete("/{name}")
async def delete_pipeline(name: str):
    """Delete a pipeline configuration."""
    try:
        if name not in active_pipelines:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{name}' not found"
            )
        
        del active_pipelines[name]
        return {"message": f"Pipeline '{name}' deleted successfully"}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error deleting pipeline: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting pipeline: {str(e)}"
        )

@router.get("/{name}/stats")
async def get_pipeline_stats(name: str) -> PipelineStats:
    """Get pipeline execution statistics."""
    try:
        pipeline = active_pipelines.get(name)
        if not pipeline:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{name}' not found"
            )
        
        stats = await pipeline.get_stats()
        return PipelineStats(**stats)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting pipeline stats: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting pipeline stats: {str(e)}"
        )

@router.get("/{name}/schema")
async def get_pipeline_schema(name: str) -> PipelineSchema:
    """Get pipeline data schema information."""
    try:
        pipeline = active_pipelines.get(name)
        if not pipeline:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{name}' not found"
            )
        
        schema = await pipeline.get_schema()
        return PipelineSchema(**schema)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting pipeline schema: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting pipeline schema: {str(e)}"
        )
