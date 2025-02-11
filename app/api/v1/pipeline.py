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
