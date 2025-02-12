"""
Main pipeline orchestrator for data collection and processing.
"""
from typing import Dict, List, Optional, Union
import pandas as pd
from datetime import datetime
from loguru import logger

from .collectors import DataCollector
from .processors import DataProcessor
from .storage import DataStorage

class Pipeline:
    """
    Main pipeline class that orchestrates the data collection, processing, and storage.
    """
    
    def __init__(
        self,
        collectors: List[DataCollector],
        processors: List[DataProcessor],
        storage: DataStorage,
        name: str
    ):
        self.collectors = collectors
        self.processors = processors
        self.storage = storage
        self.name = name
        self._status = "initialized"
        self._progress = 0
        self._error = None
        self._executions = []
        self._latest_data = None
    
    @property
    def status(self) -> Dict[str, Union[str, int, Optional[str]]]:
        """Get the current status of the pipeline."""
        return {
            "status": self._status,
            "progress": self._progress,
            "error": str(self._error) if self._error else None
        }
    
    async def execute(self) -> str:
        """
        Execute the complete pipeline.
        
        Returns:
            str: Location where the processed data was stored
        """
        try:
            self._status = "collecting"
            self._progress = 0
            
            # Collect data from all sources
            collected_data = []
            for collector in self.collectors:
                try:
                    df = await collector.collect()
                    collected_data.append(df)
                except Exception as e:
                    logger.error(f"Error collecting data: {str(e)}")
                    raise
            
            # Combine all collected data
            if len(collected_data) == 1:
                df = collected_data[0]
            else:
                df = pd.concat(collected_data, ignore_index=True)
            
            # Process the data
            self._status = "processing"
            self._progress = 50
            
            for processor in self.processors:
                try:
                    df = processor.process(df)
                except Exception as e:
                    logger.error(f"Error processing data: {str(e)}")
                    raise
            
            # Store the processed data
            self._status = "storing"
            self._progress = 90
            
            try:
                storage_location = self.storage.save(df, self.name)
                self._latest_data = df
            except Exception as e:
                logger.error(f"Error storing data: {str(e)}")
                raise
            
            # Update execution stats
            self._executions.append({
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "rows_processed": len(df)
            })
            
            self._status = "completed"
            self._progress = 100
            self._error = None
            
            return storage_location
            
        except Exception as e:
            self._status = "failed"
            self._error = str(e)
            self._executions.append({
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "error": str(e)
            })
            raise
    
    async def get_data(self) -> pd.DataFrame:
        """Get the latest processed data."""
        if self._latest_data is not None:
            return self._latest_data
        return self.storage.load(self.name)
    
    async def get_stats(self) -> Dict:
        """Get pipeline execution statistics."""
        total_executions = len(self._executions)
        if total_executions == 0:
            return {
                "total_executions": 0,
                "average_duration": 0.0,
                "success_rate": 0.0,
                "last_execution": None,
                "data_volume": {"rows_processed": 0}
            }
        
        successful = sum(1 for e in self._executions if e["status"] == "success")
        latest_data = await self.get_data()
        
        return {
            "total_executions": total_executions,
            "average_duration": 0.0,  # Would need to track duration in executions
            "success_rate": successful / total_executions,
            "last_execution": self._executions[-1]["timestamp"],
            "data_volume": {
                "rows_processed": len(latest_data) if latest_data is not None else 0
            }
        }
    
    async def get_schema(self) -> Dict:
        """Get the schema of the latest data."""
        df = await self.get_data()
        if df is None or df.empty:
            return {
                "columns": [],
                "row_count": 0,
                "data_types": {},
                "sample_data": None
            }
        
        return {
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
            "sample_data": df.head(5).to_dict(orient="records")
        }
