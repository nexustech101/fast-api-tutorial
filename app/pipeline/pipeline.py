"""
Main pipeline orchestrator for data collection and processing.
"""
from typing import Dict, List, Optional, Union
import pandas as pd
from datetime import datetime, timezone
from loguru import logger

from .collectors import DataCollector
from .processors import DataProcessor
from .storage import DataStorage

class Pipeline:
    """
    Main pipeline class that orchestrates the data collection, processing, and storage.
    """
    
    VALID_STATUSES = {"initialized", "collecting", "processing", "storing", "completed", "failed"}
    
    def __init__(
        self,
        collectors: List[DataCollector],
        processors: List[DataProcessor],
        storage: DataStorage,
        name: str
    ):
        """
        Initialize pipeline with collectors, processors, and storage.
        
        Args:
            collectors: List of data collectors
            processors: List of data processors
            storage: Data storage implementation
            name: Name of the pipeline
            
        Raises:
            ValueError: If any arguments are invalid
        """
        if not collectors:
            raise ValueError("At least one collector is required")
        if not isinstance(collectors, (list, tuple)):
            raise ValueError("collectors must be a list or tuple")
        if not all(isinstance(c, DataCollector) for c in collectors):
            raise ValueError("All collectors must be instances of DataCollector")
        
        if not isinstance(processors, (list, tuple)):
            raise ValueError("processors must be a list or tuple")
        if not all(isinstance(p, DataProcessor) for p in processors):
            raise ValueError("All processors must be instances of DataProcessor")
        
        if not isinstance(storage, DataStorage):
            raise ValueError("storage must be an instance of DataStorage")
        
        if not name or not isinstance(name, str):
            raise ValueError("name must be a non-empty string")
        
        self.collectors = collectors
        self.processors = processors
        self.storage = storage
        self.name = name
        self._status = "initialized"
        self._progress = 0
        self._error = None
        self._executions = []
        self._latest_data = None
        self._start_time = None
        self._end_time = None
    
    @property
    def status(self) -> Dict[str, Union[str, int, Optional[str]]]:
        """Get the current status of the pipeline."""
        return {
            "status": self._status,
            "progress": self._progress,
            "error": str(self._error) if self._error else None,
            "start_time": self._start_time.isoformat() if self._start_time else None,
            "end_time": self._end_time.isoformat() if self._end_time else None,
            "duration": (self._end_time - self._start_time).total_seconds() if self._start_time and self._end_time else None
        }
    
    def _set_status(self, status: str, progress: int = None):
        """Update pipeline status."""
        if status not in self.VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}")
        
        self._status = status
        if progress is not None:
            if not isinstance(progress, (int, float)) or progress < 0 or progress > 100:
                raise ValueError("Progress must be a number between 0 and 100")
            self._progress = progress
    
    async def execute(self) -> str:
        """
        Execute the complete pipeline.
        
        Returns:
            str: Location where the processed data was stored
            
        Raises:
            RuntimeError: If pipeline is already running
            Exception: If any step fails
        """
        if self._status in {"collecting", "processing", "storing"}:
            raise RuntimeError("Pipeline is already running")
        
        self._start_time = datetime.now(timezone.utc)
        self._end_time = None
        self._error = None
        
        try:
            # Collect data
            self._set_status("collecting", 0)
            collected_data = []
            progress_per_collector = 50 / len(self.collectors)
            
            for i, collector in enumerate(self.collectors):
                try:
                    logger.info(f"Collecting data from collector {i+1}/{len(self.collectors)}")
                    df = await collector.collect()
                    if df is None or not isinstance(df, pd.DataFrame):
                        raise ValueError(f"Collector {i+1} returned invalid data")
                    collected_data.append(df)
                    self._progress = (i + 1) * progress_per_collector
                except Exception as e:
                    logger.error(f"Error collecting data from collector {i+1}: {str(e)}")
                    raise
            
            # Combine collected data
            if len(collected_data) == 1:
                df = collected_data[0]
            else:
                logger.info(f"Combining data from {len(collected_data)} collectors")
                df = pd.concat(collected_data, ignore_index=True)
            
            if df.empty:
                logger.warning("No data collected from any collector")
            
            # Process data
            self._set_status("processing", 50)
            progress_per_processor = 40 / (len(self.processors) or 1)
            
            for i, processor in enumerate(self.processors):
                try:
                    logger.info(f"Running processor {i+1}/{len(self.processors)}")
                    df = processor.process(df)
                    if df is None or not isinstance(df, pd.DataFrame):
                        raise ValueError(f"Processor {i+1} returned invalid data")
                    self._progress = 50 + (i + 1) * progress_per_processor
                except Exception as e:
                    logger.error(f"Error in processor {i+1}: {str(e)}")
                    raise
            
            # Store data
            self._set_status("storing", 90)
            try:
                logger.info("Storing processed data")
                storage_location = self.storage.save(df, self.name)
                self._latest_data = df
            except Exception as e:
                logger.error(f"Error storing data: {str(e)}")
                raise
            
            # Update execution stats
            self._end_time = datetime.now(timezone.utc)
            self._executions.append({
                "start_time": self._start_time.isoformat(),
                "end_time": self._end_time.isoformat(),
                "duration": (self._end_time - self._start_time).total_seconds(),
                "status": "success",
                "rows_processed": len(df),
                "error": None
            })
            
            self._set_status("completed", 100)
            return storage_location
            
        except Exception as e:
            self._end_time = datetime.now(timezone.utc)
            self._error = str(e)
            self._executions.append({
                "start_time": self._start_time.isoformat(),
                "end_time": self._end_time.isoformat(),
                "duration": (self._end_time - self._start_time).total_seconds(),
                "status": "failed",
                "rows_processed": 0,
                "error": str(e)
            })
            self._set_status("failed", self._progress)
            raise
    
    async def get_data(self) -> pd.DataFrame:
        """
        Get the latest processed data.
        
        Returns:
            pd.DataFrame: Latest processed data
            
        Note:
            If no data in memory, attempts to load from storage
        """
        try:
            if self._latest_data is not None:
                return self._latest_data
            return self.storage.load(self.name)
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    async def get_stats(self) -> Dict:
        """
        Get pipeline execution statistics.
        
        Returns:
            dict: Statistics about pipeline executions
        """
        total_executions = len(self._executions)
        if total_executions == 0:
            return {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "success_rate": 0.0,
                "average_duration": 0.0,
                "total_rows_processed": 0,
                "last_execution": None,
                "last_error": None
            }
        
        successful = sum(1 for e in self._executions if e["status"] == "success")
        total_duration = sum(e["duration"] for e in self._executions)
        total_rows = sum(e["rows_processed"] for e in self._executions)
        
        return {
            "total_executions": total_executions,
            "successful_executions": successful,
            "failed_executions": total_executions - successful,
            "success_rate": successful / total_executions,
            "average_duration": total_duration / total_executions,
            "total_rows_processed": total_rows,
            "last_execution": self._executions[-1],
            "last_error": next((e["error"] for e in reversed(self._executions) if e["error"]), None)
        }
    
    async def get_schema(self) -> Dict:
        """
        Get the schema of the latest data.
        
        Returns:
            dict: Schema information including columns, data types, and sample data
        """
        try:
            df = await self.get_data()
            if df is None or df.empty:
                return {
                    "columns": [],
                    "row_count": 0,
                    "data_types": {},
                    "sample_data": None,
                    "memory_usage": None
                }
            
            return {
                "columns": [
                    {
                        "name": col,
                        "type": str(df[col].dtype),
                        "nullable": df[col].isnull().any(),
                        "unique_values": len(df[col].unique()),
                        "missing_values": df[col].isnull().sum()
                    }
                    for col in df.columns
                ],
                "row_count": len(df),
                "column_count": len(df.columns),
                "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "sample_data": df.head(5).to_dict(orient="records"),
                "memory_usage": {
                    "total": df.memory_usage(deep=True).sum(),
                    "by_column": df.memory_usage(deep=True).to_dict()
                }
            }
        except Exception as e:
            logger.error(f"Error getting schema: {str(e)}")
            raise
