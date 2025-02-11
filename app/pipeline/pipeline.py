"""
Main pipeline orchestrator for data collection and processing.
"""
from typing import Dict, List, Optional, Union
import pandas as pd
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
            dfs = []
            for i, collector in enumerate(self.collectors):
                df = await collector.collect()
                dfs.append(df)
                self._progress = (i + 1) / len(self.collectors) * 33  # First third
            
            # Combine all collected data
            combined_df = pd.concat(dfs, ignore_index=True)
            
            self._status = "processing"
            # Process the data through each processor
            for i, processor in enumerate(self.processors):
                combined_df = processor.process(combined_df)
                self._progress = 33 + (i + 1) / len(self.processors) * 33  # Second third
            
            self._status = "storing"
            # Store the processed data
            storage_location = self.storage.save(combined_df, self.name)
            self._progress = 100
            self._status = "completed"
            
            return storage_location
        
        except Exception as e:
            self._status = "failed"
            self._error = e
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise
    
    def load_latest(self) -> pd.DataFrame:
        """Load the latest processed data for this pipeline."""
        try:
            return self.storage.load(self.name)
        except Exception as e:
            logger.error(f"Error loading latest data: {str(e)}")
            raise
