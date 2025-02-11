"""
Data collectors for various data sources.
"""
from abc import ABC, abstractmethod
import json
from typing import Any, Dict, List, Optional
import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger

class DataCollector(ABC):
    """Base class for all data collectors."""
    
    @abstractmethod
    async def collect(self) -> pd.DataFrame:
        """Collect data from the source."""
        pass

class APICollector(DataCollector):
    """Collector for REST API data sources."""
    
    def __init__(self, endpoint: str, headers: Optional[Dict[str, str]] = None):
        self.endpoint = endpoint
        self.headers = headers or {}
    
    async def collect(self) -> pd.DataFrame:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.endpoint, headers=self.headers) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error collecting data from API {self.endpoint}: {str(e)}")
            raise

class CSVCollector(DataCollector):
    """Collector for CSV file data sources."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    async def collect(self) -> pd.DataFrame:
        try:
            return pd.read_csv(self.file_path)
        except Exception as e:
            logger.error(f"Error reading CSV file {self.file_path}: {str(e)}")
            raise

class WebScraper(DataCollector):
    """Collector for web scraping data sources."""
    
    def __init__(self, url: str, css_selectors: Dict[str, str]):
        self.url = url
        self.css_selectors = css_selectors
    
    async def collect(self) -> pd.DataFrame:
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            data = []
            for item in soup.select(self.css_selectors['container']):
                row = {}
                for field, selector in self.css_selectors.items():
                    if field != 'container':
                        element = item.select_one(selector)
                        row[field] = element.text.strip() if element else None
                data.append(row)
            
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error scraping data from {self.url}: {str(e)}")
            raise
