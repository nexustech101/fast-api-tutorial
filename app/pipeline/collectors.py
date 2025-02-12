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
        """
        Collect data from a REST API endpoint.
        
        Returns:
            pd.DataFrame: Collected data in a DataFrame
            
        Raises:
            HTTPError: If the API request fails
            ValueError: If the response is not valid JSON or cannot be converted to DataFrame
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.endpoint, headers=self.headers) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    if not data:
                        logger.warning(f"No data received from API {self.endpoint}")
                        return pd.DataFrame()
                    
                    if isinstance(data, dict):
                        # Handle single object response
                        data = [data]
                    elif not isinstance(data, list):
                        raise ValueError(f"Unexpected data format from API {self.endpoint}")
                    
                    return pd.DataFrame(data)
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error collecting data from API {self.endpoint}: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from API {self.endpoint}: {str(e)}")
            raise ValueError(f"Invalid JSON response from API: {str(e)}")
        except Exception as e:
            logger.error(f"Error collecting data from API {self.endpoint}: {str(e)}")
            raise

class CSVCollector(DataCollector):
    """Collector for CSV file data sources."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    async def collect(self) -> pd.DataFrame:
        """
        Collect data from a CSV file.
        
        Returns:
            pd.DataFrame: Data from the CSV file
            
        Raises:
            FileNotFoundError: If the CSV file does not exist
            pd.errors.EmptyDataError: If the CSV file is empty
            pd.errors.ParserError: If the CSV file is malformed
        """
        try:
            df = pd.read_csv(self.file_path)
            if df.empty:
                logger.warning(f"CSV file {self.file_path} is empty")
            return df
        except FileNotFoundError:
            logger.error(f"CSV file not found: {self.file_path}")
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"CSV file {self.file_path} is empty")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV file {self.file_path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file {self.file_path}: {str(e)}")
            raise

class WebScraper(DataCollector):
    """Collector for web scraping data sources."""
    
    def __init__(self, url: str, css_selectors: Dict[str, str]):
        """
        Initialize WebScraper.
        
        Args:
            url: URL to scrape
            css_selectors: Dictionary mapping field names to CSS selectors.
                Must include a 'container' selector for the repeating elements.
        """
        self.url = url
        self.css_selectors = css_selectors
        if 'container' not in css_selectors:
            raise ValueError("css_selectors must include a 'container' selector")
    
    async def collect(self) -> pd.DataFrame:
        """
        Collect data by scraping a webpage.
        
        Returns:
            pd.DataFrame: Scraped data in a DataFrame
            
        Raises:
            HTTPError: If the webpage request fails
            ValueError: If the required selectors are not found
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as response:
                    response.raise_for_status()
                    html = await response.text()
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    containers = soup.select(self.css_selectors['container'])
                    
                    if not containers:
                        logger.warning(f"No elements found matching container selector '{self.css_selectors['container']}'")
                        return pd.DataFrame()
                    
                    data = []
                    for item in containers:
                        row = {}
                        for field, selector in self.css_selectors.items():
                            if field != 'container':
                                element = item.select_one(selector)
                                row[field] = element.text.strip() if element else None
                        data.append(row)
                    
                    df = pd.DataFrame(data)
                    if df.empty:
                        logger.warning(f"No data extracted from {self.url}")
                    return df
                    
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error scraping data from {self.url}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error scraping data from {self.url}: {str(e)}")
            raise
