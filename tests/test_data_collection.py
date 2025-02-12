"""
Unit tests for data collection components.
"""
import pytest
import aiohttp
import pandas as pd
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup

from app.pipeline.collectors import APICollector, CSVCollector, WebScraper

@pytest.mark.asyncio
async def test_api_collector_successful_fetch(mock_api_response):
    """Test successful data collection from API."""
    endpoint = "https://api.example.com/data"
    headers = {"Authorization": "Bearer test-token"}
    
    # Mock the aiohttp ClientSession
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = mock_api_response
    mock_response.__aenter__.return_value = mock_response
    
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    mock_session.__aenter__.return_value = mock_session
    
    with patch('aiohttp.ClientSession', return_value=mock_session):
        collector = APICollector(endpoint=endpoint, headers=headers)
        df = await collector.collect()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(mock_api_response)
    assert all(col in df.columns for col in ['id', 'name', 'price'])

@pytest.mark.asyncio
async def test_api_collector_failed_fetch():
    """Test API collector error handling."""
    endpoint = "https://api.example.com/data"
    
    # Mock a failed response
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = aiohttp.ClientError("API Error")
    mock_response.__aenter__.return_value = mock_response
    
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    mock_session.__aenter__.return_value = mock_session
    
    with patch('aiohttp.ClientSession', return_value=mock_session):
        collector = APICollector(endpoint=endpoint)
        with pytest.raises(aiohttp.ClientError):
            await collector.collect()

def test_csv_collector_successful_read(csv_file, sample_data):
    """Test successful data collection from CSV file."""
    collector = CSVCollector(file_path=csv_file)
    df = collector.collect()
    
    assert isinstance(df, pd.DataFrame)
    pd.testing.assert_frame_equal(df, sample_data)

def test_csv_collector_file_not_found():
    """Test CSV collector error handling for missing file."""
    collector = CSVCollector(file_path="nonexistent.csv")
    with pytest.raises(FileNotFoundError):
        collector.collect()

def test_web_scraper_successful_scrape(mock_html_content):
    """Test successful web scraping."""
    url = "https://example.com"
    css_selectors = {
        'container': '.product',
        'name': 'h2',
        'price': '.price'
    }
    
    # Mock the requests.get response
    mock_response = MagicMock()
    mock_response.text = mock_html_content
    mock_response.raise_for_status = MagicMock()
    
    with patch('requests.get', return_value=mock_response):
        scraper = WebScraper(url=url, css_selectors=css_selectors)
        df = scraper.collect()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # Two products in mock HTML
    assert all(col in df.columns for col in ['name', 'price'])
    assert df['name'].iloc[0] == 'Product 1'
    assert df['price'].iloc[0] == '$99.99'

def test_web_scraper_failed_request():
    """Test web scraper error handling for failed requests."""
    url = "https://example.com"
    css_selectors = {'container': '.product'}
    
    # Mock a failed response
    with patch('requests.get', side_effect=Exception("Connection error")):
        scraper = WebScraper(url=url, css_selectors=css_selectors)
        with pytest.raises(Exception):
            scraper.collect()

def test_web_scraper_invalid_selectors(mock_html_content):
    """Test web scraper error handling for invalid CSS selectors."""
    url = "https://example.com"
    css_selectors = {
        'container': '.nonexistent',
        'name': 'h2',
        'price': '.price'
    }
    
    mock_response = MagicMock()
    mock_response.text = mock_html_content
    mock_response.raise_for_status = MagicMock()
    
    with patch('requests.get', return_value=mock_response):
        scraper = WebScraper(url=url, css_selectors=css_selectors)
        df = scraper.collect()
        assert len(df) == 0  # No matches for invalid selector
