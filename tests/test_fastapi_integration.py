"""
Unit tests for FastAPI integration.
"""
import pytest
from fastapi import status
import json
from unittest.mock import patch, MagicMock

def test_create_pipeline(client, pipeline_config):
    """Test creating a new pipeline."""
    response = client.post(
        "/api/v1/pipelines/",
        json=pipeline_config
    )
    
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["name"] == pipeline_config["name"]
    assert "id" in data

def test_create_pipeline_invalid_config(client):
    """Test error handling for invalid pipeline configuration."""
    invalid_config = {
        "name": "invalid_pipeline",
        "collectors": []  # Empty collectors list is invalid
    }
    
    response = client.post(
        "/api/v1/pipelines/",
        json=invalid_config
    )
    
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

def test_get_pipeline(client, pipeline_config):
    """Test retrieving a pipeline by ID."""
    # First create a pipeline
    create_response = client.post(
        "/api/v1/pipelines/",
        json=pipeline_config
    )
    pipeline_id = create_response.json()["id"]
    
    # Then retrieve it
    response = client.get(f"/api/v1/pipelines/{pipeline_id}")
    
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == pipeline_config["name"]
    assert data["id"] == pipeline_id

def test_get_nonexistent_pipeline(client):
    """Test error handling for retrieving non-existent pipeline."""
    response = client.get("/api/v1/pipelines/999999")
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_list_pipelines(client, pipeline_config):
    """Test listing all pipelines."""
    # Create a few pipelines
    for i in range(3):
        config = pipeline_config.copy()
        config["name"] = f"pipeline_{i}"
        client.post("/api/v1/pipelines/", json=config)
    
    response = client.get("/api/v1/pipelines/")
    
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) >= 3
    assert all(isinstance(p["id"], int) for p in data)

@pytest.mark.asyncio
async def test_execute_pipeline(client, pipeline_config, mock_api_response):
    """Test executing a pipeline."""
    # Create a pipeline
    create_response = client.post(
        "/api/v1/pipelines/",
        json=pipeline_config
    )
    pipeline_id = create_response.json()["id"]
    
    # Mock the API collector
    mock_collector = MagicMock()
    mock_collector.collect.return_value = pd.DataFrame(mock_api_response)
    
    with patch('app.pipeline.collectors.APICollector', return_value=mock_collector):
        response = client.post(f"/api/v1/pipelines/{pipeline_id}/execute")
        
        assert response.status_code == status.HTTP_202_ACCEPTED
        data = response.json()
        assert "execution_id" in data

def test_get_pipeline_status(client, pipeline_config):
    """Test retrieving pipeline execution status."""
    # Create and execute a pipeline
    create_response = client.post(
        "/api/v1/pipelines/",
        json=pipeline_config
    )
    pipeline_id = create_response.json()["id"]
    
    execute_response = client.post(f"/api/v1/pipelines/{pipeline_id}/execute")
    execution_id = execute_response.json()["execution_id"]
    
    # Get status
    response = client.get(f"/api/v1/pipelines/{pipeline_id}/executions/{execution_id}")
    
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "status" in data
    assert data["status"] in ["pending", "running", "completed", "failed"]

def test_get_pipeline_results(client, pipeline_config, sample_data):
    """Test retrieving pipeline execution results."""
    # Create and execute a pipeline
    create_response = client.post(
        "/api/v1/pipelines/",
        json=pipeline_config
    )
    pipeline_id = create_response.json()["id"]
    
    execute_response = client.post(f"/api/v1/pipelines/{pipeline_id}/execute")
    execution_id = execute_response.json()["execution_id"]
    
    # Mock successful execution and store results
    with patch('app.pipeline.storage.ParquetStorage') as mock_storage:
        mock_storage.return_value.load.return_value = sample_data
        
        response = client.get(
            f"/api/v1/pipelines/{pipeline_id}/executions/{execution_id}/results"
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == len(sample_data)

def test_update_pipeline_config(client, pipeline_config):
    """Test updating pipeline configuration."""
    # Create a pipeline
    create_response = client.post(
        "/api/v1/pipelines/",
        json=pipeline_config
    )
    pipeline_id = create_response.json()["id"]
    
    # Update configuration
    updated_config = pipeline_config.copy()
    updated_config["name"] = "updated_pipeline"
    
    response = client.put(
        f"/api/v1/pipelines/{pipeline_id}",
        json=updated_config
    )
    
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == "updated_pipeline"

def test_delete_pipeline(client, pipeline_config):
    """Test deleting a pipeline."""
    # Create a pipeline
    create_response = client.post(
        "/api/v1/pipelines/",
        json=pipeline_config
    )
    pipeline_id = create_response.json()["id"]
    
    # Delete the pipeline
    response = client.delete(f"/api/v1/pipelines/{pipeline_id}")
    assert response.status_code == status.HTTP_204_NO_CONTENT
    
    # Verify pipeline is deleted
    get_response = client.get(f"/api/v1/pipelines/{pipeline_id}")
    assert get_response.status_code == status.HTTP_404_NOT_FOUND
