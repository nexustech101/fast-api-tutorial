#!/bin/bash

# Health Check
echo "Testing Health Check endpoint..."
curl -X GET http://localhost:8000/health

# Create Pipeline
echo -e "\n\nCreating pipeline..."
curl -X POST http://localhost:8000/api/v1/pipeline/create \
  -H "Content-Type: application/json" \
  -d '{
    "name": "users-pipeline",
    "collectors": [
        {
            "type": "api",
            "endpoint": "https://jsonplaceholder.typicode.com/users",
            "headers": {}
        }
    ],
    "processors": [
        {
            "type": "cleaner",
            "config": {
                "drop_columns": ["address", "company"],
                "fill_na": {"email": "no-email", "phone": "no-phone"}
            }
        },
        {
            "type": "transformer",
            "config": {
                "rename_columns": {
                    "name": "full_name",
                    "username": "user_handle"
                }
            }
        }
    ],
    "storage_dir": "data/processed"
}'

# Wait a bit for pipeline creation
sleep 2

# Execute Pipeline
echo -e "\n\nExecuting pipeline..."
curl -X POST http://localhost:8000/api/v1/pipeline/users-pipeline/execute

# Get Pipeline Status
echo -e "\n\nChecking pipeline status..."
curl -X GET http://localhost:8000/api/v1/pipeline/users-pipeline/status

# Get Pipeline Data
echo -e "\n\nRetrieving pipeline data..."
curl -X GET http://localhost:8000/api/v1/pipeline/users-pipeline/data

# Get Pipeline Schema
echo -e "\n\nRetrieving pipeline schema..."
curl -X GET http://localhost:8000/api/v1/pipeline/users-pipeline/schema

# Get Pipeline Stats
echo -e "\n\nRetrieving pipeline statistics..."
curl -X GET http://localhost:8000/api/v1/pipeline/users-pipeline/stats

# Update Pipeline
echo -e "\n\nUpdating pipeline configuration..."
curl -X PUT http://localhost:8000/api/v1/pipeline/users-pipeline/update \
  -H "Content-Type: application/json" \
  -d '{
    "name": "users-pipeline",
    "collectors": [
        {
            "type": "api",
            "endpoint": "https://jsonplaceholder.typicode.com/users",
            "headers": {}
        }
    ],
    "processors": [
        {
            "type": "cleaner",
            "config": {
                "drop_columns": ["address", "company", "website"],
                "fill_na": {"email": "no-email", "phone": "no-phone"}
            }
        }
    ],
    "storage_dir": "data/processed"
}'

# Delete Pipeline
echo -e "\n\nDeleting pipeline..."
curl -X DELETE http://localhost:8000/api/v1/pipeline/users-pipeline/delete
