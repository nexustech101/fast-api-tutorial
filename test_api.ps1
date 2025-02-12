# Health Check
Write-Host "Testing Health Check endpoint..."
Invoke-WebRequest -Method GET -Uri "http://localhost:8000/health" | Select-Object -ExpandProperty Content

# Create Pipeline
Write-Host "`nCreating pipeline..."
$createBody = @{
    name = "users-pipeline"
    collectors = @(
        @{
            type = "api"
            endpoint = "https://jsonplaceholder.typicode.com/users"
            headers = @{}
        }
    )
    processors = @(
        @{
            type = "cleaner"
            config = @{
                drop_columns = @("address", "company")
                fill_na = @{
                    email = "no-email"
                    phone = "no-phone"
                }
            }
        }
        @{
            type = "transformer"
            config = @{
                rename_columns = @{
                    name = "full_name"
                    username = "user_handle"
                }
            }
        }
    )
    storage_dir = "data/processed"
} | ConvertTo-Json -Depth 10

Write-Host "Sending request to create pipeline..."
try {
    $response = Invoke-WebRequest -Method POST -Uri "http://localhost:8000/api/v1/pipeline" `
        -Headers @{"Content-Type"="application/json"} `
        -Body $createBody
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Error creating pipeline:" $_.Exception.Message
    Write-Host "Response:" $_.Exception.Response
}

# Wait a bit for pipeline creation
Start-Sleep -Seconds 2

# Execute Pipeline
Write-Host "`nExecuting pipeline..."
try {
    $response = Invoke-WebRequest -Method POST -Uri "http://localhost:8000/api/v1/pipeline/users-pipeline/run"
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Error executing pipeline:" $_.Exception.Message
}

# Get Pipeline Status
Write-Host "`nChecking pipeline status..."
try {
    $response = Invoke-WebRequest -Method GET -Uri "http://localhost:8000/api/v1/pipeline/users-pipeline"
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Error getting status:" $_.Exception.Message
}

# Get Pipeline Data
Write-Host "`nRetrieving pipeline data..."
try {
    $response = Invoke-WebRequest -Method GET -Uri "http://localhost:8000/api/v1/pipeline/users-pipeline/data"
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Error getting data:" $_.Exception.Message
}

# Get Pipeline Schema
Write-Host "`nRetrieving pipeline schema..."
try {
    $response = Invoke-WebRequest -Method GET -Uri "http://localhost:8000/api/v1/pipeline/users-pipeline/schema"
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Error getting schema:" $_.Exception.Message
}

# Get Pipeline Stats
Write-Host "`nRetrieving pipeline statistics..."
try {
    $response = Invoke-WebRequest -Method GET -Uri "http://localhost:8000/api/v1/pipeline/users-pipeline/stats"
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Error getting stats:" $_.Exception.Message
}

# Update Pipeline
Write-Host "`nUpdating pipeline configuration..."
$updateBody = @{
    name = "users-pipeline"
    collectors = @(
        @{
            type = "api"
            endpoint = "https://jsonplaceholder.typicode.com/users"
            headers = @{}
        }
    )
    processors = @(
        @{
            type = "cleaner"
            config = @{
                drop_columns = @("address", "company", "website")
                fill_na = @{
                    email = "no-email"
                    phone = "no-phone"
                }
            }
        }
    )
    storage_dir = "data/processed"
} | ConvertTo-Json -Depth 10

try {
    $response = Invoke-WebRequest -Method PUT -Uri "http://localhost:8000/api/v1/pipeline/users-pipeline" `
        -Headers @{"Content-Type"="application/json"} `
        -Body $updateBody
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Error updating pipeline:" $_.Exception.Message
}

# Delete Pipeline
Write-Host "`nDeleting pipeline..."
try {
    $response = Invoke-WebRequest -Method DELETE -Uri "http://localhost:8000/api/v1/pipeline/users-pipeline"
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Error deleting pipeline:" $_.Exception.Message
}
