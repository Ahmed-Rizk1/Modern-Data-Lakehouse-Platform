<#
.SYNOPSIS
    Native Windows PowerShell Management Script for Modern Data Lakehouse Platform
    (Prefect + Apache Iceberg + Trino + MinIO + PostgreSQL + Apache Superset)

.DESCRIPTION
    Provides complete lifecycle automation for the lakehouse stack on Windows environments
    without requiring WSL or Git Bash. Supports start, stop, restart, status, reset, build, and test.

.EXAMPLE
    .\lakehouse.ps1 start
    .\lakehouse.ps1 status
    .\lakehouse.ps1 stop
#>

[CmdletBinding()]
param (
    [Parameter(Position = 0, Mandatory = $false)]
    [ValidateSet("start", "stop", "restart", "status", "reset", "build", "test")]
    [string]$Command = "start"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

$LocalEnvFile = Join-Path $ScriptDir ".env"
$VolumesFolder = Join-Path $ScriptDir "volumes"
$InitializedMarker = Join-Path $VolumesFolder ".initialized"

function Prepare-Environment {
    Write-Host "[*] Loading environment configuration from $LocalEnvFile..." -ForegroundColor Cyan

    if (-not (Test-Path $LocalEnvFile)) {
        if (Test-Path (Join-Path $ScriptDir ".env.example")) {
            Write-Host "[!] .env not found. Auto-generating from .env.example..." -ForegroundColor Yellow
            Copy-Item (Join-Path $ScriptDir ".env.example") $LocalEnvFile
        } else {
            throw "Neither .env nor .env.example was found in $ScriptDir."
        }
    }

    Get-Content $LocalEnvFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line -split "=", 2
            if ($parts.Length -eq 2) {
                $key = $parts[0].Trim()
                $val = $parts[1].Trim().Trim('"').Trim("'")
                [System.Environment]::SetEnvironmentVariable($key, $val, "Process")
            }
        }
    }

    # Set Catalog defaults
    $catalogSvc = "iceberg"
    $catalogUri = "http://iceberg:8181"
    if ($env:ICEBERG_CATALOG -eq "NESSIE" -or $env:ICEBERG_CATALOG -eq "nessie") {
        $catalogSvc = "nessie"
        $catalogUri = "http://nessie:19120/api"
    }
    [System.Environment]::SetEnvironmentVariable("ICEBERG_CATALOG_SVC", $catalogSvc, "Process")
    [System.Environment]::SetEnvironmentVariable("ICEBERG_CATALOG_URI", $catalogUri, "Process")
}

function Expand-Template {
    param (
        [string]$TemplatePath,
        [string]$DestinationPath
    )

    if (-not (Test-Path $TemplatePath)) {
        throw "Template not found: $TemplatePath"
    }

    $content = Get-Content $TemplatePath -Raw
    $pattern = '\$\{([A-Za-z0-9_]+)\}'

    $result = [System.Text.RegularExpressions.Regex]::Replace($content, $pattern, {
        param($match)
        $varName = $match.Groups[1].Value
        $val = [System.Environment]::GetEnvironmentVariable($varName, "Process")
        if ($null -ne $val) { return $val } else { return $match.Value }
    })

    $destDir = Split-Path -Parent $DestinationPath
    if (-not (Test-Path $destDir)) {
        New-Item -ItemType Directory -Path $destDir -Force | Out-Null
    }

    [System.IO.File]::WriteAllText($DestinationPath, $result)
}

function Init-Config {
    Write-Host "[*] Generating volume configuration files from templates..." -ForegroundColor Cyan

    $configVol = Join-Path $VolumesFolder "config"
    if (-not (Test-Path $configVol)) {
        New-Item -ItemType Directory -Path $configVol -Force | Out-Null
    }

    # PyIceberg config
    $pyicebergTemplate = Join-Path $ScriptDir "config\pyiceberg.template.yaml"
    $pyicebergDest = Join-Path $configVol "pyiceberg.yaml"
    Expand-Template -TemplatePath $pyicebergTemplate -DestinationPath $pyicebergDest

    # Trino catalog config
    $trinoTemplate = Join-Path $ScriptDir "config\trino-catalog.template.properties"
    $trinoDest = Join-Path $configVol "trino-catalog.properties"
    Expand-Template -TemplatePath $trinoTemplate -DestinationPath $trinoDest

    # Prefect pipeline deployment config
    $prefectTemplate = Join-Path $ScriptDir "data\pipelines\prefect.template.yaml"
    $prefectDest = Join-Path $ScriptDir "data\pipelines\prefect.yaml"
    Expand-Template -TemplatePath $prefectTemplate -DestinationPath $prefectDest
}

function Wait-Until-Postgres-Ready {
    Write-Host "[*] Waiting for PostgreSQL to be healthy and accepting connections..." -ForegroundColor Cyan
    $iterations = 0
    $maxIterations = 40
    $ready = $false

    while ($iterations -lt $maxIterations) {
        $output = docker compose exec -T postgres pg_isready -U $env:POSTGRES_USER 2>&1
        if ($LASTEXITCODE -eq 0 -and $output -match "accepting connections") {
            $ready = $true
            break
        }
        Start-Sleep -Seconds 3
        $iterations++
    }

    if (-not $ready) {
        throw "Timeout waiting for PostgreSQL service to become ready."
    }
    Write-Host "[+] PostgreSQL is ready." -ForegroundColor Green
}

function Download-SampleDataset {
    $datasetDir = Join-Path $ScriptDir "data\datasets"
    if (-not (Test-Path $datasetDir)) {
        New-Item -ItemType Directory -Path $datasetDir -Force | Out-Null
    }

    $parquetFile = Join-Path $datasetDir "yellow_tripdata_2024-01.parquet"
    if (-not (Test-Path $parquetFile)) {
        Write-Host "[*] Sample dataset missing. Downloading NYC Yellow Taxi Parquet (~50MB)..." -ForegroundColor Yellow
        $url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        Invoke-WebRequest -Uri $url -OutFile $parquetFile
        Write-Host "[+] Dataset downloaded: $parquetFile" -ForegroundColor Green
    } else {
        Write-Host "[+] Sample dataset already exists at $parquetFile." -ForegroundColor Green
    }
}

function Build-CustomImages {
    Write-Host "[*] Pulling base container images..." -ForegroundColor Cyan
    docker compose pull iceberg traefik minio trino postgres prefect-server

    # Superset Image
    $supersetImage = $env:SUPERSET_IMAGE
    $hasSuperset = docker images -q $supersetImage
    if (-not $hasSuperset) {
        Write-Host "[*] Building Superset image: $supersetImage..." -ForegroundColor Cyan
        docker build -f "build/superset/Dockerfile" -t $supersetImage `
            --build-arg "SUPERSET_BASE_IMAGE=$env:SUPERSET_BASE_IMAGE" "build/superset"
    }

    # Ingestion Image
    $ingestionImage = $env:INGESTION_IMAGE
    $hasIngestion = docker images -q $ingestionImage
    if (-not $hasIngestion) {
        Write-Host "[*] Building Ingestion / Worker image: $ingestionImage..." -ForegroundColor Cyan
        docker build -f "build/prefect/Dockerfile" -t $ingestionImage `
            --build-arg "INGESTION_BASE_IMAGE=$env:PREFECT_IMAGE" .
    }

    # Jupyter Image
    $jupyterImage = $env:JUPYTER_IMAGE
    $hasJupyter = docker images -q $jupyterImage
    if (-not $hasJupyter) {
        Write-Host "[*] Building Jupyter image: $jupyterImage..." -ForegroundColor Cyan
        docker build -f "build/jupyter/Dockerfile" -t $jupyterImage `
            --build-arg "JUPYTER_BASE_IMAGE=$env:INGESTION_IMAGE" .
    }
}

function Start-Databases {
    Write-Host "[*] Starting core datastores (PostgreSQL + MinIO)..." -ForegroundColor Cyan
    docker compose up -d postgres minio
    Wait-Until-Postgres-Ready
}

function Init-Environment {
    Write-Host "[*] Provisioning MinIO storage bucket ($env:WAREHOUSE_BUCKET_NAME)..." -ForegroundColor Cyan
    docker compose exec -T minio /bin/bash -c "
      until (mc config host add minio http://minio:9000 $env:MINIO_ROOT_USER $env:MINIO_ROOT_PASSWORD) do echo '...waiting for minio...' && sleep 1; done;
      mc mb minio/$env:WAREHOUSE_BUCKET_NAME || true;
      mc policy set public minio/$env:WAREHOUSE_BUCKET_NAME;
    "

    Write-Host "[*] Provisioning PostgreSQL databases (superset, $env:ICEBERG_CATALOG_DB, $env:PREFECT_DATABASE)..." -ForegroundColor Cyan
    docker compose exec -T postgres /bin/bash -c "
PGPASSWORD=$env:POSTGRES_PASSWORD psql --host postgres --username $env:POSTGRES_USER <<-EOSQL
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'superset') THEN
        CREATE ROLE superset SUPERUSER LOGIN PASSWORD 'superset';
      END IF;
    END
    \$\$;
    SELECT 'CREATE DATABASE superset' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec
    GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
    SELECT 'CREATE DATABASE $env:ICEBERG_CATALOG_DB' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$env:ICEBERG_CATALOG_DB')\gexec
    GRANT ALL PRIVILEGES ON DATABASE $env:ICEBERG_CATALOG_DB TO $env:POSTGRES_USER;
    SELECT 'CREATE DATABASE $env:PREFECT_DATABASE' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$env:PREFECT_DATABASE')\gexec
    GRANT ALL PRIVILEGES ON DATABASE $env:PREFECT_DATABASE TO $env:POSTGRES_USER;
EOSQL
    "

    Write-Host "[*] Launching catalog and analytics services..." -ForegroundColor Cyan
    docker compose up -d superset trino $env:ICEBERG_CATALOG_SVC prefect-server

    Write-Host "[*] Configuring Prefect work pool ($env:PREFECT_WORKER_POOL)..." -ForegroundColor Cyan
    Start-Sleep -Seconds 6
    docker compose exec -T prefect-server /bin/bash -c "
        cd $env:LAKEHOUSE_DATA_PATH/pipelines;
        prefect --no-prompt work-pool create $env:PREFECT_WORKER_POOL --type process || true;
    "

    Write-Host "[*] Starting Prefect worker & deploying pipelines..." -ForegroundColor Cyan
    docker compose up -d prefect-worker
    Start-Sleep -Seconds 5
    docker compose exec -T prefect-worker /bin/bash -c "
        cd $env:LAKEHOUSE_DATA_PATH/pipelines;
        prefect --no-prompt deploy --all || true;
    "

    Write-Host "[*] Registering Trino database connection inside Superset..." -ForegroundColor Cyan
    docker compose exec -T prefect-worker /bin/bash -c "
        cd $env:LAKEHOUSE_DATA_PATH/scripts && python setup_superset_db.py;
    "
}

function Start-Lakehouse {
    Build-CustomImages

    if (-not (Test-Path $VolumesFolder) -or -not (Test-Path $InitializedMarker)) {
        Write-Host "[*] First-time setup detected. Initializing lakehouse environment..." -ForegroundColor Yellow
        if (Test-Path $VolumesFolder) {
            Remove-Item -Recurse -Force $VolumesFolder
        }
        New-Item -ItemType Directory -Path (Join-Path $VolumesFolder "config") -Force | Out-Null
        Init-Config
        Start-Databases
        Init-Environment
        Download-SampleDataset
        New-Item -ItemType File -Path $InitializedMarker -Force | Out-Null
    } else {
        Init-Config
        Start-Databases
    }

    Write-Host "[*] Bringing up all Lakehouse services with Docker Compose v2..." -ForegroundColor Cyan
    docker compose up -d traefik minio $env:ICEBERG_CATALOG_SVC trino superset jupyter prefect-server prefect-worker
}

function Stop-Lakehouse {
    Write-Host "[*] Gracefully stopping all Lakehouse containers..." -ForegroundColor Yellow
    docker compose down
}

function Show-Status {
    Write-Host "`n========================================================" -ForegroundColor Green
    Write-Host "             Modern Data Lakehouse Status" -ForegroundColor Green
    Write-Host "========================================================" -ForegroundColor Green
    docker compose ps
    Write-Host "========================================================`n" -ForegroundColor Green
}

function Show-Urls {
    Write-Host "--------------------------------------------------------" -ForegroundColor Cyan
    Write-Host "             LAKEHOUSE LOCAL ACCESS URLS" -ForegroundColor Cyan
    Write-Host "--------------------------------------------------------" -ForegroundColor Cyan
    Write-Host "  * Prefect Orchestrator:   http://localhost:4200 (or http://prefect.lakehouse.localhost)"
    Write-Host "  * Apache Superset BI:     http://localhost:8088 (admin / admin)"
    Write-Host "  * Jupyter Lab Analytics:  http://localhost:5006 (tokenless)"
    Write-Host "  * MinIO Object Storage:   http://localhost:9001 (admin / password)"
    Write-Host "  * Trino Query Endpoint:   http://localhost:8080"
    Write-Host "  * Iceberg REST Catalog:   http://localhost:8181/v1/config"
    Write-Host "--------------------------------------------------------`n" -ForegroundColor Cyan
}

function Reset-Lakehouse {
    Write-Host "[!] Warning: Resetting Lakehouse will erase all volumes, Iceberg metadata, and tables." -ForegroundColor Red
    Stop-Lakehouse
    if (Test-Path $VolumesFolder) {
        Remove-Item -Recurse -Force $VolumesFolder
        Write-Host "[+] Lakehouse volumes removed." -ForegroundColor Green
    }
}

# --- Main Entry Point ---
Prepare-Environment

switch ($Command) {
    "start" {
        Start-Lakehouse
        Show-Status
        Show-Urls
    }
    "stop" {
        Stop-Lakehouse
    }
    "restart" {
        Stop-Lakehouse
        Start-Lakehouse
        Show-Status
        Show-Urls
    }
    "status" {
        Show-Status
        Show-Urls
    }
    "reset" {
        Reset-Lakehouse
    }
    "build" {
        Build-CustomImages
    }
    "test" {
        Write-Host "[*] Verifying service connectivity..." -ForegroundColor Cyan
        docker compose ps
    }
}

