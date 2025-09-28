# Modern Data Warehouse Project - Setup Guide
# ==========================================

# Environment Setup Script for Windows PowerShell

Write-Host "🏢 Modern Data Warehouse - Environment Setup" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# Check Python installation
Write-Host "`nChecking Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✓ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Python not found. Please install Python 3.8+ from https://python.org" -ForegroundColor Red
    exit 1
}

# Check pip
Write-Host "`nChecking pip..." -ForegroundColor Yellow
try {
    $pipVersion = pip --version 2>&1
    Write-Host "✓ Pip found: $pipVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Pip not found. Please ensure pip is installed with Python" -ForegroundColor Red
    exit 1
}

# Create virtual environment
Write-Host "`nCreating virtual environment..." -ForegroundColor Yellow
if (Test-Path "venv") {
    Write-Host "✓ Virtual environment already exists" -ForegroundColor Green
} else {
    python -m venv venv
    Write-Host "✓ Virtual environment created" -ForegroundColor Green
}

# Activate virtual environment
Write-Host "`nActivating virtual environment..." -ForegroundColor Yellow
& "venv\Scripts\Activate.ps1"
Write-Host "✓ Virtual environment activated" -ForegroundColor Green

# Upgrade pip
Write-Host "`nUpgrading pip..." -ForegroundColor Yellow
python -m pip install --upgrade pip

# Install requirements
Write-Host "`nInstalling Python packages..." -ForegroundColor Yellow
pip install -r requirements.txt
Write-Host "✓ Python packages installed" -ForegroundColor Green

# Create necessary directories
Write-Host "`nCreating directories..." -ForegroundColor Yellow
$directories = @("logs", "reports", "temp", "backups")
foreach ($dir in $directories) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir
        Write-Host "✓ Created directory: $dir" -ForegroundColor Green
    } else {
        Write-Host "✓ Directory already exists: $dir" -ForegroundColor Green
    }
}

# Test configuration
Write-Host "`nTesting configuration..." -ForegroundColor Yellow
python -c "
import sys
sys.path.append('pipelines')
try:
    from config.config import config_manager
    print('✓ Configuration module loaded successfully')
except Exception as e:
    print(f'✗ Configuration test failed: {e}')
"

# Display next steps
Write-Host "`n🎉 Setup Complete!" -ForegroundColor Green
Write-Host "=================" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor White
Write-Host "1. Update database connection in pipelines/config/config.yaml" -ForegroundColor White
Write-Host "2. Run database initialization: sqlcmd -S your_server -i scripts/init_database.sql" -ForegroundColor White
Write-Host "3. Test connection: python pipelines/run_etl_pipeline.py --config-check" -ForegroundColor White
Write-Host "4. Run full pipeline: python pipelines/run_etl_pipeline.py --full" -ForegroundColor White
Write-Host "5. View dashboard: python dashboards/business_dashboard.py" -ForegroundColor White

Write-Host "`nDocumentation:" -ForegroundColor White
Write-Host "- README.md - Project overview and setup instructions" -ForegroundColor White
Write-Host "- docs/ - Additional documentation" -ForegroundColor White
Write-Host "- pipelines/ - ETL pipeline code" -ForegroundColor White
Write-Host "- monitoring/ - Data quality and performance monitoring" -ForegroundColor White
Write-Host "- dashboards/ - Business intelligence dashboards" -ForegroundColor White

Write-Host "`nHappy data warehousing! 🚀" -ForegroundColor Cyan