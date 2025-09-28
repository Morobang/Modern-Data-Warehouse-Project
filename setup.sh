#!/bin/bash

# Modern Data Warehouse Project - Setup Guide for Linux/Mac
# =========================================================

echo "🏢 Modern Data Warehouse - Environment Setup"
echo "============================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Check Python installation
echo -e "\n${YELLOW}Checking Python installation...${NC}"
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✓ Python found: $PYTHON_VERSION${NC}"
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_VERSION=$(python --version)
    echo -e "${GREEN}✓ Python found: $PYTHON_VERSION${NC}"
    PYTHON_CMD="python"
else
    echo -e "${RED}✗ Python not found. Please install Python 3.8+ from https://python.org${NC}"
    exit 1
fi

# Check pip
echo -e "\n${YELLOW}Checking pip...${NC}"
if command -v pip3 &> /dev/null; then
    PIP_VERSION=$(pip3 --version)
    echo -e "${GREEN}✓ Pip found: $PIP_VERSION${NC}"
    PIP_CMD="pip3"
elif command -v pip &> /dev/null; then
    PIP_VERSION=$(pip --version)
    echo -e "${GREEN}✓ Pip found: $PIP_VERSION${NC}"
    PIP_CMD="pip"
else
    echo -e "${RED}✗ Pip not found. Please ensure pip is installed with Python${NC}"
    exit 1
fi

# Create virtual environment
echo -e "\n${YELLOW}Creating virtual environment...${NC}"
if [ -d "venv" ]; then
    echo -e "${GREEN}✓ Virtual environment already exists${NC}"
else
    $PYTHON_CMD -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
fi

# Activate virtual environment
echo -e "\n${YELLOW}Activating virtual environment...${NC}"
source venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# Upgrade pip
echo -e "\n${YELLOW}Upgrading pip...${NC}"
pip install --upgrade pip

# Install requirements
echo -e "\n${YELLOW}Installing Python packages...${NC}"
pip install -r requirements.txt
echo -e "${GREEN}✓ Python packages installed${NC}"

# Create necessary directories
echo -e "\n${YELLOW}Creating directories...${NC}"
DIRECTORIES=("logs" "reports" "temp" "backups")
for dir in "${DIRECTORIES[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        echo -e "${GREEN}✓ Created directory: $dir${NC}"
    else
        echo -e "${GREEN}✓ Directory already exists: $dir${NC}"
    fi
done

# Test configuration
echo -e "\n${YELLOW}Testing configuration...${NC}"
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
echo -e "\n${GREEN}🎉 Setup Complete!${NC}"
echo -e "${GREEN}=================${NC}"
echo -e "\n${NC}Next steps:${NC}"
echo -e "${NC}1. Update database connection in pipelines/config/config.yaml${NC}"
echo -e "${NC}2. Run database initialization: sqlcmd -S your_server -i scripts/init_database.sql${NC}"
echo -e "${NC}3. Test connection: python pipelines/run_etl_pipeline.py --config-check${NC}"
echo -e "${NC}4. Run full pipeline: python pipelines/run_etl_pipeline.py --full${NC}"
echo -e "${NC}5. View dashboard: python dashboards/business_dashboard.py${NC}"

echo -e "\n${NC}Documentation:${NC}"
echo -e "${NC}- README.md - Project overview and setup instructions${NC}"
echo -e "${NC}- docs/ - Additional documentation${NC}"
echo -e "${NC}- pipelines/ - ETL pipeline code${NC}"
echo -e "${NC}- monitoring/ - Data quality and performance monitoring${NC}"
echo -e "${NC}- dashboards/ - Business intelligence dashboards${NC}"

echo -e "\n${CYAN}Happy data warehousing! 🚀${NC}"