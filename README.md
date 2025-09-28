# 🏢 Modern Data Warehouse Project

[![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=for-the-badge&logo=microsoft%20sql%20server&logoColor=white)](https://www.microsoft.com/en-us/sql-server)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com)

A comprehensive modern data warehouse implementation following industry best practices, featuring medallion architecture (Bronze-Silver-Gold), automated ETL pipelines, data quality monitoring, and interactive dashboards.

## 🏗️ Architecture Overview

This project implements a **Medallion Architecture** data warehouse with the following layers:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Bronze Layer  │    │  Silver Layer   │    │   Gold Layer    │
│                 │────│                 │────│                 │────│                 │
│ • CRM System    │    │ • Raw Data      │    │ • Cleansed Data │    │ • Star Schema   │
│ • ERP System    │    │ • No Transform  │    │ • Standardized  │    │ • Dimensions    │
│ • CSV Files     │    │ • Historic Data │    │ • Business Rules│    │ • Fact Tables   │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow Process:
1. **Bronze Layer**: Raw data ingestion from multiple sources (CRM, ERP)
2. **Silver Layer**: Data cleansing, standardization, and business rule application
3. **Gold Layer**: Star schema with optimized dimensional model for analytics

## 📊 Business Domain: Sales Analytics

The warehouse integrates data from:
- **CRM System**: Customer information, product catalog, sales transactions
- **ERP System**: Additional customer demographics, location data, product categorization

### Key Business Metrics:
- Sales performance analysis
- Customer demographics and behavior
- Product line profitability
- Geographic sales distribution
- Seasonal trends and patterns

## 🗂️ Project Structure

```
Modern-Data-Warehouse-Project/
├── 📄 README.md                    # Project documentation
├── 🎨 data_architecture.drawio     # Architecture diagram
├── 📁 datasets/                    # Sample data files
│   ├── source_crm/                 # CRM system data
│   │   ├── cust_info.csv          # Customer information
│   │   ├── prd_info.csv           # Product catalog
│   │   └── sales_details.csv      # Sales transactions
│   └── source_erp/                 # ERP system data
│       ├── CUST_AZ12.csv          # Customer demographics
│       ├── LOC_A101.csv           # Location data
│       └── PX_CAT_G1V2.csv        # Product categories
├── 📁 scripts/                     # SQL scripts
│   ├── init_database.sql          # Database initialization
│   ├── bronze/                     # Bronze layer scripts
│   │   ├── ddl_bronze.sql         # Table definitions
│   │   └── proc_load_bronze.sql   # Data loading procedures
│   ├── silver/                     # Silver layer scripts
│   │   ├── ddl_silver.sql         # Table definitions  
│   │   └── proc_load_silver.sql   # ETL procedures
│   └── gold/                       # Gold layer scripts
│       └── ddl_gold.sql           # Views and star schema
├── 📁 pipelines/                   # Python ETL automation
├── 📁 monitoring/                  # Data quality & monitoring
├── 📁 dashboards/                  # Power BI reports
└── 📁 docs/                        # Additional documentation
```

## 🚀 Quick Start Guide

### Prerequisites
- SQL Server 2019+ or SQL Server Express
- SQL Server Management Studio (SSMS)
- Python 3.8+ (for automation scripts)
- Power BI Desktop (for dashboards)

### Step-by-Step Execution Order

#### Phase 1: Environment Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/Modern-Data-Warehouse-Project.git
   cd Modern-Data-Warehouse-Project
   ```

2. **Run Setup Script (Choose your OS)**
   ```bash
   # Windows PowerShell
   .\setup.ps1
   
   # Linux/Mac
   chmod +x setup.sh && ./setup.sh
   ```

#### Phase 2: Database Infrastructure (Run in EXACT Order)

3. **Step 1: Initialize Database and Schemas**
   ```sql
   -- In SSMS, connect to your SQL Server instance and run:
   sqlcmd -S your_server_name -i scripts/init_database.sql
   ```
   ✅ **What this does**: Creates the `DataWarehouse` database and `bronze`, `silver`, `gold` schemas

4. **Step 2: Create Bronze Layer Tables**
   ```sql
   -- Run in SSMS connected to DataWarehouse database:
   sqlcmd -S your_server_name -d DataWarehouse -i scripts/bronze/ddl_bronze.sql
   ```
   ✅ **What this does**: Creates all Bronze layer tables for raw data storage

5. **Step 3: Create Bronze Layer Stored Procedures**
   ```sql
   -- Run in SSMS:
   sqlcmd -S your_server_name -d DataWarehouse -i scripts/bronze/proc_load_bronze.sql
   ```
   ✅ **What this does**: Creates the `bronze.load_bronze` stored procedure

6. **Step 4: Create Silver Layer Tables**
   ```sql
   -- Run in SSMS:
   sqlcmd -S your_server_name -d DataWarehouse -i scripts/silver/ddl_silver.sql
   ```
   ✅ **What this does**: Creates all Silver layer tables for cleansed data

7. **Step 5: Create Silver Layer Stored Procedures**
   ```sql
   -- Run in SSMS:
   sqlcmd -S your_server_name -d DataWarehouse -i scripts/silver/proc_load_silver.sql
   ```
   ✅ **What this does**: Creates the `silver.load_silver` stored procedure

8. **Step 6: Create Gold Layer Views**
   ```sql
   -- Run in SSMS:
   sqlcmd -S your_server_name -d DataWarehouse -i scripts/gold/ddl_gold.sql
   ```
   ✅ **What this does**: Creates dimension and fact views for analytics

#### Phase 3: Data Loading (First Time Setup)

9. **Step 7: Update File Paths**
   - Edit `scripts/bronze/proc_load_bronze.sql`
   - Update the CSV file paths to match your system:
   ```sql
   -- Change these paths to your actual file locations:
   FROM 'C:\your_path\Modern-Data-Warehouse-Project\datasets\source_crm\cust_info.csv'
   FROM 'C:\your_path\Modern-Data-Warehouse-Project\datasets\source_crm\prd_info.csv'
   -- ... etc for all files
   ```

10. **Step 8: Load Bronze Layer Data**
   ```sql
   -- In SSMS, run:
   EXEC bronze.load_bronze;
   ```
   ✅ **What this does**: Loads all raw CSV data into Bronze tables

11. **Step 9: Load Silver Layer Data**
   ```sql
   -- In SSMS, run:
   EXEC silver.load_silver;
   ```
   ✅ **What this does**: Cleanses and transforms data from Bronze to Silver

#### Phase 4: Python Pipeline Setup

12. **Step 10: Configure Python Pipeline**
   - Edit `pipelines/config/config.yaml`
   - Update database connection settings:
   ```yaml
   database:
     server: "your_server_name"  # Update this
     database: "DataWarehouse"
     trusted_connection: true    # or false if using SQL auth
   ```

13. **Step 11: Test Configuration**
   ```bash
   python pipelines/run_etl_pipeline.py --config-check
   ```
   ✅ **What this does**: Validates database connection and configuration

14. **Step 12: Run Full Pipeline**
   ```bash
   python pipelines/run_etl_pipeline.py --full
   ```
   ✅ **What this does**: Runs complete ETL pipeline with data quality checks

#### Phase 5: Dashboards and Monitoring

15. **Step 13: Launch Business Dashboard**
   ```bash
   python dashboards/business_dashboard.py
   ```
   ✅ **Access at**: http://localhost:8050
   ✅ **What this shows**: Sales analytics, customer insights, product performance

16. **Step 14: Launch Data Quality Dashboard**
   ```bash
   python monitoring/dashboard.py
   ```
   ✅ **Access at**: http://localhost:5000
   ✅ **What this shows**: Data quality metrics, pipeline health, performance stats

### 🎯 Quick Test Commands

After setup, you can use these commands to test different components:

```bash
# Test database connection only
python pipelines/run_etl_pipeline.py --config-check

# Run only data validation
python pipelines/run_etl_pipeline.py --validate-only

# Run only Bronze layer
python pipelines/run_etl_pipeline.py --bronze-only

# Run only Silver layer  
python pipelines/run_etl_pipeline.py --silver-only

# Generate static reports
python dashboards/business_dashboard.py --static
python monitoring/dashboard.py --static
```

### 📋 Execution Checklist

- [ ] ✅ Step 1: Database initialization (`init_database.sql`)
- [ ] ✅ Step 2: Bronze tables (`bronze/ddl_bronze.sql`)
- [ ] ✅ Step 3: Bronze procedures (`bronze/proc_load_bronze.sql`)
- [ ] ✅ Step 4: Silver tables (`silver/ddl_silver.sql`)
- [ ] ✅ Step 5: Silver procedures (`silver/proc_load_silver.sql`)
- [ ] ✅ Step 6: Gold views (`gold/ddl_gold.sql`)
- [ ] ✅ Step 7: Update CSV file paths
- [ ] ✅ Step 8: Load Bronze data (`EXEC bronze.load_bronze`)
- [ ] ✅ Step 9: Load Silver data (`EXEC silver.load_silver`)
- [ ] ✅ Step 10: Configure Python settings
- [ ] ✅ Step 11: Test connection
- [ ] ✅ Step 12: Run full pipeline
- [ ] ✅ Step 13: Launch dashboards

## 🎯 Key Features

### ✨ Data Architecture Excellence
- **Medallion Architecture**: Industry-standard Bronze-Silver-Gold pattern
- **Star Schema Design**: Optimized dimensional model in Gold layer
- **Multi-Source Integration**: Seamlessly combines CRM and ERP data
- **Slowly Changing Dimensions**: Proper historical data handling

### 🔄 ETL/ELT Automation
- **Python-Based Pipelines**: Automated data processing workflows
- **Error Handling**: Comprehensive logging and error recovery
- **Incremental Loading**: Efficient data refresh strategies
- **Data Validation**: Quality checks at each layer

### 📈 Data Quality & Monitoring
- **Data Profiling**: Automated data quality assessments
- **Anomaly Detection**: Statistical outlier identification
- **Data Lineage**: Complete data flow tracking
- **Performance Monitoring**: Pipeline execution metrics

### 📊 Analytics & Visualization  
- **Interactive Dashboards**: Power BI reports and dashboards
- **KPI Monitoring**: Key business metrics tracking
- **Self-Service Analytics**: User-friendly data exploration
- **Mobile Responsive**: Dashboards optimized for all devices

## 📋 Data Model Details

### Bronze Layer Tables
- `bronze.crm_cust_info` - Raw customer data from CRM
- `bronze.crm_prd_info` - Raw product information  
- `bronze.crm_sales_details` - Raw sales transactions
- `bronze.erp_cust_az12` - Raw customer demographics from ERP
- `bronze.erp_loc_a101` - Raw location data
- `bronze.erp_px_cat_g1v2` - Raw product categories

### Silver Layer Tables  
- `silver.crm_cust_info` - Cleansed customer data
- `silver.crm_prd_info` - Standardized product information
- `silver.crm_sales_details` - Validated sales transactions
- `silver.erp_cust_az12` - Cleansed demographics
- `silver.erp_loc_a101` - Standardized locations
- `silver.erp_px_cat_g1v2` - Clean product categories

### Gold Layer (Star Schema)
- `gold.dim_customers` - Customer dimension with demographics
- `gold.dim_products` - Product dimension with categories  
- `gold.fact_sales` - Sales fact table with metrics

## 🔧 Configuration

### Database Connection
Update connection strings in:
- `pipelines/config/config.yaml` - Main configuration file
- Update server name, database name, and authentication method

### File Paths
Update CSV file paths in:
- `scripts/bronze/proc_load_bronze.sql` - Update all `FROM 'path'` statements

### Scheduling
Configure automated runs in:
- `pipelines/scheduler.py`
- GitHub Actions (`.github/workflows/`)

## 🚨 Troubleshooting Guide

### Common Issues and Solutions

#### 1. Database Connection Failed
**Error**: `Database connection test failed`
**Solutions**:
- Verify SQL Server is running
- Check server name in `pipelines/config/config.yaml`
- Ensure Windows Authentication is enabled (or provide SQL credentials)
- Test connection manually in SSMS first

#### 2. File Not Found Errors
**Error**: `Cannot bulk load. The file does not exist`
**Solutions**:
- Check CSV file paths in `scripts/bronze/proc_load_bronze.sql`
- Ensure CSV files exist in `datasets/` folders
- Use full absolute paths (e.g., `C:\Users\YourName\...`)
- Check file permissions

#### 3. Python Import Errors
**Error**: `ModuleNotFoundError: No module named 'pandas'`
**Solutions**:
- Run setup script: `.\setup.ps1` or `./setup.sh`
- Install requirements: `pip install -r requirements.txt`
- Activate virtual environment: `venv\Scripts\activate` (Windows)

#### 4. Permission Denied
**Error**: `Access is denied` or `Permission denied`
**Solutions**:
- Run PowerShell/Command Prompt as Administrator
- Check SQL Server service account permissions
- Ensure user has db_owner rights on DataWarehouse database

#### 5. Port Already in Use
**Error**: `Port 8050 is already in use`
**Solutions**:
- Stop other applications using the port
- Change port in dashboard code: `dashboard.run_dashboard(port=8051)`
- Kill process: `netstat -ano | findstr :8050` then `taskkill /PID <process_id>`

#### 6. Data Quality Scores Low
**Issue**: Quality dashboard shows low scores
**Solutions**:
- Check Bronze layer data for completeness
- Review Silver layer transformation logic
- Examine error logs in `logs/` folder
- Run validation only: `python pipelines/run_etl_pipeline.py --validate-only`

### Quick Diagnostics

Run these commands to diagnose issues:

```bash
# Test Python environment
python --version
pip list | grep pandas

# Test database connection
python -c "
import sys
sys.path.append('pipelines')
from config.database import db_manager
print('✓ Connection OK' if db_manager.test_connection() else '✗ Connection Failed')
"

# Check file paths
python -c "
import os
crm_files = ['datasets/source_crm/cust_info.csv', 'datasets/source_crm/prd_info.csv']
for f in crm_files:
    print(f'✓ {f}' if os.path.exists(f) else f'✗ {f} not found')
"

# Test SQL Server connection (Windows)
sqlcmd -S your_server_name -Q "SELECT @@VERSION"
```

## 📊 Sample Queries

### Top 10 Customers by Sales
```sql
SELECT TOP 10
    c.first_name + ' ' + c.last_name as customer_name,
    c.country,
    SUM(f.sales_amount) as total_sales,
    COUNT(f.order_number) as order_count
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name, c.country
ORDER BY total_sales DESC;
```

### Monthly Sales Trend
```sql
SELECT 
    YEAR(order_date) as sales_year,
    MONTH(order_date) as sales_month,
    SUM(sales_amount) as monthly_sales,
    COUNT(DISTINCT customer_key) as unique_customers
FROM gold.fact_sales
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY sales_year, sales_month;
```

## 🚀 Advanced Features

### Data Lineage Tracking
- Complete data flow documentation
- Impact analysis for schema changes
- Data governance compliance

### Performance Optimization
- Columnstore indexes on fact tables
- Partitioning strategies for large tables
- Query optimization recommendations

### Security & Compliance
- Row-level security implementation
- Data masking for sensitive information
- Audit logging for all operations

## 🧪 Testing

### Data Quality Tests
```bash
python tests/test_data_quality.py
```

### Pipeline Tests  
```bash
python tests/test_etl_pipeline.py
```

### Performance Tests
```bash
python tests/test_performance.py
```

## 📈 Monitoring & Alerting

### Key Metrics Tracked
- Pipeline execution times
- Data quality scores
- Record counts and growth
- Error rates and types

### Alerting Rules
- Failed pipeline executions
- Data quality threshold breaches
- Unusual data volume changes
- Performance degradation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Contact

**Morobang Tshigidimisa** - [morobangtshigidimisa@gmail.com](mailto:your.email@example.com)

Project Link: [https://github.com/yourusername/Modern-Data-Warehouse-Project](https://github.com/yourusername/Modern-Data-Warehouse-Project)

## 🙏 Acknowledgments

- Microsoft SQL Server documentation
- Modern data warehouse design patterns
- Medallion architecture best practices
- Data engineering community resources

---
⭐ **Star this repository if you found it helpful!** ⭐
