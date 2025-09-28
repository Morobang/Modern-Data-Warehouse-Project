"""
Modern Data Warehouse - ETL Pipeline Core
=========================================
Main ETL pipeline orchestrator for the modern data warehouse.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.config import config_manager
from config.database import db_manager
from utils.logger import setup_logging
from data_loader import DataLoader
from data_validator import DataValidator
from monitoring.metrics import MetricsCollector

class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self):
        self.config = config_manager
        self.db = db_manager
        self.logger = self._setup_logger()
        self.data_loader = DataLoader()
        self.data_validator = DataValidator()
        self.metrics = MetricsCollector()
        
        # Pipeline state
        self.pipeline_start_time = None
        self.pipeline_end_time = None
        self.pipeline_status = "INITIALIZED"
        self.pipeline_errors = []
        
    def _setup_logger(self) -> logging.Logger:
        """Setup pipeline logger"""
        logging_config = self.config.get_logging_config()
        setup_logging(
            level=logging_config.get('level', 'INFO'),
            log_file=logging_config.get('file_path', 'logs/pipeline.log')
        )
        return logging.getLogger(__name__)
    
    def run_full_pipeline(self) -> bool:
        """Run the complete ETL pipeline (Bronze -> Silver -> Gold)"""
        self.pipeline_start_time = datetime.now()
        self.pipeline_status = "RUNNING"
        
        try:
            self.logger.info("=" * 80)
            self.logger.info("STARTING MODERN DATA WAREHOUSE ETL PIPELINE")
            self.logger.info("=" * 80)
            
            # Step 1: Test database connection
            if not self._test_database_connection():
                return False
            
            # Step 2: Run Bronze layer ETL
            if not self._run_bronze_etl():
                return False
            
            # Step 3: Run Silver layer ETL
            if not self._run_silver_etl():
                return False
            
            # Step 4: Refresh Gold layer views (no ETL needed for views)
            if not self._refresh_gold_layer():
                return False
            
            # Step 5: Run data quality validations
            if not self._run_data_quality_checks():
                return False
            
            # Step 6: Collect and report metrics
            self._collect_pipeline_metrics()
            
            self.pipeline_status = "COMPLETED"
            self.pipeline_end_time = datetime.now()
            
            duration = self.pipeline_end_time - self.pipeline_start_time
            self.logger.info("=" * 80)
            self.logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {duration}")
            self.logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            self.pipeline_status = "FAILED"
            self.pipeline_end_time = datetime.now()
            self.pipeline_errors.append(str(e))
            self.logger.error(f"Pipeline failed with error: {str(e)}")
            return False
    
    def _test_database_connection(self) -> bool:
        """Test database connectivity"""
        self.logger.info("Testing database connection...")
        
        try:
            if self.db.test_connection():
                self.logger.info("✓ Database connection successful")
                
                # Log database info
                schemas = self.db.get_schema_list()
                self.logger.info(f"Available schemas: {', '.join(schemas)}")
                
                return True
            else:
                self.logger.error("✗ Database connection failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Database connection test error: {str(e)}")
            return False
    
    def _run_bronze_etl(self) -> bool:
        """Execute Bronze layer ETL"""
        self.logger.info("-" * 60)
        self.logger.info("STARTING BRONZE LAYER ETL")
        self.logger.info("-" * 60)
        
        try:
            start_time = time.time()
            
            # Execute the Bronze layer stored procedure
            success = self.db.execute_stored_procedure("bronze.load_bronze")
            
            if success:
                end_time = time.time()
                duration = end_time - start_time
                
                # Log table counts after loading
                bronze_tables = [
                    "crm_cust_info", "crm_prd_info", "crm_sales_details",
                    "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"
                ]
                
                total_rows = 0
                for table in bronze_tables:
                    count = self.db.get_table_row_count(table, "bronze")
                    total_rows += count
                    self.logger.info(f"bronze.{table}: {count:,} rows")
                
                self.logger.info(f"✓ Bronze layer completed - {total_rows:,} total rows loaded in {duration:.2f}s")
                
                # Store metrics
                self.metrics.add_metric("bronze_load_duration", duration)
                self.metrics.add_metric("bronze_rows_loaded", total_rows)
                
                return True
            else:
                self.logger.error("✗ Bronze layer ETL failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Bronze layer ETL error: {str(e)}")
            return False
    
    def _run_silver_etl(self) -> bool:
        """Execute Silver layer ETL"""
        self.logger.info("-" * 60)
        self.logger.info("STARTING SILVER LAYER ETL")
        self.logger.info("-" * 60)
        
        try:
            start_time = time.time()
            
            # Execute the Silver layer stored procedure
            success = self.db.execute_stored_procedure("silver.load_silver")
            
            if success:
                end_time = time.time()
                duration = end_time - start_time
                
                # Log table counts after loading
                silver_tables = [
                    "crm_cust_info", "crm_prd_info", "crm_sales_details",
                    "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"
                ]
                
                total_rows = 0
                for table in silver_tables:
                    count = self.db.get_table_row_count(table, "silver")
                    total_rows += count
                    self.logger.info(f"silver.{table}: {count:,} rows")
                
                self.logger.info(f"✓ Silver layer completed - {total_rows:,} total rows processed in {duration:.2f}s")
                
                # Store metrics
                self.metrics.add_metric("silver_load_duration", duration)
                self.metrics.add_metric("silver_rows_processed", total_rows)
                
                return True
            else:
                self.logger.error("✗ Silver layer ETL failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Silver layer ETL error: {str(e)}")
            return False
    
    def _refresh_gold_layer(self) -> bool:
        """Refresh Gold layer views and collect metrics"""
        self.logger.info("-" * 60)
        self.logger.info("REFRESHING GOLD LAYER")
        self.logger.info("-" * 60)
        
        try:
            start_time = time.time()
            
            # Since Gold layer uses views, we just need to query them to verify they work
            gold_views = ["dim_customers", "dim_products", "fact_sales"]
            
            total_rows = 0
            for view in gold_views:
                try:
                    query = f"SELECT COUNT(*) as row_count FROM gold.{view}"
                    result = self.db.execute_query(query)
                    count = result[0]['row_count'] if result else 0
                    total_rows += count
                    self.logger.info(f"gold.{view}: {count:,} rows")
                except Exception as e:
                    self.logger.warning(f"Could not query gold.{view}: {str(e)}")
            
            end_time = time.time()
            duration = end_time - start_time
            
            self.logger.info(f"✓ Gold layer refreshed - {total_rows:,} total rows available in {duration:.2f}s")
            
            # Store metrics
            self.metrics.add_metric("gold_refresh_duration", duration)
            self.metrics.add_metric("gold_rows_available", total_rows)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Gold layer refresh error: {str(e)}")
            return False
    
    def _run_data_quality_checks(self) -> bool:
        """Run data quality validation checks"""
        self.logger.info("-" * 60)
        self.logger.info("RUNNING DATA QUALITY CHECKS")
        self.logger.info("-" * 60)
        
        try:
            # Run data quality validations
            quality_results = self.data_validator.run_all_validations()
            
            overall_score = sum(result['score'] for result in quality_results) / len(quality_results)
            
            self.logger.info(f"Data Quality Score: {overall_score:.2%}")
            
            for result in quality_results:
                status = "✓" if result['passed'] else "✗"
                self.logger.info(f"{status} {result['check_name']}: {result['score']:.2%}")
                if not result['passed']:
                    self.logger.warning(f"  Issues: {result.get('details', 'No details')}")
            
            # Store metrics
            self.metrics.add_metric("data_quality_score", overall_score)
            
            # Determine if quality checks passed
            quality_threshold = self.config.get_monitoring_config().get('alert_threshold', 0.95)
            
            if overall_score >= quality_threshold:
                self.logger.info(f"✓ Data quality checks passed (score: {overall_score:.2%})")
                return True
            else:
                self.logger.warning(f"⚠ Data quality below threshold (score: {overall_score:.2%}, threshold: {quality_threshold:.2%})")
                return True  # Don't fail pipeline for quality issues, just warn
                
        except Exception as e:
            self.logger.error(f"Data quality checks error: {str(e)}")
            return False
    
    def _collect_pipeline_metrics(self):
        """Collect and log pipeline performance metrics"""
        try:
            if self.pipeline_start_time and self.pipeline_end_time:
                total_duration = (self.pipeline_end_time - self.pipeline_start_time).total_seconds()
                self.metrics.add_metric("total_pipeline_duration", total_duration)
            
            # Log all metrics
            self.logger.info("-" * 60)
            self.logger.info("PIPELINE METRICS")
            self.logger.info("-" * 60)
            
            for metric_name, metric_value in self.metrics.get_all_metrics().items():
                if "duration" in metric_name:
                    self.logger.info(f"{metric_name}: {metric_value:.2f} seconds")
                elif "rows" in metric_name:
                    self.logger.info(f"{metric_name}: {metric_value:,}")
                else:
                    self.logger.info(f"{metric_name}: {metric_value}")
            
        except Exception as e:
            self.logger.error(f"Error collecting metrics: {str(e)}")
    
    def run_incremental_pipeline(self) -> bool:
        """Run incremental ETL pipeline (for future enhancement)"""
        # This would implement incremental loading logic
        # For now, we just run the full pipeline
        self.logger.info("Incremental pipeline not yet implemented, running full pipeline")
        return self.run_full_pipeline()
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and metrics"""
        return {
            "status": self.pipeline_status,
            "start_time": self.pipeline_start_time,
            "end_time": self.pipeline_end_time,
            "duration": (self.pipeline_end_time - self.pipeline_start_time).total_seconds() if self.pipeline_end_time and self.pipeline_start_time else None,
            "errors": self.pipeline_errors,
            "metrics": self.metrics.get_all_metrics()
        }

def main():
    """Main entry point for the ETL pipeline"""
    pipeline = ETLPipeline()
    
    # Run the pipeline
    success = pipeline.run_full_pipeline()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()