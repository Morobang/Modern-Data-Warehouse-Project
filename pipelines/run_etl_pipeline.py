"""
Modern Data Warehouse - Pipeline Runner
======================================
Main entry point to run the ETL pipeline with different options.
"""

import sys
import argparse
import logging
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from etl_pipeline import ETLPipeline
from data_validator import DataValidator
from config.config import config_manager
from utils.logger import setup_logging

def main():
    """Main entry point with command line arguments"""
    parser = argparse.ArgumentParser(
        description="Modern Data Warehouse ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_etl_pipeline.py --full                    # Run full pipeline
  python run_etl_pipeline.py --validate-only           # Run only data validation
  python run_etl_pipeline.py --config-check           # Check configuration
  python run_etl_pipeline.py --full --log-level DEBUG # Run with debug logging
        """
    )
    
    parser.add_argument(
        '--full', 
        action='store_true',
        help='Run the full ETL pipeline (Bronze -> Silver -> Gold)'
    )
    
    parser.add_argument(
        '--bronze-only',
        action='store_true', 
        help='Run only Bronze layer ETL'
    )
    
    parser.add_argument(
        '--silver-only',
        action='store_true',
        help='Run only Silver layer ETL'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Run only data quality validation'
    )
    
    parser.add_argument(
        '--config-check',
        action='store_true',
        help='Check configuration and database connectivity'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Override log file path'
    )
    
    parser.add_argument(
        '--export-metrics',
        type=str,
        help='Export metrics to specified JSON file'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_config = config_manager.get_logging_config()
    log_file = args.log_file or log_config.get('file_path', 'logs/pipeline.log')
    
    setup_logging(
        level=args.log_level,
        log_file=log_file
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Configuration check
        if args.config_check or not any([args.full, args.bronze_only, args.silver_only, args.validate_only]):
            return run_config_check()
        
        # Data validation only
        if args.validate_only:
            return run_validation_only()
        
        # Initialize pipeline
        pipeline = ETLPipeline()
        success = False
        
        # Run requested operations
        if args.full:
            logger.info("Starting full ETL pipeline...")
            success = pipeline.run_full_pipeline()
        
        elif args.bronze_only:
            logger.info("Starting Bronze layer ETL...")
            success = pipeline._run_bronze_etl()
        
        elif args.silver_only:
            logger.info("Starting Silver layer ETL...")
            success = pipeline._run_silver_etl()
        
        # Export metrics if requested
        if args.export_metrics:
            pipeline.metrics.export_metrics_json(args.export_metrics)
        
        # Print final status
        if success:
            logger.info("Pipeline completed successfully!")
            print("\n✓ Pipeline completed successfully!")
        else:
            logger.error("Pipeline failed!")
            print("\n✗ Pipeline failed!")
        
        # Print metrics summary
        print("\n" + pipeline.metrics.get_summary_report())
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        print("\nPipeline interrupted by user")
        return 1
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"\nUnexpected error: {str(e)}")
        return 1

def run_config_check() -> int:
    """Run configuration and connectivity checks"""
    logger = logging.getLogger(__name__)
    
    print("=" * 60)
    print("CONFIGURATION AND CONNECTIVITY CHECK")
    print("=" * 60)
    
    try:
        # Check configuration
        config = config_manager.config
        print("✓ Configuration loaded successfully")
        print(f"  Database: {config['database']['server']}/{config['database']['database']}")
        print(f"  CRM Path: {config['data_sources']['crm_path']}")
        print(f"  ERP Path: {config['data_sources']['erp_path']}")
        
        # Test database connection
        from config.database import db_manager
        
        print("\nTesting database connection...")
        if db_manager.test_connection():
            print("✓ Database connection successful")
            
            # Get schema info
            schemas = db_manager.get_schema_list()
            print(f"  Available schemas: {', '.join(schemas)}")
            
            # Get table info for each schema
            for schema in ['bronze', 'silver', 'gold']:
                if schema in schemas:
                    tables = db_manager.get_table_info(schema)
                    print(f"  {schema.title()} layer: {len(tables)} tables")
        else:
            print("✗ Database connection failed")
            return 1
        
        # Check data files
        from data_loader import DataLoader
        data_loader = DataLoader()
        
        print("\nChecking data files...")
        file_info = data_loader.get_all_data_files_info()
        
        for source_type, files in file_info.items():
            print(f"\n{source_type.upper()} Files:")
            for file_type, info in files.items():
                if info.get('exists', False):
                    print(f"  ✓ {file_type}: {info['size_mb']} MB, {info['column_count']} columns")
                else:
                    print(f"  ✗ {file_type}: File not found")
        
        print("\n" + "=" * 60)
        print("Configuration check completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Configuration check failed: {str(e)}")
        print(f"\n✗ Configuration check failed: {str(e)}")
        return 1

def run_validation_only() -> int:
    """Run only data quality validation"""
    logger = logging.getLogger(__name__)
    
    print("=" * 60)
    print("DATA QUALITY VALIDATION")
    print("=" * 60)
    
    try:
        validator = DataValidator()
        report = validator.generate_data_quality_report()
        print(report)
        
        # Also run validations to get pass/fail status
        results = validator.run_all_validations()
        
        if results:
            failed_checks = [r for r in results if not r['passed']]
            overall_score = sum(r['score'] for r in results) / len(results)
            
            if failed_checks:
                print(f"\n⚠ {len(failed_checks)} validation checks failed")
                return 1
            else:
                print(f"\n✓ All validation checks passed (Score: {overall_score:.1%})")
                return 0
        else:
            print("\nNo validation results available")
            return 1
            
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        print(f"\n✗ Validation failed: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)