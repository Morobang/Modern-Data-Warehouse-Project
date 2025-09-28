"""
Modern Data Warehouse - Logging Utilities
=========================================
Centralized logging configuration for the data warehouse pipeline.
"""

import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

def setup_logging(level: str = "INFO", log_file: str = None, max_file_size: int = 10485760, backup_count: int = 5):
    """
    Setup centralized logging for the data warehouse pipeline
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        max_file_size: Maximum size of log file in bytes (default: 10MB)
        backup_count: Number of backup log files to keep
    """
    
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
    
    # Clear any existing handlers
    logging.getLogger().handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (if log_file is specified)
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Log the setup
    logger = logging.getLogger(__name__)
    logger.info(f"Logging setup complete - Level: {level}")
    if log_file:
        logger.info(f"Log file: {log_file}")

class PipelineLogger:
    """Enhanced logger with pipeline-specific features"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.start_time = None
        self.step_times = {}
    
    def start_step(self, step_name: str):
        """Start timing a pipeline step"""
        self.step_times[step_name] = datetime.now()
        self.logger.info(f"Starting step: {step_name}")
    
    def end_step(self, step_name: str, success: bool = True):
        """End timing a pipeline step and log duration"""
        if step_name in self.step_times:
            duration = datetime.now() - self.step_times[step_name]
            status = "✓" if success else "✗"
            self.logger.info(f"{status} Completed step: {step_name} (Duration: {duration})")
            del self.step_times[step_name]
        else:
            self.logger.warning(f"Step '{step_name}' was not started with start_step()")
    
    def log_metrics(self, metrics: dict):
        """Log metrics in a formatted way"""
        self.logger.info("-" * 50)
        self.logger.info("METRICS")
        self.logger.info("-" * 50)
        for key, value in metrics.items():
            if isinstance(value, float):
                self.logger.info(f"{key}: {value:.2f}")
            else:
                self.logger.info(f"{key}: {value}")
    
    def log_table_counts(self, table_counts: dict, schema: str = None):
        """Log table row counts in a formatted way"""
        schema_prefix = f"{schema}." if schema else ""
        
        self.logger.info("-" * 50)
        self.logger.info(f"TABLE COUNTS {f'({schema.upper()})' if schema else ''}")
        self.logger.info("-" * 50)
        
        total_rows = 0
        for table_name, count in table_counts.items():
            self.logger.info(f"{schema_prefix}{table_name}: {count:,} rows")
            total_rows += count
        
        self.logger.info("-" * 50)
        self.logger.info(f"TOTAL: {total_rows:,} rows")

def get_pipeline_logger(name: str) -> PipelineLogger:
    """Get a pipeline logger instance"""
    return PipelineLogger(name)