"""
Modern Data Warehouse - ETL Pipeline Configuration
==================================================
This module handles database connections and configuration settings
for the data warehouse ETL pipeline.
"""

import os
import yaml
from typing import Dict, Any
from dataclasses import dataclass
import logging

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    server: str
    database: str
    username: str = None
    password: str = None
    trusted_connection: bool = True
    driver: str = "ODBC Driver 17 for SQL Server"
    
class ConfigManager:
    """Manages configuration settings for the data warehouse"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(os.path.dirname(__file__), 'config.yaml')
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as file:
                    return yaml.safe_load(file)
            else:
                # Return default configuration if file doesn't exist
                return self._get_default_config()
        except Exception as e:
            logging.error(f"Error loading configuration: {str(e)}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration settings"""
        return {
            'database': {
                'server': 'localhost',
                'database': 'DataWarehouse',
                'trusted_connection': True,
                'driver': 'ODBC Driver 17 for SQL Server'
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file_path': 'logs/pipeline.log'
            },
            'pipeline': {
                'batch_size': 10000,
                'max_retries': 3,
                'retry_delay': 30,
                'timeout': 300
            },
            'data_sources': {
                'crm_path': 'datasets/source_crm/',
                'erp_path': 'datasets/source_erp/'
            },
            'monitoring': {
                'enable_alerts': True,
                'alert_threshold': 0.95,
                'email_notifications': False
            }
        }
    
    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration"""
        db_config = self.config.get('database', {})
        return DatabaseConfig(
            server=db_config.get('server', 'localhost'),
            database=db_config.get('database', 'DataWarehouse'),
            username=db_config.get('username'),
            password=db_config.get('password'),
            trusted_connection=db_config.get('trusted_connection', True),
            driver=db_config.get('driver', 'ODBC Driver 17 for SQL Server')
        )
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.config.get('logging', {})
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration"""
        return self.config.get('pipeline', {})
    
    def get_data_sources_config(self) -> Dict[str, Any]:
        """Get data sources configuration"""
        return self.config.get('data_sources', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration"""
        return self.config.get('monitoring', {})
    
    def save_config(self, config: Dict[str, Any] = None):
        """Save configuration to YAML file"""
        try:
            config_to_save = config or self.config
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as file:
                yaml.dump(config_to_save, file, default_flow_style=False, indent=2)
            logging.info(f"Configuration saved to {self.config_path}")
        except Exception as e:
            logging.error(f"Error saving configuration: {str(e)}")

# Global configuration instance
config_manager = ConfigManager()