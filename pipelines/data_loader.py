"""
Modern Data Warehouse - Data Loader
===================================
Handles data loading operations for the ETL pipeline.
"""

import os
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from config.config import config_manager
from config.database import db_manager

class DataLoader:
    """Handles data loading operations"""
    
    def __init__(self):
        self.config = config_manager
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        
        # Get data source paths
        data_sources = self.config.get_data_sources_config()
        self.crm_path = data_sources.get('crm_path', 'datasets/source_crm/')
        self.erp_path = data_sources.get('erp_path', 'datasets/source_erp/')
    
    def load_csv_file(self, file_path: str, encoding: str = 'utf-8') -> Optional[pd.DataFrame]:
        """Load CSV file into pandas DataFrame"""
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"File not found: {file_path}")
                return None
            
            df = pd.read_csv(file_path, encoding=encoding)
            self.logger.info(f"Loaded {len(df)} rows from {file_path}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading CSV file {file_path}: {str(e)}")
            return None
    
    def get_crm_files(self) -> Dict[str, str]:
        """Get CRM data file paths"""
        base_path = Path(self.crm_path)
        return {
            'customers': str(base_path / 'cust_info.csv'),
            'products': str(base_path / 'prd_info.csv'),
            'sales': str(base_path / 'sales_details.csv')
        }
    
    def get_erp_files(self) -> Dict[str, str]:
        """Get ERP data file paths"""
        base_path = Path(self.erp_path)
        return {
            'customers': str(base_path / 'CUST_AZ12.csv'),
            'locations': str(base_path / 'LOC_A101.csv'),
            'categories': str(base_path / 'PX_CAT_G1V2.csv')
        }
    
    def load_crm_data(self) -> Dict[str, pd.DataFrame]:
        """Load all CRM data files"""
        crm_files = self.get_crm_files()
        crm_data = {}
        
        for data_type, file_path in crm_files.items():
            df = self.load_csv_file(file_path)
            if df is not None:
                crm_data[data_type] = df
                self.logger.info(f"CRM {data_type} loaded: {len(df)} rows")
            else:
                self.logger.warning(f"Failed to load CRM {data_type} from {file_path}")
        
        return crm_data
    
    def load_erp_data(self) -> Dict[str, pd.DataFrame]:
        """Load all ERP data files"""
        erp_files = self.get_erp_files()
        erp_data = {}
        
        for data_type, file_path in erp_files.items():
            df = self.load_csv_file(file_path)
            if df is not None:
                erp_data[data_type] = df
                self.logger.info(f"ERP {data_type} loaded: {len(df)} rows")
            else:
                self.logger.warning(f"Failed to load ERP {data_type} from {file_path}")
        
        return erp_data
    
    def validate_file_structure(self, df: pd.DataFrame, expected_columns: List[str], file_name: str) -> bool:
        """Validate that DataFrame has expected column structure"""
        try:
            df_columns = set(df.columns.str.lower())
            expected_columns_lower = set(col.lower() for col in expected_columns)
            
            missing_columns = expected_columns_lower - df_columns
            extra_columns = df_columns - expected_columns_lower
            
            if missing_columns:
                self.logger.warning(f"{file_name}: Missing columns: {missing_columns}")
            
            if extra_columns:
                self.logger.info(f"{file_name}: Extra columns found: {extra_columns}")
            
            # Return True if no critical columns are missing
            return len(missing_columns) == 0
            
        except Exception as e:
            self.logger.error(f"Error validating file structure for {file_name}: {str(e)}")
            return False
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get information about a data file"""
        try:
            if not os.path.exists(file_path):
                return {"exists": False}
            
            file_stat = os.stat(file_path)
            df = pd.read_csv(file_path, nrows=1)  # Just read header
            
            return {
                "exists": True,
                "size_bytes": file_stat.st_size,
                "size_mb": round(file_stat.st_size / (1024 * 1024), 2),
                "modified_time": file_stat.st_mtime,
                "columns": list(df.columns),
                "column_count": len(df.columns)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting file info for {file_path}: {str(e)}")
            return {"exists": False, "error": str(e)}
    
    def archive_processed_files(self, source_files: List[str], archive_path: str = None):
        """Archive processed files (for future enhancement)"""
        # This would implement file archiving logic
        # For now, just log the operation
        archive_path = archive_path or self.config.get_data_sources_config().get('archive_path', 'datasets/archive/')
        
        self.logger.info(f"Archiving {len(source_files)} files to {archive_path}")
        # Implementation would go here
    
    def get_all_data_files_info(self) -> Dict[str, Any]:
        """Get information about all data files"""
        info = {
            "crm_files": {},
            "erp_files": {}
        }
        
        # CRM files info
        crm_files = self.get_crm_files()
        for data_type, file_path in crm_files.items():
            info["crm_files"][data_type] = self.get_file_info(file_path)
        
        # ERP files info
        erp_files = self.get_erp_files()
        for data_type, file_path in erp_files.items():
            info["erp_files"][data_type] = self.get_file_info(file_path)
        
        return info