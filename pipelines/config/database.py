"""
Modern Data Warehouse - Database Connection Manager
==================================================
This module handles database connections and operations for the data warehouse.
"""

import pyodbc
import pandas as pd
import logging
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Union
from .config import config_manager, DatabaseConfig

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, config: DatabaseConfig = None):
        self.config = config or config_manager.get_database_config()
        self.logger = logging.getLogger(__name__)
        
    def get_connection_string(self) -> str:
        """Build connection string based on configuration"""
        if self.config.trusted_connection:
            return (
                f"DRIVER={{{self.config.driver}}};"
                f"SERVER={self.config.server};"
                f"DATABASE={self.config.database};"
                f"Trusted_Connection=yes;"
            )
        else:
            return (
                f"DRIVER={{{self.config.driver}}};"
                f"SERVER={self.config.server};"
                f"DATABASE={self.config.database};"
                f"UID={self.config.username};"
                f"PWD={self.config.password};"
            )
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        connection = None
        try:
            connection_string = self.get_connection_string()
            connection = pyodbc.connect(connection_string)
            self.logger.debug("Database connection established")
            yield connection
        except Exception as e:
            self.logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
                self.logger.debug("Database connection closed")
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results as list of dictionaries"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get column names
                columns = [column[0] for column in cursor.description]
                
                # Fetch all results
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                self.logger.info(f"Query executed successfully, returned {len(results)} rows")
                return results
                
        except Exception as e:
            self.logger.error(f"Query execution error: {str(e)}")
            raise
    
    def execute_non_query(self, query: str, params: tuple = None) -> int:
        """Execute INSERT, UPDATE, DELETE query and return affected rows count"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                affected_rows = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"Non-query executed successfully, affected {affected_rows} rows")
                return affected_rows
                
        except Exception as e:
            self.logger.error(f"Non-query execution error: {str(e)}")
            raise
    
    def execute_stored_procedure(self, procedure_name: str, params: tuple = None) -> bool:
        """Execute stored procedure"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(f"EXEC {procedure_name} {','.join(['?' for _ in params])}", params)
                else:
                    cursor.execute(f"EXEC {procedure_name}")
                
                conn.commit()
                self.logger.info(f"Stored procedure '{procedure_name}' executed successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Stored procedure execution error: {str(e)}")
            raise
    
    def bulk_insert_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = 'dbo') -> bool:
        """Insert pandas DataFrame into database table"""
        try:
            with self.get_connection() as conn:
                # Create the full table name
                full_table_name = f"{schema}.{table_name}"
                
                # Insert data using pandas to_sql method with pyodbc
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                self.logger.info(f"Bulk insert completed for {full_table_name}, {len(df)} rows inserted")
                return True
                
        except Exception as e:
            self.logger.error(f"Bulk insert error: {str(e)}")
            raise
    
    def get_table_row_count(self, table_name: str, schema: str = 'dbo') -> int:
        """Get row count for a specific table"""
        try:
            query = f"SELECT COUNT(*) as row_count FROM {schema}.{table_name}"
            result = self.execute_query(query)
            return result[0]['row_count'] if result else 0
        except Exception as e:
            self.logger.error(f"Error getting row count for {schema}.{table_name}: {str(e)}")
            return 0
    
    def get_table_info(self, schema: str = None) -> List[Dict[str, Any]]:
        """Get information about tables in the database"""
        try:
            query = """
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            """
            
            if schema:
                query += f" AND TABLE_SCHEMA = '{schema}'"
            
            query += " ORDER BY TABLE_SCHEMA, TABLE_NAME"
            
            return self.execute_query(query)
        except Exception as e:
            self.logger.error(f"Error getting table info: {str(e)}")
            return []
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    self.logger.info("Database connection test successful")
                    return True
                else:
                    self.logger.error("Database connection test failed")
                    return False
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False
    
    def get_schema_list(self) -> List[str]:
        """Get list of all schemas in the database"""
        try:
            query = """
            SELECT DISTINCT SCHEMA_NAME 
            FROM INFORMATION_SCHEMA.SCHEMATA 
            WHERE SCHEMA_NAME NOT IN ('sys', 'information_schema')
            ORDER BY SCHEMA_NAME
            """
            results = self.execute_query(query)
            return [row['SCHEMA_NAME'] for row in results]
        except Exception as e:
            self.logger.error(f"Error getting schema list: {str(e)}")
            return []

# Global database manager instance
db_manager = DatabaseManager()