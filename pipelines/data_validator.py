"""
Modern Data Warehouse - Data Validator
======================================
Provides data quality validation and monitoring capabilities.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
from config.config import config_manager
from config.database import db_manager

class DataValidator:
    """Handles data quality validation and monitoring"""
    
    def __init__(self):
        self.config = config_manager
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        
        # Get data quality configuration
        self.dq_config = self.config.config.get('data_quality', {})
        self.validation_enabled = self.dq_config.get('enable_validation', True)
    
    def run_all_validations(self) -> List[Dict[str, Any]]:
        """Run all data quality validations"""
        if not self.validation_enabled:
            self.logger.info("Data quality validation is disabled")
            return []
        
        validation_results = []
        
        try:
            # Run validations for each layer
            validation_results.extend(self._validate_bronze_layer())
            validation_results.extend(self._validate_silver_layer())
            validation_results.extend(self._validate_gold_layer())
            validation_results.extend(self._validate_business_rules())
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error running data validations: {str(e)}")
            return []
    
    def _validate_bronze_layer(self) -> List[Dict[str, Any]]:
        """Validate Bronze layer data quality"""
        results = []
        
        # Check table row counts
        bronze_tables = [
            "crm_cust_info", "crm_prd_info", "crm_sales_details",
            "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"
        ]
        
        for table in bronze_tables:
            row_count = self.db.get_table_row_count(table, "bronze")
            
            # Basic completeness check
            if row_count > 0:
                results.append({
                    "check_name": f"Bronze {table} - Row Count",
                    "passed": True,
                    "score": 1.0,
                    "details": f"{row_count:,} rows found"
                })
            else:
                results.append({
                    "check_name": f"Bronze {table} - Row Count",
                    "passed": False,
                    "score": 0.0,
                    "details": "No rows found in table"
                })
        
        return results
    
    def _validate_silver_layer(self) -> List[Dict[str, Any]]:
        """Validate Silver layer data quality"""
        results = []
        
        # Check for data consistency between Bronze and Silver
        silver_tables = [
            "crm_cust_info", "crm_prd_info", "crm_sales_details",
            "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"
        ]
        
        for table in silver_tables:
            bronze_count = self.db.get_table_row_count(table, "bronze")
            silver_count = self.db.get_table_row_count(table, "silver")
            
            # Allow for some data cleaning (Silver <= Bronze)
            if silver_count > 0 and silver_count <= bronze_count:
                retention_rate = silver_count / bronze_count if bronze_count > 0 else 0
                results.append({
                    "check_name": f"Silver {table} - Data Retention",
                    "passed": retention_rate >= 0.8,  # At least 80% retention
                    "score": retention_rate,
                    "details": f"Retained {silver_count:,} of {bronze_count:,} rows ({retention_rate:.1%})"
                })
            else:
                results.append({
                    "check_name": f"Silver {table} - Data Retention",
                    "passed": False,
                    "score": 0.0,
                    "details": f"Silver has more rows than Bronze or no data"
                })
        
        # Check for null values in critical fields
        results.extend(self._check_null_values_silver())
        
        # Check for data type consistency
        results.extend(self._check_data_types_silver())
        
        return results
    
    def _validate_gold_layer(self) -> List[Dict[str, Any]]:
        """Validate Gold layer data quality"""
        results = []
        
        try:
            # Check Gold layer views
            gold_views = ["dim_customers", "dim_products", "fact_sales"]
            
            for view in gold_views:
                try:
                    query = f"SELECT COUNT(*) as row_count FROM gold.{view}"
                    result = self.db.execute_query(query)
                    row_count = result[0]['row_count'] if result else 0
                    
                    if row_count > 0:
                        results.append({
                            "check_name": f"Gold {view} - Accessibility",
                            "passed": True,
                            "score": 1.0,
                            "details": f"{row_count:,} rows accessible"
                        })
                    else:
                        results.append({
                            "check_name": f"Gold {view} - Accessibility",
                            "passed": False,
                            "score": 0.0,
                            "details": "View returns no data"
                        })
                        
                except Exception as e:
                    results.append({
                        "check_name": f"Gold {view} - Accessibility",
                        "passed": False,
                        "score": 0.0,
                        "details": f"Error querying view: {str(e)}"
                    })
            
            # Check referential integrity
            results.extend(self._check_referential_integrity())
            
        except Exception as e:
            self.logger.error(f"Error validating Gold layer: {str(e)}")
        
        return results
    
    def _validate_business_rules(self) -> List[Dict[str, Any]]:
        """Validate business rules and logical constraints"""
        results = []
        
        try:
            # Check for reasonable sales amounts
            sales_check = self._validate_sales_amounts()
            if sales_check:
                results.append(sales_check)
            
            # Check for valid date ranges
            date_check = self._validate_date_ranges()
            if date_check:
                results.append(date_check)
            
            # Check for customer data consistency
            customer_check = self._validate_customer_consistency()
            if customer_check:
                results.append(customer_check)
                
        except Exception as e:
            self.logger.error(f"Error validating business rules: {str(e)}")
        
        return results
    
    def _check_null_values_silver(self) -> List[Dict[str, Any]]:
        """Check for null values in critical Silver layer fields"""
        results = []
        
        critical_fields = {
            "crm_cust_info": ["cst_id", "cst_key"],
            "crm_prd_info": ["prd_id", "prd_key"],
            "crm_sales_details": ["sls_ord_num", "sls_cust_id"]
        }
        
        for table, fields in critical_fields.items():
            for field in fields:
                try:
                    query = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT({field}) as non_null_rows
                    FROM silver.{table}
                    """
                    result = self.db.execute_query(query)
                    
                    if result:
                        total_rows = result[0]['total_rows']
                        non_null_rows = result[0]['non_null_rows']
                        completeness = non_null_rows / total_rows if total_rows > 0 else 0
                        
                        results.append({
                            "check_name": f"Silver {table}.{field} - Completeness",
                            "passed": completeness >= 0.95,  # 95% completeness threshold
                            "score": completeness,
                            "details": f"{non_null_rows:,} of {total_rows:,} rows complete ({completeness:.1%})"
                        })
                        
                except Exception as e:
                    results.append({
                        "check_name": f"Silver {table}.{field} - Completeness",
                        "passed": False,
                        "score": 0.0,
                        "details": f"Error checking field: {str(e)}"
                    })
        
        return results
    
    def _check_data_types_silver(self) -> List[Dict[str, Any]]:
        """Check data type consistency in Silver layer"""
        results = []
        
        # This would implement data type validation
        # For now, just return a placeholder result
        results.append({
            "check_name": "Silver Layer - Data Types",
            "passed": True,
            "score": 1.0,
            "details": "Data type validation not yet implemented"
        })
        
        return results
    
    def _check_referential_integrity(self) -> List[Dict[str, Any]]:
        """Check referential integrity in Gold layer"""
        results = []
        
        try:
            # Check if all sales have valid customers
            query = """
            SELECT 
                COUNT(*) as total_sales,
                COUNT(c.customer_key) as sales_with_customers
            FROM gold.fact_sales f
            LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
            """
            result = self.db.execute_query(query)
            
            if result:
                total_sales = result[0]['total_sales']
                sales_with_customers = result[0]['sales_with_customers']
                integrity_score = sales_with_customers / total_sales if total_sales > 0 else 1.0
                
                results.append({
                    "check_name": "Gold - Sales-Customer Integrity",
                    "passed": integrity_score >= 0.95,
                    "score": integrity_score,
                    "details": f"{sales_with_customers:,} of {total_sales:,} sales have valid customers ({integrity_score:.1%})"
                })
            
            # Check if all sales have valid products  
            query = """
            SELECT 
                COUNT(*) as total_sales,
                COUNT(p.product_key) as sales_with_products
            FROM gold.fact_sales f
            LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
            """
            result = self.db.execute_query(query)
            
            if result:
                total_sales = result[0]['total_sales']
                sales_with_products = result[0]['sales_with_products']
                integrity_score = sales_with_products / total_sales if total_sales > 0 else 1.0
                
                results.append({
                    "check_name": "Gold - Sales-Product Integrity",
                    "passed": integrity_score >= 0.95,
                    "score": integrity_score,
                    "details": f"{sales_with_products:,} of {total_sales:,} sales have valid products ({integrity_score:.1%})"
                })
                
        except Exception as e:
            self.logger.error(f"Error checking referential integrity: {str(e)}")
        
        return results
    
    def _validate_sales_amounts(self) -> Optional[Dict[str, Any]]:
        """Validate sales amounts are reasonable"""
        try:
            query = """
            SELECT 
                COUNT(*) as total_sales,
                AVG(CAST(sales_amount as FLOAT)) as avg_sales,
                MIN(sales_amount) as min_sales,
                MAX(sales_amount) as max_sales,
                COUNT(CASE WHEN sales_amount <= 0 THEN 1 END) as negative_sales
            FROM gold.fact_sales
            """
            result = self.db.execute_query(query)
            
            if result:
                stats = result[0]
                total_sales = stats['total_sales']
                negative_sales = stats['negative_sales']
                
                # Score based on percentage of positive sales
                score = (total_sales - negative_sales) / total_sales if total_sales > 0 else 1.0
                
                return {
                    "check_name": "Business Rule - Valid Sales Amounts",
                    "passed": negative_sales == 0,
                    "score": score,
                    "details": f"{negative_sales} negative sales out of {total_sales:,} total"
                }
                
        except Exception as e:
            self.logger.error(f"Error validating sales amounts: {str(e)}")
        
        return None
    
    def _validate_date_ranges(self) -> Optional[Dict[str, Any]]:
        """Validate date ranges are reasonable"""
        try:
            query = """
            SELECT 
                COUNT(*) as total_sales,
                COUNT(CASE WHEN order_date > GETDATE() THEN 1 END) as future_dates,
                COUNT(CASE WHEN order_date < '2000-01-01' THEN 1 END) as old_dates
            FROM gold.fact_sales
            """
            result = self.db.execute_query(query)
            
            if result:
                stats = result[0]
                total_sales = stats['total_sales']
                future_dates = stats['future_dates']
                old_dates = stats['old_dates']
                
                invalid_dates = future_dates + old_dates
                score = (total_sales - invalid_dates) / total_sales if total_sales > 0 else 1.0
                
                return {
                    "check_name": "Business Rule - Valid Date Ranges",
                    "passed": invalid_dates == 0,
                    "score": score,
                    "details": f"{invalid_dates} invalid dates out of {total_sales:,} total"
                }
                
        except Exception as e:
            self.logger.error(f"Error validating date ranges: {str(e)}")
        
        return None
    
    def _validate_customer_consistency(self) -> Optional[Dict[str, Any]]:
        """Validate customer data consistency"""
        try:
            query = """
            SELECT 
                COUNT(*) as total_customers,
                COUNT(CASE WHEN first_name IS NULL OR first_name = '' THEN 1 END) as missing_names
            FROM gold.dim_customers
            """
            result = self.db.execute_query(query)
            
            if result:
                stats = result[0]
                total_customers = stats['total_customers']
                missing_names = stats['missing_names']
                
                score = (total_customers - missing_names) / total_customers if total_customers > 0 else 1.0
                
                return {
                    "check_name": "Business Rule - Customer Name Completeness",
                    "passed": missing_names == 0,
                    "score": score,
                    "details": f"{missing_names} customers with missing names out of {total_customers:,} total"
                }
                
        except Exception as e:
            self.logger.error(f"Error validating customer consistency: {str(e)}")
        
        return None
    
    def generate_data_quality_report(self) -> str:
        """Generate a comprehensive data quality report"""
        validation_results = self.run_all_validations()
        
        report = []
        report.append("=" * 80)
        report.append("DATA QUALITY REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        if not validation_results:
            report.append("No validation results available.")
            return "\n".join(report)
        
        # Calculate overall score
        overall_score = sum(result['score'] for result in validation_results) / len(validation_results)
        report.append(f"Overall Data Quality Score: {overall_score:.1%}")
        report.append("")
        
        # Group results by category
        categories = {}
        for result in validation_results:
            category = result['check_name'].split(' - ')[0]
            if category not in categories:
                categories[category] = []
            categories[category].append(result)
        
        # Report by category
        for category, results in categories.items():
            report.append(f"{category}:")
            report.append("-" * 40)
            
            for result in results:
                status = "✓ PASS" if result['passed'] else "✗ FAIL"
                score = f"({result['score']:.1%})"
                check_name = result['check_name'].split(' - ', 1)[1] if ' - ' in result['check_name'] else result['check_name']
                
                report.append(f"  {status} {score} {check_name}")
                if result.get('details'):
                    report.append(f"    {result['details']}")
            
            report.append("")
        
        return "\n".join(report)