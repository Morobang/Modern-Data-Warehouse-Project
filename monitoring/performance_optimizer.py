"""
Modern Data Warehouse - Performance Optimizer
=============================================
Analyzes and optimizes data warehouse performance through indexing,
partitioning, and query optimization recommendations.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import json

@dataclass
class PerformanceMetric:
    """Performance metric data structure"""
    metric_name: str
    value: float
    unit: str
    timestamp: str
    table_name: Optional[str] = None
    query_id: Optional[str] = None

@dataclass
class IndexRecommendation:
    """Index recommendation data structure"""
    table_name: str
    index_type: str  # 'clustered', 'nonclustered', 'columnstore'
    columns: List[str]
    reason: str
    expected_benefit: str
    creation_script: str

@dataclass
class PartitionRecommendation:
    """Partition recommendation data structure"""
    table_name: str
    partition_column: str
    partition_strategy: str  # 'range', 'hash', 'list'
    partition_count: int
    reason: str
    creation_script: str

class PerformanceOptimizer:
    """Analyzes and optimizes data warehouse performance"""
    
    def __init__(self, db_manager=None):
        from pipelines.config.database import db_manager as default_db_manager
        self.db = db_manager or default_db_manager
        self.logger = logging.getLogger(__name__)
        
        # Performance thresholds
        self.thresholds = {
            'slow_query_duration': 5.0,  # seconds
            'high_cpu_percentage': 80.0,  # percent
            'high_io_wait': 100.0,        # milliseconds
            'large_table_rows': 1000000,  # rows
            'index_scan_ratio': 0.9       # ratio of index seeks to scans
        }
    
    def analyze_table_performance(self) -> List[Dict[str, Any]]:
        """Analyze performance of all tables in the warehouse"""
        performance_analysis = []
        
        try:
            # Get table statistics
            query = """
            SELECT 
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                p.rows AS row_count,
                p.reserved AS reserved_space_kb,
                p.data AS data_space_kb,
                p.index_size AS index_space_kb,
                p.unused AS unused_space_kb
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN (
                SELECT 
                    s.name AS schema_name,
                    t.name AS table_name,
                    SUM(p.rows) AS rows,
                    SUM(a.total_pages) * 8 AS reserved,
                    SUM(a.used_pages) * 8 AS data,
                    (SUM(a.used_pages) - SUM(p.rows)) * 8 AS index_size,
                    (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS unused
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.partitions p ON t.object_id = p.object_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                WHERE p.index_id <= 1
                GROUP BY s.name, t.name
            ) p ON t.TABLE_SCHEMA = p.schema_name AND t.TABLE_NAME = p.table_name
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY p.rows DESC
            """
            
            results = self.db.execute_query(query)
            
            for row in results:
                table_key = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
                row_count = row['row_count'] or 0
                
                analysis = {
                    'table': table_key,
                    'row_count': row_count,
                    'reserved_space_mb': (row['reserved_space_kb'] or 0) / 1024,
                    'data_space_mb': (row['data_space_kb'] or 0) / 1024,
                    'index_space_mb': (row['index_space_kb'] or 0) / 1024,
                    'unused_space_mb': (row['unused_space_kb'] or 0) / 1024,
                    'performance_issues': [],
                    'recommendations': []
                }
                
                # Analyze performance issues
                if row_count > self.thresholds['large_table_rows']:
                    analysis['performance_issues'].append('Large table - consider partitioning')
                    analysis['recommendations'].append(self._generate_partition_recommendation(table_key))
                
                # Check for missing indexes (simplified check)
                index_recommendations = self._analyze_missing_indexes(table_key)
                if index_recommendations:
                    analysis['recommendations'].extend(index_recommendations)
                
                performance_analysis.append(analysis)
            
            return performance_analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing table performance: {str(e)}")
            return []
    
    def _analyze_missing_indexes(self, table_name: str) -> List[IndexRecommendation]:
        """Analyze missing indexes for a table"""
        recommendations = []
        
        try:
            schema, table = table_name.split('.')
            
            # Check if table has clustered index
            clustered_check_query = """
            SELECT COUNT(*) as clustered_count
            FROM sys.indexes i
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ? AND i.type = 1
            """
            
            result = self.db.execute_query(clustered_check_query, (schema, table))
            
            if result and result[0]['clustered_count'] == 0:
                # Table doesn't have clustered index
                primary_key_columns = self._get_primary_key_columns(table_name)
                
                if primary_key_columns:
                    recommendations.append(IndexRecommendation(
                        table_name=table_name,
                        index_type='clustered',
                        columns=primary_key_columns,
                        reason='Table lacks clustered index',
                        expected_benefit='Improved query performance and data organization',
                        creation_script=f"CREATE CLUSTERED INDEX IX_{table}_clustered ON {table_name} ({', '.join(primary_key_columns)})"
                    ))
            
            # Recommend common indexes based on table type and schema
            if schema == 'silver' and 'sales' in table.lower():
                recommendations.append(IndexRecommendation(
                    table_name=table_name,
                    index_type='nonclustered',
                    columns=['sls_cust_id', 'sls_order_dt'],
                    reason='Frequently queried columns for sales analysis',
                    expected_benefit='Faster customer and date-based queries',
                    creation_script=f"CREATE NONCLUSTERED INDEX IX_{table}_customer_date ON {table_name} (sls_cust_id, sls_order_dt)"
                ))
            
            if schema == 'gold' and 'fact' in table.lower():
                recommendations.append(IndexRecommendation(
                    table_name=table_name,
                    index_type='columnstore',
                    columns=['*'],
                    reason='Fact table benefits from columnstore compression',
                    expected_benefit='Better compression and analytical query performance',
                    creation_script=f"CREATE NONCLUSTERED COLUMNSTORE INDEX IX_{table}_columnstore ON {table_name}"
                ))
            
        except Exception as e:
            self.logger.error(f"Error analyzing missing indexes for {table_name}: {str(e)}")
        
        return recommendations
    
    def _get_primary_key_columns(self, table_name: str) -> List[str]:
        """Get primary key columns for a table"""
        try:
            schema, table = table_name.split('.')
            
            query = """
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
            INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON ccu.COLUMN_NAME = c.COLUMN_NAME AND ccu.TABLE_NAME = c.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY c.ORDINAL_POSITION
            """
            
            results = self.db.execute_query(query, (schema, table))
            return [row['COLUMN_NAME'] for row in results] if results else []
            
        except Exception as e:
            self.logger.error(f"Error getting primary key columns for {table_name}: {str(e)}")
            return []
    
    def _generate_partition_recommendation(self, table_name: str) -> PartitionRecommendation:
        """Generate partition recommendation for large tables"""
        schema, table = table_name.split('.')
        
        # Determine partition column based on table type
        partition_column = 'dwh_create_date'  # Default for Silver tables
        
        if 'sales' in table.lower():
            if schema == 'silver':
                partition_column = 'sls_order_dt'
            elif schema == 'gold':
                partition_column = 'order_date'
        elif 'customer' in table.lower():
            partition_column = 'cst_create_date' if schema == 'silver' else 'create_date'
        
        return PartitionRecommendation(
            table_name=table_name,
            partition_column=partition_column,
            partition_strategy='range',
            partition_count=12,  # Monthly partitions
            reason='Large table benefits from date-based partitioning',
            creation_script=f"""
-- Create partition function and scheme for {table_name}
CREATE PARTITION FUNCTION PF_{table}_Monthly (DATE)
AS RANGE RIGHT FOR VALUES (
    '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01',
    '2023-05-01', '2023-06-01', '2023-07-01', '2023-08-01',
    '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01'
);

CREATE PARTITION SCHEME PS_{table}_Monthly
AS PARTITION PF_{table}_Monthly ALL TO ([PRIMARY]);

-- Apply to existing table (requires rebuilding)
CREATE CLUSTERED INDEX IX_{table}_partitioned ON {table_name} ({partition_column})
ON PS_{table}_Monthly ({partition_column});
            """.strip()
        )
    
    def analyze_query_performance(self) -> List[Dict[str, Any]]:
        """Analyze slow-running queries"""
        slow_queries = []
        
        try:
            # Query to find slow queries from query store or DMVs
            query = """
            SELECT TOP 20
                qs.sql_handle,
                qs.execution_count,
                qs.total_elapsed_time / 1000000.0 AS total_elapsed_time_seconds,
                qs.total_elapsed_time / qs.execution_count / 1000000.0 AS avg_elapsed_time_seconds,
                qs.total_cpu_time / 1000000.0 AS total_cpu_time_seconds,
                qs.total_physical_reads,
                qs.total_logical_reads,
                SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
                    ((CASE qs.statement_end_offset
                        WHEN -1 THEN DATALENGTH(qt.text)
                        ELSE qs.statement_end_offset
                    END - qs.statement_start_offset)/2)+1) AS query_text
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
            WHERE qs.total_elapsed_time / qs.execution_count / 1000000.0 > ?
            ORDER BY qs.total_elapsed_time / qs.execution_count DESC
            """
            
            results = self.db.execute_query(query, (self.thresholds['slow_query_duration'],))
            
            for row in results:
                slow_queries.append({
                    'query_text': row['query_text'][:200] + '...' if len(row['query_text']) > 200 else row['query_text'],
                    'execution_count': row['execution_count'],
                    'avg_duration_seconds': round(row['avg_elapsed_time_seconds'], 3),
                    'total_duration_seconds': round(row['total_elapsed_time_seconds'], 3),
                    'total_cpu_seconds': round(row['total_cpu_time_seconds'], 3),
                    'total_physical_reads': row['total_physical_reads'],
                    'total_logical_reads': row['total_logical_reads'],
                    'optimization_suggestions': self._generate_query_optimization_suggestions(row)
                })
            
        except Exception as e:
            self.logger.error(f"Error analyzing query performance: {str(e)}")
        
        return slow_queries
    
    def _generate_query_optimization_suggestions(self, query_stats: Dict[str, Any]) -> List[str]:
        """Generate optimization suggestions for a slow query"""
        suggestions = []
        
        # High CPU usage
        if query_stats['total_cpu_seconds'] > 10:
            suggestions.append("Consider adding appropriate indexes to reduce CPU usage")
        
        # High physical reads
        if query_stats['total_physical_reads'] > 1000000:
            suggestions.append("High physical reads - consider index optimization or query rewriting")
        
        # High logical reads
        if query_stats['total_logical_reads'] > 10000000:
            suggestions.append("High logical reads - review query for unnecessary data access")
        
        # Frequent execution with slow performance
        if query_stats['execution_count'] > 1000 and query_stats['avg_elapsed_time_seconds'] > 1:
            suggestions.append("Frequently executed slow query - high priority for optimization")
        
        return suggestions
    
    def generate_optimization_report(self) -> str:
        """Generate a comprehensive performance optimization report"""
        report = []
        report.append("=" * 80)
        report.append("PERFORMANCE OPTIMIZATION REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Table performance analysis
        table_analysis = self.analyze_table_performance()
        
        if table_analysis:
            report.append("TABLE PERFORMANCE ANALYSIS:")
            report.append("-" * 50)
            
            for analysis in table_analysis:
                report.append(f"\n{analysis['table']}:")
                report.append(f"  Rows: {analysis['row_count']:,}")
                report.append(f"  Data Size: {analysis['data_space_mb']:.1f} MB")
                report.append(f"  Index Size: {analysis['index_space_mb']:.1f} MB")
                
                if analysis['performance_issues']:
                    report.append("  Issues:")
                    for issue in analysis['performance_issues']:
                        report.append(f"    • {issue}")
                
                if analysis['recommendations']:
                    report.append("  Recommendations:")
                    for rec in analysis['recommendations']:
                        if isinstance(rec, IndexRecommendation):
                            report.append(f"    • Index: {rec.index_type} on {', '.join(rec.columns)}")
                            report.append(f"      Reason: {rec.reason}")
                        elif isinstance(rec, PartitionRecommendation):
                            report.append(f"    • Partition: {rec.partition_strategy} on {rec.partition_column}")
                            report.append(f"      Reason: {rec.reason}")
        
        # Query performance analysis
        slow_queries = self.analyze_query_performance()
        
        if slow_queries:
            report.append("\n\nSLOW QUERY ANALYSIS:")
            report.append("-" * 50)
            
            for i, query in enumerate(slow_queries[:5], 1):  # Top 5 slow queries
                report.append(f"\n{i}. Slow Query:")
                report.append(f"   Average Duration: {query['avg_duration_seconds']} seconds")
                report.append(f"   Execution Count: {query['execution_count']:,}")
                report.append(f"   Query: {query['query_text']}")
                
                if query['optimization_suggestions']:
                    report.append("   Suggestions:")
                    for suggestion in query['optimization_suggestions']:
                        report.append(f"     • {suggestion}")
        
        # Overall recommendations
        report.append("\n\nOVERALL RECOMMENDATIONS:")
        report.append("-" * 50)
        report.append("• Implement recommended indexes to improve query performance")
        report.append("• Consider partitioning large tables for better maintenance")
        report.append("• Monitor query execution plans for optimization opportunities")
        report.append("• Regular statistics updates for optimal query plans")
        report.append("• Consider columnstore indexes for analytical workloads")
        
        return "\n".join(report)
    
    def generate_index_creation_scripts(self) -> str:
        """Generate SQL scripts for all recommended indexes"""
        scripts = []
        scripts.append("-- Performance Optimization Index Creation Scripts")
        scripts.append("-- Generated: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        scripts.append("")
        
        table_analysis = self.analyze_table_performance()
        
        for analysis in table_analysis:
            if analysis['recommendations']:
                scripts.append(f"-- Indexes for {analysis['table']}")
                scripts.append("-" * 50)
                
                for rec in analysis['recommendations']:
                    if isinstance(rec, IndexRecommendation):
                        scripts.append(f"-- {rec.reason}")
                        scripts.append(rec.creation_script + ";")
                        scripts.append("")
                    elif isinstance(rec, PartitionRecommendation):
                        scripts.append(f"-- {rec.reason}")
                        scripts.append(rec.creation_script)
                        scripts.append("")
        
        return "\n".join(scripts)
    
    def collect_performance_metrics(self) -> List[PerformanceMetric]:
        """Collect current performance metrics"""
        metrics = []
        timestamp = datetime.now().isoformat()
        
        try:
            # Database size metrics
            size_query = """
            SELECT 
                name AS database_name,
                size * 8.0 / 1024 AS size_mb,
                max_size * 8.0 / 1024 AS max_size_mb
            FROM sys.database_files
            WHERE type = 0  -- Data files
            """
            
            results = self.db.execute_query(size_query)
            for row in results:
                metrics.append(PerformanceMetric(
                    metric_name='database_size_mb',
                    value=row['size_mb'],
                    unit='MB',
                    timestamp=timestamp
                ))
            
            # Wait statistics
            wait_stats_query = """
            SELECT TOP 10
                wait_type,
                wait_time_ms / 1000.0 AS wait_time_seconds,
                waiting_tasks_count
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT IN ('CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE',
                'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR',
                'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH',
                'XE_TIMER_EVENT', 'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT',
                'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
                'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP')
            ORDER BY wait_time_ms DESC
            """
            
            wait_results = self.db.execute_query(wait_stats_query)
            for row in wait_results:
                metrics.append(PerformanceMetric(
                    metric_name=f'wait_time_{row["wait_type"].lower()}',
                    value=row['wait_time_seconds'],
                    unit='seconds',
                    timestamp=timestamp
                ))
            
        except Exception as e:
            self.logger.error(f"Error collecting performance metrics: {str(e)}")
        
        return metrics

def main():
    """Demo the performance optimizer"""
    optimizer = PerformanceOptimizer()
    
    # Generate optimization report
    report = optimizer.generate_optimization_report()
    print(report)
    
    # Generate index creation scripts
    scripts = optimizer.generate_index_creation_scripts()
    
    # Save scripts to file
    with open('performance_optimization_scripts.sql', 'w') as f:
        f.write(scripts)
    
    print(f"\nIndex creation scripts saved to performance_optimization_scripts.sql")
    
    # Collect metrics
    metrics = optimizer.collect_performance_metrics()
    print(f"Collected {len(metrics)} performance metrics")

if __name__ == "__main__":
    main()