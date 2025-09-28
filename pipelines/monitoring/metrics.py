"""
Modern Data Warehouse - Metrics Collector
=========================================
Collects and manages pipeline performance metrics.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

class MetricsCollector:
    """Collects and manages pipeline performance metrics"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metrics = {}
        self.start_time = datetime.now()
        
    def add_metric(self, metric_name: str, value: Any, timestamp: datetime = None):
        """Add a metric value"""
        timestamp = timestamp or datetime.now()
        
        self.metrics[metric_name] = {
            'value': value,
            'timestamp': timestamp,
            'type': type(value).__name__
        }
        
        self.logger.debug(f"Metric added: {metric_name} = {value}")
    
    def get_metric(self, metric_name: str) -> Optional[Any]:
        """Get a specific metric value"""
        metric = self.metrics.get(metric_name)
        return metric['value'] if metric else None
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics as a dictionary"""
        return {name: metric['value'] for name, metric in self.metrics.items()}
    
    def get_metrics_with_timestamps(self) -> Dict[str, Dict[str, Any]]:
        """Get all metrics with their timestamps"""
        return self.metrics.copy()
    
    def calculate_pipeline_duration(self) -> float:
        """Calculate total pipeline duration"""
        duration = (datetime.now() - self.start_time).total_seconds()
        self.add_metric('pipeline_duration', duration)
        return duration
    
    def add_table_metrics(self, schema: str, table_counts: Dict[str, int]):
        """Add table-specific metrics"""
        for table_name, count in table_counts.items():
            metric_name = f"{schema}_{table_name}_rows"
            self.add_metric(metric_name, count)
    
    def add_performance_metrics(self, operation: str, duration: float, records_processed: int = 0):
        """Add performance metrics for an operation"""
        self.add_metric(f"{operation}_duration", duration)
        if records_processed > 0:
            self.add_metric(f"{operation}_records", records_processed)
            self.add_metric(f"{operation}_records_per_second", records_processed / duration if duration > 0 else 0)
    
    def add_error_metrics(self, error_type: str, error_count: int = 1):
        """Add error metrics"""
        current_count = self.get_metric(f"{error_type}_errors") or 0
        self.add_metric(f"{error_type}_errors", current_count + error_count)
    
    def calculate_throughput_metrics(self):
        """Calculate throughput metrics"""
        bronze_rows = self.get_metric('bronze_rows_loaded') or 0
        silver_rows = self.get_metric('silver_rows_processed') or 0
        total_duration = self.get_metric('pipeline_duration') or 1
        
        if bronze_rows > 0:
            self.add_metric('bronze_throughput_per_second', bronze_rows / total_duration)
        
        if silver_rows > 0:
            self.add_metric('silver_throughput_per_second', silver_rows / total_duration)
    
    def export_metrics_json(self, file_path: str = None) -> str:
        """Export metrics to JSON file"""
        if not file_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path = f"metrics_{timestamp}.json"
        
        try:
            # Prepare metrics for JSON serialization
            export_data = {
                'pipeline_run': {
                    'start_time': self.start_time.isoformat(),
                    'export_time': datetime.now().isoformat(),
                    'total_duration': self.get_metric('pipeline_duration')
                },
                'metrics': {}
            }
            
            for name, metric in self.metrics.items():
                export_data['metrics'][name] = {
                    'value': metric['value'],
                    'timestamp': metric['timestamp'].isoformat(),
                    'type': metric['type']
                }
            
            # Ensure directory exists
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            self.logger.info(f"Metrics exported to {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error exporting metrics: {str(e)}")
            return ""
    
    def get_summary_report(self) -> str:
        """Generate a summary report of all metrics"""
        report = []
        report.append("=" * 60)
        report.append("PIPELINE METRICS SUMMARY")
        report.append("=" * 60)
        report.append(f"Pipeline Start: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        total_duration = self.get_metric('pipeline_duration')
        if total_duration:
            report.append(f"Total Duration: {total_duration:.2f} seconds")
        
        report.append("")
        
        # Group metrics by category
        categories = {
            'Duration': [k for k in self.metrics.keys() if 'duration' in k],
            'Row Counts': [k for k in self.metrics.keys() if 'rows' in k and 'per_second' not in k],
            'Throughput': [k for k in self.metrics.keys() if 'per_second' in k or 'throughput' in k],
            'Quality': [k for k in self.metrics.keys() if 'quality' in k or 'score' in k],
            'Errors': [k for k in self.metrics.keys() if 'error' in k],
            'Other': [k for k in self.metrics.keys() if not any(cat in k.lower() for cat in ['duration', 'rows', 'per_second', 'throughput', 'quality', 'score', 'error'])]
        }
        
        for category, metric_names in categories.items():
            if metric_names:
                report.append(f"{category}:")
                report.append("-" * 30)
                
                for metric_name in sorted(metric_names):
                    value = self.get_metric(metric_name)
                    if value is not None:
                        if 'duration' in metric_name:
                            report.append(f"  {metric_name}: {value:.2f} seconds")
                        elif 'per_second' in metric_name or 'throughput' in metric_name:
                            report.append(f"  {metric_name}: {value:.2f}/sec")
                        elif isinstance(value, float):
                            report.append(f"  {metric_name}: {value:.2f}")
                        else:
                            report.append(f"  {metric_name}: {value:,}")
                
                report.append("")
        
        return "\n".join(report)
    
    def check_performance_thresholds(self, thresholds: Dict[str, float]) -> List[Dict[str, Any]]:
        """Check metrics against performance thresholds"""
        alerts = []
        
        for metric_name, threshold in thresholds.items():
            current_value = self.get_metric(metric_name)
            
            if current_value is not None:
                if current_value > threshold:
                    alerts.append({
                        'metric': metric_name,
                        'current_value': current_value,
                        'threshold': threshold,
                        'severity': 'WARNING' if current_value < threshold * 1.2 else 'CRITICAL',
                        'message': f"{metric_name} ({current_value}) exceeds threshold ({threshold})"
                    })
        
        return alerts
    
    def reset_metrics(self):
        """Reset all metrics"""
        self.metrics.clear()
        self.start_time = datetime.now()
        self.logger.info("Metrics reset")

class PipelineMonitor:
    """High-level pipeline monitoring and alerting"""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self.logger = logging.getLogger(__name__)
        self.alerts = []
    
    def check_pipeline_health(self) -> Dict[str, Any]:
        """Check overall pipeline health"""
        health_status = {
            'status': 'HEALTHY',
            'checks': [],
            'alerts': self.alerts.copy()
        }
        
        # Check if pipeline completed
        duration = self.metrics.get_metric('pipeline_duration')
        if duration:
            health_status['checks'].append({
                'name': 'Pipeline Completion',
                'status': 'PASS',
                'message': f'Pipeline completed in {duration:.2f} seconds'
            })
        else:
            health_status['checks'].append({
                'name': 'Pipeline Completion',
                'status': 'FAIL',
                'message': 'Pipeline did not complete successfully'
            })
            health_status['status'] = 'UNHEALTHY'
        
        # Check data quality score
        quality_score = self.metrics.get_metric('data_quality_score')
        if quality_score:
            if quality_score >= 0.95:
                health_status['checks'].append({
                    'name': 'Data Quality',
                    'status': 'PASS',
                    'message': f'Data quality score: {quality_score:.1%}'
                })
            else:
                health_status['checks'].append({
                    'name': 'Data Quality',
                    'status': 'WARNING',
                    'message': f'Data quality score below threshold: {quality_score:.1%}'
                })
                health_status['status'] = 'WARNING'
        
        # Check for errors
        error_metrics = [k for k in self.metrics.get_all_metrics().keys() if 'error' in k]
        total_errors = sum(self.metrics.get_metric(k) or 0 for k in error_metrics)
        
        if total_errors == 0:
            health_status['checks'].append({
                'name': 'Error Count',
                'status': 'PASS',
                'message': 'No errors detected'
            })
        else:
            health_status['checks'].append({
                'name': 'Error Count',
                'status': 'FAIL',
                'message': f'{total_errors} errors detected'
            })
            health_status['status'] = 'UNHEALTHY'
        
        return health_status
    
    def add_alert(self, severity: str, message: str, metric_name: str = None):
        """Add an alert"""
        alert = {
            'timestamp': datetime.now(),
            'severity': severity,
            'message': message,
            'metric': metric_name
        }
        self.alerts.append(alert)
        self.logger.warning(f"Alert added: {severity} - {message}")
    
    def get_alerts(self, severity: str = None) -> List[Dict[str, Any]]:
        """Get alerts, optionally filtered by severity"""
        if severity:
            return [alert for alert in self.alerts if alert['severity'] == severity]
        return self.alerts.copy()
    
    def clear_alerts(self):
        """Clear all alerts"""
        self.alerts.clear()
        self.logger.info("All alerts cleared")