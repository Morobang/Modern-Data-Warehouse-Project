"""
Modern Data Warehouse - Data Quality Dashboard
==============================================
A web-based dashboard for monitoring data quality metrics and pipeline health.
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

try:
    from flask import Flask, render_template, jsonify, request
    import plotly.graph_objs as go
    import plotly.utils
    import pandas as pd
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

class DataQualityDashboard:
    """Web-based data quality monitoring dashboard"""
    
    def __init__(self, db_path: str = "monitoring/quality_metrics.db"):
        self.db_path = db_path
        self.app = None
        
        if FLASK_AVAILABLE:
            self.app = Flask(__name__)
            self._setup_routes()
        
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database for storing metrics"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create tables for storing metrics
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    duration_seconds REAL,
                    total_records INTEGER,
                    data_quality_score REAL,
                    error_count INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    check_name TEXT NOT NULL,
                    check_category TEXT,
                    passed BOOLEAN NOT NULL,
                    score REAL NOT NULL,
                    details TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (run_id) REFERENCES pipeline_runs (id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS table_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    schema_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (run_id) REFERENCES pipeline_runs (id)
                )
            """)
            
            conn.commit()
    
    def store_pipeline_run(self, run_data: Dict[str, Any]) -> int:
        """Store pipeline run data and return run ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO pipeline_runs 
                (run_timestamp, status, duration_seconds, total_records, data_quality_score, error_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                run_data.get('timestamp', datetime.now().isoformat()),
                run_data.get('status', 'UNKNOWN'),
                run_data.get('duration', 0),
                run_data.get('total_records', 0),
                run_data.get('data_quality_score', 0),
                run_data.get('error_count', 0)
            ))
            
            run_id = cursor.lastrowid
            conn.commit()
            return run_id
    
    def store_quality_checks(self, run_id: int, quality_results: List[Dict[str, Any]]):
        """Store data quality check results"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for result in quality_results:
                cursor.execute("""
                    INSERT INTO data_quality_checks 
                    (run_id, check_name, check_category, passed, score, details)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    run_id,
                    result.get('check_name', ''),
                    result.get('category', 'General'),
                    result.get('passed', False),
                    result.get('score', 0.0),
                    result.get('details', '')
                ))
            
            conn.commit()
    
    def store_table_metrics(self, run_id: int, table_counts: Dict[str, Dict[str, int]]):
        """Store table row count metrics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for schema, tables in table_counts.items():
                for table_name, row_count in tables.items():
                    cursor.execute("""
                        INSERT INTO table_metrics 
                        (run_id, schema_name, table_name, row_count)
                        VALUES (?, ?, ?, ?)
                    """, (run_id, schema, table_name, row_count))
            
            conn.commit()
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for the dashboard"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get latest pipeline runs
            cursor.execute("""
                SELECT * FROM pipeline_runs 
                ORDER BY created_at DESC 
                LIMIT 10
            """)
            recent_runs = [dict(row) for row in cursor.fetchall()]
            
            # Get quality score trend
            cursor.execute("""
                SELECT run_timestamp, data_quality_score, status
                FROM pipeline_runs 
                WHERE data_quality_score IS NOT NULL
                ORDER BY created_at DESC 
                LIMIT 30
            """)
            quality_trend = [dict(row) for row in cursor.fetchall()]
            
            # Get latest quality checks
            cursor.execute("""
                SELECT dqc.*, pr.run_timestamp
                FROM data_quality_checks dqc
                JOIN pipeline_runs pr ON dqc.run_id = pr.id
                WHERE pr.id = (SELECT MAX(id) FROM pipeline_runs)
            """)
            latest_checks = [dict(row) for row in cursor.fetchall()]
            
            # Get table size trends
            cursor.execute("""
                SELECT tm.schema_name, tm.table_name, tm.row_count, pr.run_timestamp
                FROM table_metrics tm
                JOIN pipeline_runs pr ON tm.run_id = pr.id
                ORDER BY pr.created_at DESC, tm.schema_name, tm.table_name
                LIMIT 100
            """)
            table_trends = [dict(row) for row in cursor.fetchall()]
            
            return {
                'recent_runs': recent_runs,
                'quality_trend': quality_trend,
                'latest_checks': latest_checks,
                'table_trends': table_trends,
                'summary': self._get_summary_stats()
            }
    
    def _get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Total runs
            cursor.execute("SELECT COUNT(*) FROM pipeline_runs")
            total_runs = cursor.fetchone()[0]
            
            # Success rate
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as successful
                FROM pipeline_runs
                WHERE created_at >= datetime('now', '-30 days')
            """)
            result = cursor.fetchone()
            success_rate = (result[1] / result[0] * 100) if result[0] > 0 else 0
            
            # Average quality score
            cursor.execute("""
                SELECT AVG(data_quality_score) 
                FROM pipeline_runs 
                WHERE data_quality_score IS NOT NULL 
                AND created_at >= datetime('now', '-30 days')
            """)
            avg_quality_score = cursor.fetchone()[0] or 0
            
            # Latest run status
            cursor.execute("""
                SELECT status, run_timestamp 
                FROM pipeline_runs 
                ORDER BY created_at DESC 
                LIMIT 1
            """)
            latest_run = cursor.fetchone()
            
            return {
                'total_runs': total_runs,
                'success_rate': round(success_rate, 1),
                'avg_quality_score': round(avg_quality_score * 100, 1) if avg_quality_score else 0,
                'latest_status': latest_run[0] if latest_run else 'UNKNOWN',
                'latest_timestamp': latest_run[1] if latest_run else None
            }
    
    def _setup_routes(self):
        """Setup Flask routes for the dashboard"""
        if not self.app:
            return
        
        @self.app.route('/')
        def dashboard():
            """Main dashboard page"""
            return render_template('dashboard.html')
        
        @self.app.route('/api/dashboard-data')
        def api_dashboard_data():
            """API endpoint for dashboard data"""
            return jsonify(self.get_dashboard_data())
        
        @self.app.route('/api/quality-trend-chart')
        def api_quality_trend_chart():
            """API endpoint for quality trend chart"""
            data = self.get_dashboard_data()
            quality_trend = data['quality_trend']
            
            if not quality_trend:
                return jsonify({})
            
            # Create Plotly chart
            df = pd.DataFrame(quality_trend)
            df['run_timestamp'] = pd.to_datetime(df['run_timestamp'])
            df['data_quality_score'] = df['data_quality_score'] * 100  # Convert to percentage
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df['run_timestamp'],
                y=df['data_quality_score'],
                mode='lines+markers',
                name='Data Quality Score',
                line=dict(color='#2E86AB', width=3),
                marker=dict(size=8)
            ))
            
            fig.update_layout(
                title='Data Quality Trend (Last 30 Runs)',
                xaxis_title='Run Date',
                yaxis_title='Quality Score (%)',
                yaxis=dict(range=[0, 100]),
                template='plotly_white',
                height=400
            )
            
            return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        @self.app.route('/api/table-size-chart')
        def api_table_size_chart():
            """API endpoint for table size chart"""
            data = self.get_dashboard_data()
            table_trends = data['table_trends']
            
            if not table_trends:
                return jsonify({})
            
            # Get latest run data
            df = pd.DataFrame(table_trends)
            latest_run = df['run_timestamp'].max()
            latest_data = df[df['run_timestamp'] == latest_run]
            
            fig = go.Figure()
            
            for schema in latest_data['schema_name'].unique():
                schema_data = latest_data[latest_data['schema_name'] == schema]
                
                fig.add_trace(go.Bar(
                    name=schema.title(),
                    x=schema_data['table_name'],
                    y=schema_data['row_count'],
                    text=schema_data['row_count'],
                    textposition='auto'
                ))
            
            fig.update_layout(
                title='Table Row Counts (Latest Run)',
                xaxis_title='Table Name',
                yaxis_title='Row Count',
                barmode='group',
                template='plotly_white',
                height=400
            )
            
            return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def run_dashboard(self, host: str = '127.0.0.1', port: int = 5000, debug: bool = False):
        """Run the Flask dashboard"""
        if not FLASK_AVAILABLE:
            print("Flask is not available. Please install it to run the dashboard:")
            print("pip install flask plotly")
            return
        
        if not self.app:
            print("Dashboard app not initialized")
            return
        
        print(f"Starting Data Quality Dashboard at http://{host}:{port}")
        self.app.run(host=host, port=port, debug=debug)
    
    def generate_html_report(self, output_file: str = "data_quality_report.html") -> str:
        """Generate a static HTML report"""
        data = self.get_dashboard_data()
        
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .header { background-color: #2E86AB; color: white; padding: 20px; border-radius: 5px; }
                .summary { display: flex; justify-content: space-around; margin: 20px 0; }
                .metric { text-align: center; padding: 20px; background-color: #f5f5f5; border-radius: 5px; }
                .metric-value { font-size: 2em; font-weight: bold; color: #2E86AB; }
                .section { margin: 30px 0; }
                .section h2 { color: #2E86AB; border-bottom: 2px solid #2E86AB; padding-bottom: 10px; }
                table { width: 100%; border-collapse: collapse; margin: 20px 0; }
                th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
                th { background-color: #f2f2f2; }
                .pass { color: green; font-weight: bold; }
                .fail { color: red; font-weight: bold; }
                .status-completed { color: green; font-weight: bold; }
                .status-failed { color: red; font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Data Quality Report</h1>
                <p>Generated: {generated_time}</p>
            </div>
            
            <div class="summary">
                <div class="metric">
                    <div class="metric-value">{total_runs}</div>
                    <div>Total Runs</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{success_rate}%</div>
                    <div>Success Rate (30d)</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{avg_quality_score}%</div>
                    <div>Avg Quality Score</div>
                </div>
                <div class="metric">
                    <div class="metric-value status-{latest_status_class}">{latest_status}</div>
                    <div>Latest Status</div>
                </div>
            </div>
            
            <div class="section">
                <h2>Recent Pipeline Runs</h2>
                <table>
                    <tr>
                        <th>Timestamp</th>
                        <th>Status</th>
                        <th>Duration (s)</th>
                        <th>Records</th>
                        <th>Quality Score</th>
                        <th>Errors</th>
                    </tr>
                    {recent_runs_rows}
                </table>
            </div>
            
            <div class="section">
                <h2>Latest Quality Checks</h2>
                <table>
                    <tr>
                        <th>Check Name</th>
                        <th>Status</th>
                        <th>Score</th>
                        <th>Details</th>
                    </tr>
                    {quality_checks_rows}
                </table>
            </div>
        </body>
        </html>
        """
        
        # Format the data
        summary = data['summary']
        
        # Recent runs table
        recent_runs_rows = ""
        for run in data['recent_runs']:
            status_class = run['status'].lower()
            quality_score = f"{run['data_quality_score']*100:.1f}%" if run['data_quality_score'] else "N/A"
            recent_runs_rows += f"""
                <tr>
                    <td>{run['run_timestamp']}</td>
                    <td class="status-{status_class}">{run['status']}</td>
                    <td>{run['duration_seconds']:.1f}</td>
                    <td>{run['total_records']:,}</td>
                    <td>{quality_score}</td>
                    <td>{run['error_count']}</td>
                </tr>
            """
        
        # Quality checks table
        quality_checks_rows = ""
        for check in data['latest_checks']:
            status_text = "PASS" if check['passed'] else "FAIL"
            status_class = "pass" if check['passed'] else "fail"
            quality_checks_rows += f"""
                <tr>
                    <td>{check['check_name']}</td>
                    <td class="{status_class}">{status_text}</td>
                    <td>{check['score']*100:.1f}%</td>
                    <td>{check['details']}</td>
                </tr>
            """
        
        # Fill in the template
        html_content = html_template.format(
            generated_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            total_runs=summary['total_runs'],
            success_rate=summary['success_rate'],
            avg_quality_score=summary['avg_quality_score'],
            latest_status=summary['latest_status'],
            latest_status_class=summary['latest_status'].lower(),
            recent_runs_rows=recent_runs_rows,
            quality_checks_rows=quality_checks_rows
        )
        
        # Write to file
        with open(output_file, 'w') as f:
            f.write(html_content)
        
        return output_file

def main():
    """Demo the dashboard functionality"""
    dashboard = DataQualityDashboard()
    
    # Generate sample data
    run_id = dashboard.store_pipeline_run({
        'timestamp': datetime.now().isoformat(),
        'status': 'COMPLETED',
        'duration': 45.2,
        'total_records': 78895,
        'data_quality_score': 0.96,
        'error_count': 0
    })
    
    # Sample quality checks
    quality_results = [
        {'check_name': 'Bronze crm_cust_info - Row Count', 'passed': True, 'score': 1.0, 'details': '18,495 rows found'},
        {'check_name': 'Silver Data Retention', 'passed': True, 'score': 0.98, 'details': 'Retained 98% of bronze data'},
        {'check_name': 'Gold Referential Integrity', 'passed': True, 'score': 0.95, 'details': '95% of sales have valid customers'}
    ]
    dashboard.store_quality_checks(run_id, quality_results)
    
    # Generate HTML report
    report_file = dashboard.generate_html_report()
    print(f"HTML report generated: {report_file}")
    
    # Run dashboard (if Flask is available)
    if FLASK_AVAILABLE:
        dashboard.run_dashboard(debug=True)

if __name__ == "__main__":
    main()