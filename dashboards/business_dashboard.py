"""
Modern Data Warehouse - Business Intelligence Dashboard
=======================================================
Creates interactive business dashboards using Plotly Dash for data visualization.
"""

import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    import dash
    from dash import dcc, html, Input, Output, callback
    import dash_bootstrap_components as dbc
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False
    print("Dash not available. Install with: pip install dash dash-bootstrap-components")

from pipelines.config.database import db_manager

class BusinessDashboard:
    """Interactive business intelligence dashboard"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = db_manager
        
        if DASH_AVAILABLE:
            self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
            self.setup_layout()
            self.setup_callbacks()
    
    def get_sales_data(self) -> pd.DataFrame:
        """Get sales data from the Gold layer"""
        try:
            query = """
            SELECT 
                f.order_date,
                f.sales_amount,
                f.quantity,
                f.price,
                c.first_name + ' ' + c.last_name as customer_name,
                c.country,
                c.gender,
                c.marital_status,
                p.product_name,
                p.category,
                p.subcategory,
                p.product_line
            FROM gold.fact_sales f
            LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
            LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
            WHERE f.order_date IS NOT NULL
            ORDER BY f.order_date DESC
            """
            
            results = self.db.execute_query(query)
            if results:
                df = pd.DataFrame(results)
                df['order_date'] = pd.to_datetime(df['order_date'])
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching sales data: {str(e)}")
            return pd.DataFrame()
    
    def get_customer_summary(self) -> pd.DataFrame:
        """Get customer summary statistics"""
        try:
            query = """
            SELECT 
                c.country,
                c.gender,
                c.marital_status,
                COUNT(*) as customer_count,
                AVG(CAST(f.sales_amount as FLOAT)) as avg_sales
            FROM gold.dim_customers c
            LEFT JOIN gold.fact_sales f ON c.customer_key = f.customer_key
            GROUP BY c.country, c.gender, c.marital_status
            ORDER BY customer_count DESC
            """
            
            results = self.db.execute_query(query)
            return pd.DataFrame(results) if results else pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error fetching customer summary: {str(e)}")
            return pd.DataFrame()
    
    def get_product_performance(self) -> pd.DataFrame:
        """Get product performance metrics"""
        try:
            query = """
            SELECT 
                p.product_name,
                p.category,
                p.subcategory,
                p.product_line,
                COUNT(*) as order_count,
                SUM(f.sales_amount) as total_sales,
                SUM(f.quantity) as total_quantity,
                AVG(CAST(f.sales_amount as FLOAT)) as avg_order_value
            FROM gold.fact_sales f
            LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
            GROUP BY p.product_name, p.category, p.subcategory, p.product_line
            ORDER BY total_sales DESC
            """
            
            results = self.db.execute_query(query)
            return pd.DataFrame(results) if results else pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error fetching product performance: {str(e)}")
            return pd.DataFrame()
    
    def setup_layout(self):
        """Setup the dashboard layout"""
        if not DASH_AVAILABLE:
            return
        
        self.app.layout = dbc.Container([
            # Header
            dbc.Row([
                dbc.Col([
                    html.H1("🏢 Modern Data Warehouse", className="text-primary mb-4"),
                    html.H3("Business Intelligence Dashboard", className="text-secondary mb-4"),
                    html.Hr()
                ])
            ]),
            
            # Key Metrics Cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Total Sales", className="card-title"),
                            html.H2(id="total-sales", className="text-primary"),
                            html.P("Last 30 days", className="text-muted")
                        ])
                    ], color="light")
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Orders", className="card-title"),
                            html.H2(id="total-orders", className="text-success"),
                            html.P("Total count", className="text-muted")
                        ])
                    ], color="light")  
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Customers", className="card-title"),
                            html.H2(id="total-customers", className="text-info"),
                            html.P("Unique customers", className="text-muted")
                        ])
                    ], color="light")
                ], width=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Avg Order", className="card-title"),
                            html.H2(id="avg-order-value", className="text-warning"),
                            html.P("Per transaction", className="text-muted")
                        ])
                    ], color="light")
                ], width=3)
            ], className="mb-4"),
            
            # Charts Row 1
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5("Sales Trend Over Time")),
                        dbc.CardBody([
                            dcc.Graph(id="sales-trend-chart")
                        ])
                    ])
                ], width=8),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5("Sales by Country")),
                        dbc.CardBody([
                            dcc.Graph(id="sales-by-country-chart")
                        ])
                    ])
                ], width=4)
            ], className="mb-4"),
            
            # Charts Row 2
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5("Top Products")),
                        dbc.CardBody([
                            dcc.Graph(id="top-products-chart")
                        ])
                    ])
                ], width=6),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5("Customer Demographics")),
                        dbc.CardBody([
                            dcc.Graph(id="customer-demographics-chart")
                        ])
                    ])
                ], width=6)
            ], className="mb-4"),
            
            # Charts Row 3
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(html.H5("Product Categories Performance")),
                        dbc.CardBody([
                            dcc.Graph(id="category-performance-chart")
                        ])
                    ])
                ], width=12)
            ], className="mb-4"),
            
            # Auto-refresh component
            dcc.Interval(
                id='interval-component',
                interval=300*1000,  # Update every 5 minutes
                n_intervals=0
            ),
            
            # Footer
            dbc.Row([
                dbc.Col([
                    html.Hr(),
                    html.P("Data refreshed automatically every 5 minutes", 
                          className="text-muted text-center"),
                    html.P(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                          id="last-updated", className="text-muted text-center")
                ])
            ])
            
        ], fluid=True)
    
    def setup_callbacks(self):
        """Setup dashboard callbacks for interactivity"""
        if not DASH_AVAILABLE:
            return
        
        @self.app.callback(
            [Output('total-sales', 'children'),
             Output('total-orders', 'children'),
             Output('total-customers', 'children'),
             Output('avg-order-value', 'children'),
             Output('sales-trend-chart', 'figure'),
             Output('sales-by-country-chart', 'figure'),
             Output('top-products-chart', 'figure'),
             Output('customer-demographics-chart', 'figure'),
             Output('category-performance-chart', 'figure'),
             Output('last-updated', 'children')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_dashboard(n):
            # Get data
            sales_df = self.get_sales_data()
            customer_df = self.get_customer_summary()
            product_df = self.get_product_performance()
            
            if sales_df.empty:
                # Return empty/default values if no data
                return ("No data", "No data", "No data", "No data", 
                       {}, {}, {}, {}, {}, 
                       f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Calculate KPIs
            total_sales = f"${sales_df['sales_amount'].sum():,.0f}"
            total_orders = f"{len(sales_df):,}"
            total_customers = f"{sales_df['customer_name'].nunique():,}"
            avg_order = f"${sales_df['sales_amount'].mean():.2f}"
            
            # Sales trend chart
            daily_sales = sales_df.groupby(sales_df['order_date'].dt.date)['sales_amount'].sum().reset_index()
            sales_trend_fig = px.line(daily_sales, x='order_date', y='sales_amount',
                                    title="Daily Sales Trend")
            sales_trend_fig.update_traces(line_color='#2E86AB')
            
            # Sales by country chart
            country_sales = sales_df.groupby('country')['sales_amount'].sum().reset_index()
            country_sales = country_sales.sort_values('sales_amount', ascending=True).tail(10)
            sales_by_country_fig = px.bar(country_sales, x='sales_amount', y='country',
                                        orientation='h', title="Sales by Country")
            sales_by_country_fig.update_traces(marker_color='#A23B72')
            
            # Top products chart
            if not product_df.empty:
                top_products = product_df.head(10)
                top_products_fig = px.bar(top_products, x='product_name', y='total_sales',
                                        title="Top 10 Products by Sales")
                top_products_fig.update_traces(marker_color='#F18F01')
                top_products_fig.update_xaxes(tickangle=45)
            else:
                top_products_fig = {}
            
            # Customer demographics chart
            if not customer_df.empty:
                demographics_fig = px.sunburst(customer_df, 
                                             path=['country', 'gender', 'marital_status'],
                                             values='customer_count',
                                             title="Customer Demographics")
            else:
                demographics_fig = {}
            
            # Category performance chart
            if not product_df.empty:
                category_perf = product_df.groupby('category').agg({
                    'total_sales': 'sum',
                    'total_quantity': 'sum',
                    'order_count': 'sum'
                }).reset_index()
                
                category_fig = make_subplots(
                    rows=1, cols=2,
                    subplot_titles=("Sales by Category", "Quantity by Category"),
                    specs=[[{"type": "bar"}, {"type": "bar"}]]
                )
                
                category_fig.add_trace(
                    go.Bar(x=category_perf['category'], y=category_perf['total_sales'],
                          name="Sales", marker_color='#2E86AB'),
                    row=1, col=1
                )
                
                category_fig.add_trace(
                    go.Bar(x=category_perf['category'], y=category_perf['total_quantity'],
                          name="Quantity", marker_color='#A23B72'),
                    row=1, col=2
                )
                
                category_fig.update_layout(showlegend=False, title_text="Category Performance")
            else:
                category_fig = {}
            
            last_updated = f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            return (total_sales, total_orders, total_customers, avg_order,
                   sales_trend_fig, sales_by_country_fig, top_products_fig,
                   demographics_fig, category_fig, last_updated)
    
    def run_dashboard(self, host='127.0.0.1', port=8050, debug=False):
        """Run the dashboard server"""
        if not DASH_AVAILABLE:
            print("Dash is not available. Please install it to run the dashboard:")
            print("pip install dash dash-bootstrap-components plotly")
            return
        
        print(f"Starting Business Intelligence Dashboard at http://{host}:{port}")
        print("Features:")
        print("- Sales performance metrics")
        print("- Customer analytics")
        print("- Product performance")
        print("- Interactive visualizations")
        print("- Auto-refresh every 5 minutes")
        
        self.app.run_server(host=host, port=port, debug=debug)
    
    def generate_static_report(self, output_file='business_report.html') -> str:
        """Generate a static HTML business report"""
        sales_df = self.get_sales_data()
        customer_df = self.get_customer_summary()
        product_df = self.get_product_performance()
        
        if sales_df.empty:
            print("No data available for report generation")
            return ""
        
        # Create visualizations
        figs = []
        
        # Sales trend
        daily_sales = sales_df.groupby(sales_df['order_date'].dt.date)['sales_amount'].sum().reset_index()
        fig1 = px.line(daily_sales, x='order_date', y='sales_amount', title="Sales Trend")
        figs.append(fig1.to_html(full_html=False, include_plotlyjs='cdn'))
        
        # Top products
        if not product_df.empty:
            top_products = product_df.head(10)
            fig2 = px.bar(top_products, x='product_name', y='total_sales', title="Top Products")
            figs.append(fig2.to_html(full_html=False, include_plotlyjs=False))
        
        # Country distribution
        country_sales = sales_df.groupby('country')['sales_amount'].sum().reset_index()
        fig3 = px.pie(country_sales, values='sales_amount', names='country', title="Sales by Country")
        figs.append(fig3.to_html(full_html=False, include_plotlyjs=False))
        
        # Generate HTML report
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Business Intelligence Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ background-color: #2E86AB; color: white; padding: 20px; border-radius: 5px; }}
                .kpi {{ display: flex; justify-content: space-around; margin: 20px 0; }}
                .kpi-card {{ text-align: center; padding: 20px; background-color: #f5f5f5; border-radius: 5px; }}
                .kpi-value {{ font-size: 2em; font-weight: bold; color: #2E86AB; }}
                .chart {{ margin: 30px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Business Intelligence Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="kpi">
                <div class="kpi-card">
                    <div class="kpi-value">${sales_df['sales_amount'].sum():,.0f}</div>
                    <div>Total Sales</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{len(sales_df):,}</div>
                    <div>Total Orders</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{sales_df['customer_name'].nunique():,}</div>
                    <div>Unique Customers</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">${sales_df['sales_amount'].mean():.2f}</div>
                    <div>Avg Order Value</div>
                </div>
            </div>
            
            <div class="chart">
                {figs[0] if len(figs) > 0 else '<p>No charts available</p>'}
            </div>
            
            <div class="chart">
                {figs[1] if len(figs) > 1 else ''}
            </div>
            
            <div class="chart">
                {figs[2] if len(figs) > 2 else ''}
            </div>
            
        </body>
        </html>
        """
        
        with open(output_file, 'w') as f:
            f.write(html_content)
        
        print(f"Static business report generated: {output_file}")
        return output_file

def main():
    """Demo the dashboard"""
    dashboard = BusinessDashboard()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--static':
        # Generate static report
        dashboard.generate_static_report()
    else:
        # Run interactive dashboard
        dashboard.run_dashboard(debug=True)

if __name__ == "__main__":
    main()