"""
Modern Data Warehouse - Data Lineage Tracker
============================================
Tracks data lineage and dependencies across the data warehouse layers.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, asdict
from pathlib import Path

@dataclass
class DataAsset:
    """Represents a data asset (table, view, column)"""
    name: str
    type: str  # 'table', 'view', 'column'
    schema: str
    description: Optional[str] = None
    created_date: Optional[str] = None
    last_modified: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []

@dataclass  
class DataTransformation:
    """Represents a data transformation"""
    source_assets: List[str]
    target_asset: str
    transformation_type: str  # 'etl', 'view', 'procedure'
    transformation_logic: str
    created_date: str
    created_by: str

class DataLineageTracker:
    """Tracks data lineage and dependencies"""
    
    def __init__(self, lineage_file: str = "monitoring/data_lineage.json"):
        self.lineage_file = lineage_file
        self.logger = logging.getLogger(__name__)
        
        # Initialize lineage data structures
        self.assets: Dict[str, DataAsset] = {}
        self.transformations: List[DataTransformation] = []
        self.dependencies: Dict[str, Set[str]] = {}  # asset -> dependencies
        self.downstream: Dict[str, Set[str]] = {}    # asset -> downstream assets
        
        self.load_lineage()
    
    def add_asset(self, asset: DataAsset):
        """Add a data asset to the lineage"""
        asset_key = f"{asset.schema}.{asset.name}"
        self.assets[asset_key] = asset
        
        if asset_key not in self.dependencies:
            self.dependencies[asset_key] = set()
        if asset_key not in self.downstream:
            self.downstream[asset_key] = set()
        
        self.logger.info(f"Added asset: {asset_key}")
    
    def add_transformation(self, transformation: DataTransformation):
        """Add a data transformation to the lineage"""
        self.transformations.append(transformation)
        
        # Update dependencies
        target = transformation.target_asset
        if target not in self.dependencies:
            self.dependencies[target] = set()
        
        for source in transformation.source_assets:
            self.dependencies[target].add(source)
            
            # Update downstream relationships
            if source not in self.downstream:
                self.downstream[source] = set()
            self.downstream[source].add(target)
        
        self.logger.info(f"Added transformation: {' + '.join(transformation.source_assets)} -> {target}")
    
    def get_upstream_dependencies(self, asset: str, visited: Set[str] = None) -> Set[str]:
        """Get all upstream dependencies for an asset"""
        if visited is None:
            visited = set()
        
        if asset in visited:
            return set()  # Avoid circular dependencies
        
        visited.add(asset)
        dependencies = set()
        
        direct_deps = self.dependencies.get(asset, set())
        dependencies.update(direct_deps)
        
        # Recursively get dependencies of dependencies
        for dep in direct_deps:
            dependencies.update(self.get_upstream_dependencies(dep, visited.copy()))
        
        return dependencies
    
    def get_downstream_impact(self, asset: str, visited: Set[str] = None) -> Set[str]:
        """Get all downstream assets affected by changes to this asset"""
        if visited is None:
            visited = set()
        
        if asset in visited:
            return set()  # Avoid circular dependencies
        
        visited.add(asset)
        downstream = set()
        
        direct_downstream = self.downstream.get(asset, set())
        downstream.update(direct_downstream)
        
        # Recursively get downstream of downstream
        for ds in direct_downstream:
            downstream.update(self.get_downstream_impact(ds, visited.copy()))
        
        return downstream
    
    def get_lineage_path(self, source: str, target: str) -> List[str]:
        """Get the lineage path between two assets"""
        def find_path(current: str, target: str, path: List[str], visited: Set[str]) -> Optional[List[str]]:
            if current == target:
                return path + [current]
            
            if current in visited:
                return None
            
            visited.add(current)
            
            for downstream_asset in self.downstream.get(current, set()):
                result = find_path(downstream_asset, target, path + [current], visited.copy())
                if result:
                    return result
            
            return None
        
        return find_path(source, target, [], set()) or []
    
    def analyze_impact(self, changed_assets: List[str]) -> Dict[str, Any]:
        """Analyze the impact of changes to specified assets"""
        impact_analysis = {
            'changed_assets': changed_assets,
            'affected_assets': set(),
            'affected_transformations': [],
            'risk_level': 'LOW',
            'recommendations': []
        }
        
        for asset in changed_assets:
            downstream = self.get_downstream_impact(asset)
            impact_analysis['affected_assets'].update(downstream)
            
            # Find affected transformations
            for transformation in self.transformations:
                if asset in transformation.source_assets:
                    impact_analysis['affected_transformations'].append({
                        'transformation': transformation.target_asset,
                        'type': transformation.transformation_type,
                        'affected_source': asset
                    })
        
        # Determine risk level
        affected_count = len(impact_analysis['affected_assets'])
        transformation_count = len(impact_analysis['affected_transformations'])
        
        if affected_count > 10 or transformation_count > 5:
            impact_analysis['risk_level'] = 'HIGH'
            impact_analysis['recommendations'].append('Consider implementing changes in stages')
            impact_analysis['recommendations'].append('Run comprehensive testing before deployment')
        elif affected_count > 5 or transformation_count > 2:
            impact_analysis['risk_level'] = 'MEDIUM'
            impact_analysis['recommendations'].append('Test affected downstream assets')
        
        # Convert set to list for JSON serialization
        impact_analysis['affected_assets'] = list(impact_analysis['affected_assets'])
        
        return impact_analysis
    
    def generate_lineage_diagram(self, output_format: str = 'mermaid') -> str:
        """Generate a data lineage diagram"""
        if output_format == 'mermaid':
            return self._generate_mermaid_diagram()
        elif output_format == 'dot':
            return self._generate_dot_diagram()
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
    
    def _generate_mermaid_diagram(self) -> str:
        """Generate Mermaid diagram syntax"""
        lines = ['graph TD']
        
        # Add nodes
        for asset_key, asset in self.assets.items():
            node_id = asset_key.replace('.', '_').replace('-', '_')
            node_label = f"{asset.name}\\n({asset.type})"
            
            if asset.schema == 'bronze':
                lines.append(f'    {node_id}["{node_label}"]:::bronze')
            elif asset.schema == 'silver':
                lines.append(f'    {node_id}["{node_label}"]:::silver') 
            elif asset.schema == 'gold':
                lines.append(f'    {node_id}["{node_label}"]:::gold')
            else:
                lines.append(f'    {node_id}["{node_label}"]')
        
        # Add edges
        for transformation in self.transformations:
            target_id = transformation.target_asset.replace('.', '_').replace('-', '_')
            for source in transformation.source_assets:
                source_id = source.replace('.', '_').replace('-', '_')
                lines.append(f'    {source_id} --> {target_id}')
        
        # Add styles
        lines.extend([
            '',
            '    classDef bronze fill:#cd7f32,stroke:#8B4513,stroke-width:2px,color:#fff',
            '    classDef silver fill:#c0c0c0,stroke:#808080,stroke-width:2px,color:#000',
            '    classDef gold fill:#ffd700,stroke:#daa520,stroke-width:2px,color:#000'
        ])
        
        return '\n'.join(lines)
    
    def _generate_dot_diagram(self) -> str:
        """Generate Graphviz DOT diagram syntax"""
        lines = ['digraph DataLineage {', '    rankdir=LR;', '    node [shape=box];']
        
        # Add nodes with styling
        for asset_key, asset in self.assets.items():
            node_id = asset_key.replace('.', '_').replace('-', '_')
            
            if asset.schema == 'bronze':
                color = '#CD7F32'
            elif asset.schema == 'silver':
                color = '#C0C0C0'
            elif asset.schema == 'gold':
                color = '#FFD700'
            else:
                color = '#CCCCCC'
            
            lines.append(f'    {node_id} [label="{asset.name}\\n({asset.type})", fillcolor="{color}", style=filled];')
        
        # Add edges
        for transformation in self.transformations:
            target_id = transformation.target_asset.replace('.', '_').replace('-', '_')
            for source in transformation.source_assets:
                source_id = source.replace('.', '_').replace('-', '_')
                lines.append(f'    {source_id} -> {target_id};')
        
        lines.append('}')
        return '\n'.join(lines)
    
    def initialize_warehouse_lineage(self):
        """Initialize lineage for the modern data warehouse"""
        # Bronze layer assets
        bronze_tables = [
            'crm_cust_info', 'crm_prd_info', 'crm_sales_details',
            'erp_cust_az12', 'erp_loc_a101', 'erp_px_cat_g1v2'
        ]
        
        for table in bronze_tables:
            self.add_asset(DataAsset(
                name=table,
                type='table',
                schema='bronze',
                description=f'Raw data from source system',
                created_date=datetime.now().isoformat(),
                owner='ETL Pipeline',
                tags=['raw', 'bronze', 'source']
            ))
        
        # Silver layer assets
        silver_tables = [
            'crm_cust_info', 'crm_prd_info', 'crm_sales_details',
            'erp_cust_az12', 'erp_loc_a101', 'erp_px_cat_g1v2'
        ]
        
        for table in silver_tables:
            self.add_asset(DataAsset(
                name=table,
                type='table',
                schema='silver',
                description=f'Cleansed and standardized data',
                created_date=datetime.now().isoformat(),
                owner='ETL Pipeline',
                tags=['cleansed', 'silver', 'standardized']
            ))
        
        # Gold layer assets
        gold_objects = [
            ('dim_customers', 'view', 'Customer dimension with demographics'),
            ('dim_products', 'view', 'Product dimension with categories'),
            ('fact_sales', 'view', 'Sales fact table with metrics')
        ]
        
        for name, obj_type, description in gold_objects:
            self.add_asset(DataAsset(
                name=name,
                type=obj_type,
                schema='gold',
                description=description,
                created_date=datetime.now().isoformat(),
                owner='Analytics Team',
                tags=['analytics', 'gold', 'star-schema']
            ))
        
        # Add transformations
        # Bronze to Silver transformations
        silver_transforms = [
            ('bronze.crm_cust_info', 'silver.crm_cust_info', 'Data cleansing and standardization'),
            ('bronze.crm_prd_info', 'silver.crm_prd_info', 'Product data transformation'),
            ('bronze.crm_sales_details', 'silver.crm_sales_details', 'Sales data validation and cleansing'),
            ('bronze.erp_cust_az12', 'silver.erp_cust_az12', 'Customer demographic cleansing'),
            ('bronze.erp_loc_a101', 'silver.erp_loc_a101', 'Location data standardization'),
            ('bronze.erp_px_cat_g1v2', 'silver.erp_px_cat_g1v2', 'Category data transformation')
        ]
        
        for source, target, logic in silver_transforms:
            self.add_transformation(DataTransformation(
                source_assets=[source],
                target_asset=target,
                transformation_type='etl',
                transformation_logic=logic,
                created_date=datetime.now().isoformat(),
                created_by='silver.load_silver'
            ))
        
        # Silver to Gold transformations
        self.add_transformation(DataTransformation(
            source_assets=['silver.crm_cust_info', 'silver.erp_cust_az12', 'silver.erp_loc_a101'],
            target_asset='gold.dim_customers',
            transformation_type='view',
            transformation_logic='Customer dimension combining CRM and ERP data',
            created_date=datetime.now().isoformat(),
            created_by='gold.dim_customers view'
        ))
        
        self.add_transformation(DataTransformation(
            source_assets=['silver.crm_prd_info', 'silver.erp_px_cat_g1v2'],
            target_asset='gold.dim_products',
            transformation_type='view',
            transformation_logic='Product dimension with category information',
            created_date=datetime.now().isoformat(),
            created_by='gold.dim_products view'
        ))
        
        self.add_transformation(DataTransformation(
            source_assets=['silver.crm_sales_details', 'gold.dim_customers', 'gold.dim_products'],
            target_asset='gold.fact_sales',
            transformation_type='view',
            transformation_logic='Sales fact table with dimension keys',
            created_date=datetime.now().isoformat(),
            created_by='gold.fact_sales view'
        ))
    
    def save_lineage(self):
        """Save lineage data to file"""
        lineage_data = {
            'assets': {k: asdict(v) for k, v in self.assets.items()},
            'transformations': [asdict(t) for t in self.transformations],
            'last_updated': datetime.now().isoformat()
        }
        
        Path(self.lineage_file).parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.lineage_file, 'w') as f:
            json.dump(lineage_data, f, indent=2, default=str)
        
        self.logger.info(f"Lineage data saved to {self.lineage_file}")
    
    def load_lineage(self):
        """Load lineage data from file"""
        if not Path(self.lineage_file).exists():
            return
        
        try:
            with open(self.lineage_file, 'r') as f:
                lineage_data = json.load(f)
            
            # Load assets
            for asset_key, asset_data in lineage_data.get('assets', {}).items():
                self.assets[asset_key] = DataAsset(**asset_data)
            
            # Load transformations
            for transform_data in lineage_data.get('transformations', []):
                self.transformations.append(DataTransformation(**transform_data))
            
            # Rebuild dependency graphs
            self._rebuild_dependency_graphs()
            
            self.logger.info(f"Lineage data loaded from {self.lineage_file}")
            
        except Exception as e:
            self.logger.error(f"Error loading lineage data: {str(e)}")
    
    def _rebuild_dependency_graphs(self):
        """Rebuild dependency and downstream graphs from transformations"""
        self.dependencies.clear()
        self.downstream.clear()
        
        # Initialize all assets
        for asset_key in self.assets.keys():
            self.dependencies[asset_key] = set()
            self.downstream[asset_key] = set()
        
        # Rebuild from transformations
        for transformation in self.transformations:
            target = transformation.target_asset
            
            for source in transformation.source_assets:
                self.dependencies[target].add(source)
                self.downstream[source].add(target)
    
    def generate_lineage_report(self) -> str:
        """Generate a comprehensive lineage report"""
        report = []
        report.append("=" * 80)
        report.append("DATA LINEAGE REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Assets: {len(self.assets)}")
        report.append(f"Total Transformations: {len(self.transformations)}")
        report.append("")
        
        # Assets by schema
        schemas = {}
        for asset_key, asset in self.assets.items():
            if asset.schema not in schemas:
                schemas[asset.schema] = []
            schemas[asset.schema].append(asset)
        
        for schema, assets in schemas.items():
            report.append(f"{schema.upper()} Layer ({len(assets)} assets):")
            report.append("-" * 40)
            for asset in sorted(assets, key=lambda x: x.name):
                report.append(f"  • {asset.name} ({asset.type})")
                if asset.description:
                    report.append(f"    {asset.description}")
            report.append("")
        
        # Dependency analysis
        report.append("DEPENDENCY ANALYSIS:")
        report.append("-" * 40)
        
        for asset_key in sorted(self.assets.keys()):
            deps = self.get_upstream_dependencies(asset_key)
            downstream = self.get_downstream_impact(asset_key)
            
            if deps or downstream:
                report.append(f"{asset_key}:")
                if deps:
                    report.append(f"  Depends on: {', '.join(sorted(deps))}")
                if downstream:
                    report.append(f"  Impacts: {', '.join(sorted(downstream))}")
                report.append("")
        
        return "\n".join(report)

def main():
    """Demo the data lineage tracker"""
    tracker = DataLineageTracker()
    
    # Initialize warehouse lineage
    tracker.initialize_warehouse_lineage()
    
    # Save lineage
    tracker.save_lineage()
    
    # Generate report
    report = tracker.generate_lineage_report()
    print(report)
    
    # Generate diagram
    mermaid_diagram = tracker.generate_lineage_diagram('mermaid')
    with open('data_lineage_diagram.mmd', 'w') as f:
        f.write(mermaid_diagram)
    print("\nMermaid diagram saved to data_lineage_diagram.mmd")
    
    # Analyze impact
    impact = tracker.analyze_impact(['bronze.crm_cust_info'])
    print("\nImpact Analysis for bronze.crm_cust_info:")
    print(json.dumps(impact, indent=2))

if __name__ == "__main__":
    main()