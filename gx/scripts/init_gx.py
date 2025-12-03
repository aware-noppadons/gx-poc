#!/usr/bin/env python3
"""Initialize Great Expectations context and PostgreSQL datasource."""

import os
import great_expectations as gx

def get_connection_string():
    """Build PostgreSQL connection string from environment variables."""
    host = os.environ.get('GX_DATASOURCE_HOST', 'localhost')
    port = os.environ.get('GX_DATASOURCE_PORT', '5432')
    user = os.environ.get('GX_DATASOURCE_USER', 'postgres')
    password = os.environ.get('GX_DATASOURCE_PASSWORD', 'postgres')
    database = os.environ.get('GX_DATASOURCE_DATABASE', 'tpcc')

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

def init_context():
    """Initialize GX context with PostgreSQL datasource."""
    print("Initializing Great Expectations context...")
    print(f"GX Version: {gx.__version__}")

    # Get existing context
    context = gx.get_context(project_root_dir="/app")

    # Add PostgreSQL datasource
    connection_string = get_connection_string()
    print(f"Connecting to PostgreSQL at {os.environ.get('GX_DATASOURCE_HOST')}...")

    # Check if datasource already exists using new API
    datasource_name = "tpcc_postgres"
    existing_datasources = [ds.name for ds in context.data_sources.all()]

    if datasource_name in existing_datasources:
        print(f"Datasource '{datasource_name}' already exists")
        datasource = context.data_sources.get(datasource_name)
    else:
        # Create new datasource using factory method
        datasource = context.data_sources.add_postgres(
            name=datasource_name,
            connection_string=connection_string
        )
        print(f"Created datasource: {datasource_name}")

    # TPC-C tables to add as assets
    tpcc_tables = [
        "warehouse",
        "district",
        "customer",
        "history",
        "orders",
        "new_order",
        "order_line",
        "stock",
        "item"
    ]

    # Add table assets
    for table_name in tpcc_tables:
        asset_name = f"{table_name}_asset"
        try:
            asset = datasource.add_table_asset(
                name=asset_name,
                table_name=table_name
            )
            print(f"  Added table asset: {asset_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  Table asset already exists: {asset_name}")
            else:
                print(f"  Warning - could not add {table_name}: {e}")

    print("\nGX initialization complete!")
    print(f"Datasources: {list(context.data_sources.all())}")

    return context

if __name__ == "__main__":
    init_context()
