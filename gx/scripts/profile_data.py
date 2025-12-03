#!/usr/bin/env python3
"""
Auto-profile TPC-C data and generate baseline expectations.

Since GX 1.0+ removed the built-in auto-profiler, this script implements
a custom profiler that:
1. Queries PostgreSQL for column statistics
2. Analyzes data patterns (nulls, ranges, uniqueness)
3. Automatically generates appropriate GX expectations
"""

import os
import great_expectations as gx
from sqlalchemy import create_engine, text


def get_connection_string():
    """Build PostgreSQL connection string."""
    host = os.environ.get('GX_DATASOURCE_HOST', 'postgres')
    port = os.environ.get('GX_DATASOURCE_PORT', '5432')
    user = os.environ.get('GX_DATASOURCE_USER', 'postgres')
    password = os.environ.get('GX_DATASOURCE_PASSWORD', 'postgres')
    database = os.environ.get('GX_DATASOURCE_DATABASE', 'tpcc')
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def get_column_stats(engine, table_name):
    """Query PostgreSQL for column statistics."""
    stats = []

    # Get column info
    column_query = text("""
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = :table_name
        AND table_schema = 'public'
        ORDER BY ordinal_position
    """)

    with engine.connect() as conn:
        columns = conn.execute(column_query, {"table_name": table_name}).fetchall()

        for col_name, data_type, is_nullable in columns:
            stat = {
                "column": col_name,
                "data_type": data_type,
                "is_nullable": is_nullable == "YES"
            }

            # Get statistics for numeric columns
            if data_type in ('integer', 'smallint', 'bigint', 'numeric', 'real', 'double precision'):
                numeric_query = text(f"""
                    SELECT
                        MIN("{col_name}") as min_val,
                        MAX("{col_name}") as max_val,
                        AVG("{col_name}")::numeric(20,4) as avg_val,
                        COUNT(*) as total_count,
                        SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) as null_count,
                        COUNT(DISTINCT "{col_name}") as distinct_count
                    FROM {table_name}
                """)
                result = conn.execute(numeric_query).fetchone()
                stat.update({
                    "min": float(result[0]) if result[0] is not None else None,
                    "max": float(result[1]) if result[1] is not None else None,
                    "avg": float(result[2]) if result[2] is not None else None,
                    "total_count": result[3],
                    "null_count": result[4],
                    "distinct_count": result[5],
                    "is_numeric": True
                })
            else:
                # Get basic stats for non-numeric columns
                basic_query = text(f"""
                    SELECT
                        COUNT(*) as total_count,
                        SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) as null_count,
                        COUNT(DISTINCT "{col_name}") as distinct_count
                    FROM {table_name}
                """)
                result = conn.execute(basic_query).fetchone()
                stat.update({
                    "total_count": result[0],
                    "null_count": result[1],
                    "distinct_count": result[2],
                    "is_numeric": False
                })

            stats.append(stat)

    return stats


def generate_expectations_from_stats(suite, stats, table_name):
    """Generate GX expectations based on column statistics."""
    expectations_added = 0

    for stat in stats:
        col = stat["column"]

        # 1. NOT NULL expectation if column has no nulls and is defined as NOT NULL
        if stat["null_count"] == 0:
            try:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
                )
                expectations_added += 1
            except Exception:
                pass

        # 2. Numeric range expectations with margin
        if stat.get("is_numeric") and stat.get("min") is not None:
            min_val = stat["min"]
            max_val = stat["max"]

            # Add 10% margin to allow for normal variation
            range_size = max_val - min_val if max_val != min_val else abs(max_val) * 0.1
            margin = range_size * 0.1

            expected_min = min_val - margin
            expected_max = max_val + margin

            try:
                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column=col,
                        min_value=round(expected_min, 4),
                        max_value=round(expected_max, 4)
                    )
                )
                expectations_added += 1
            except Exception:
                pass

        # 3. Uniqueness expectation if column appears to be a key
        if stat["distinct_count"] == stat["total_count"] and stat["total_count"] > 0:
            # Skip if it's a compound key situation (check column name patterns)
            if "_id" in col.lower() or col.lower().endswith("id"):
                try:
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToBeUnique(column=col)
                    )
                    expectations_added += 1
                except Exception:
                    pass

    # 4. Table row count expectation
    if stats and stats[0].get("total_count"):
        row_count = stats[0]["total_count"]
        # Allow 20% variation in row count
        min_rows = int(row_count * 0.8)
        max_rows = int(row_count * 1.2)
        try:
            suite.add_expectation(
                gx.expectations.ExpectTableRowCountToBeBetween(
                    min_value=min_rows,
                    max_value=max_rows
                )
            )
            expectations_added += 1
        except Exception:
            pass

    return expectations_added


def auto_profile_table(context, engine, datasource_name, table_name, asset_name, suite_name):
    """Automatically profile a table and generate expectations."""
    print(f"\n{'='*50}")
    print(f"Auto-profiling: {table_name}")
    print(f"{'='*50}")

    # Get column statistics from database
    print("  Querying column statistics...")
    stats = get_column_stats(engine, table_name)
    print(f"  Found {len(stats)} columns")

    # Show discovered statistics
    for stat in stats:
        col = stat["column"]
        if stat.get("is_numeric"):
            print(f"    {col}: {stat['data_type']} "
                  f"[{stat['min']:.2f} - {stat['max']:.2f}] "
                  f"nulls={stat['null_count']}")
        else:
            print(f"    {col}: {stat['data_type']} "
                  f"distinct={stat['distinct_count']} "
                  f"nulls={stat['null_count']}")

    # Create expectation suite
    print(f"\n  Generating expectations...")
    try:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Suite {suite_name} already exists, deleting and recreating...")
            context.suites.delete(suite_name)
            suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        else:
            raise e

    # Generate expectations from statistics
    count = generate_expectations_from_stats(suite, stats, table_name)
    print(f"  Created {count} expectations automatically")

    # Show generated expectations
    print(f"  Expectations:")
    for exp in suite.expectations:
        print(f"    - {exp.expectation_type}")

    return suite


def main():
    print("=" * 60)
    print("Great Expectations Custom Auto-Profiler")
    print("Learning rules from actual data statistics")
    print("=" * 60)

    # Connect to database
    connection_string = get_connection_string()
    engine = create_engine(connection_string)
    print(f"\nConnected to PostgreSQL")

    # Get GX context
    context = gx.get_context(project_root_dir="/app")

    # Tables to auto-profile
    tables = [
        ("warehouse", "warehouse_asset", "warehouse_auto"),
        ("customer", "customer_asset", "customer_auto"),
        ("orders", "orders_asset", "orders_auto"),
        ("order_line", "order_line_asset", "order_line_auto"),
        ("item", "item_asset", "item_auto"),
    ]

    for table_name, asset_name, suite_name in tables:
        try:
            auto_profile_table(
                context, engine, "tpcc_postgres",
                table_name, asset_name, suite_name
            )
        except Exception as e:
            print(f"  Error profiling {table_name}: {e}")

    print("\n" + "=" * 60)
    print("AUTO-PROFILING COMPLETE")
    print("=" * 60)
    print("\nGenerated expectation suites:")
    for suite in context.suites.all():
        print(f"  - {suite.name}: {len(suite.expectations)} expectations")
    print("\nRun validation with:")
    print("  python /app/scripts/run_validation.py")


if __name__ == "__main__":
    main()
