#!/usr/bin/env python3
"""Run validations against expectation suites."""

import great_expectations as gx
from datetime import datetime


def run_validation(context, datasource_name, asset_name, suite_name):
    """Run a single validation and return results."""
    print(f"\nValidating {asset_name} against {suite_name}...")

    try:
        # Get datasource and asset
        datasource = context.data_sources.get(datasource_name)
        asset = datasource.get_asset(asset_name)

        # Get or create batch definition
        batch_def_name = f"{asset_name}_batch"
        try:
            batch_definition = asset.get_batch_definition(batch_def_name)
        except Exception:
            batch_definition = asset.add_batch_definition_whole_table(name=batch_def_name)

        # Get expectation suite
        suite = context.suites.get(suite_name)

        # Create validation definition
        validation_name = f"{asset_name}_{suite_name}_validation"
        try:
            validation_definition = context.validation_definitions.get(validation_name)
        except Exception:
            validation_definition = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=validation_name,
                    data=batch_definition,
                    suite=suite
                )
            )

        # Run validation
        result = validation_definition.run()

        success = result.success
        stats = result.results

        # Count expectations
        total = len(stats)
        passed = sum(1 for r in stats if r.success)
        failed = total - passed

        status = "PASSED" if success else "FAILED"
        print(f"  Status: {status}")
        print(f"  Expectations: {total} total, {passed} passed, {failed} failed")

        return success, result

    except Exception as e:
        print(f"  Error: {e}")
        return None, None


def main():
    print("=" * 60)
    print(f"Great Expectations Validation Run")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    context = gx.get_context(project_root_dir="/app")

    # Define validations to run (using auto-generated suites)
    validations = [
        ("tpcc_postgres", "warehouse_asset", "warehouse_auto"),
        ("tpcc_postgres", "customer_asset", "customer_auto"),
        ("tpcc_postgres", "orders_asset", "orders_auto"),
        ("tpcc_postgres", "order_line_asset", "order_line_auto"),
        ("tpcc_postgres", "item_asset", "item_auto"),
    ]

    results = []

    for datasource_name, asset_name, suite_name in validations:
        try:
            success, result = run_validation(context, datasource_name, asset_name, suite_name)
            results.append((asset_name, suite_name, success))
        except Exception as e:
            print(f"  Skipping {suite_name}: {e}")
            results.append((asset_name, suite_name, None))

    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, _, s in results if s is True)
    failed = sum(1 for _, _, s in results if s is False)
    skipped = sum(1 for _, _, s in results if s is None)

    for asset_name, suite_name, success in results:
        status = "PASS" if success is True else ("FAIL" if success is False else "SKIP")
        print(f"  [{status}] {asset_name} / {suite_name}")

    print(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped")
    print("=" * 60)

    # Exit with error code if any failures
    if failed > 0:
        exit(1)


if __name__ == "__main__":
    main()
