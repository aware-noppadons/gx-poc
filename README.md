# Great Expectations POC - User Manual

This POC demonstrates how Great Expectations (GX) can **automatically learn data rules** and detect anomalies/data drift in a PostgreSQL database populated with TPC-C OLTP data.

## Table of Contents

- [Key Feature: Automatic Rule Learning](#key-feature-automatic-rule-learning)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
  - [Step 1: Build All Images](#step-1-build-all-images)
  - [Step 2: Start PostgreSQL](#step-2-start-postgresql)
  - [Step 3: Generate TPC-C Base Data](#step-3-generate-tpc-c-base-data)
  - [Step 4: Start Great Expectations Service](#step-4-start-great-expectations-service)
  - [Step 5: Initialize GX and Connect to PostgreSQL](#step-5-initialize-gx-and-connect-to-postgresql)
  - [Step 6: Auto-Profile Data (Learn Rules Automatically)](#step-6-auto-profile-data-learn-rules-automatically)
  - [Step 7: Run Baseline Validation (Should Pass)](#step-7-run-baseline-validation-should-pass)
- [Data Drift Demonstration](#data-drift-demonstration)
  - [Drift Scenario 1: Statistical Outliers](#drift-scenario-1-statistical-outliers-recommended-first-test)
  - [Drift Scenario 2: NULL Anomalies](#drift-scenario-2-null-anomalies)
  - [Drift Scenario 3: Volume Anomalies](#drift-scenario-3-volume-anomalies)
- [Reset Environment](#reset-environment)
- [Troubleshooting](#troubleshooting)
- [Summary](#summary)
- [Files Reference](#files-reference)
- [GX Data Directory Structure](#gx-data-directory-structure)

---

## Key Feature: Automatic Rule Learning

GX 1.0+ removed the built-in auto-profiler. This POC implements a **custom auto-profiler** that:
1. Queries PostgreSQL for column statistics (min, max, nulls, distinct counts)
2. Automatically generates expectations based on discovered patterns
3. Detects anomalies when data drifts outside learned ranges

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Network                   │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │ PostgreSQL  │    │  HammerDB   │    │ Great           │  │
│  │   (tpcc)    │◄───│  (TPC-C)    │    │ Expectations    │  │
│  │             │◄───┼─────────────┼────│                 │  │
│  │ Port: 5432  │    │  One-shot   │    │ Ports: 8888,    │  │
│  │             │    │   loader    │    │        8080     │  │
│  └─────────────┘    └─────────────┘    └─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Docker and Docker Compose installed
- ~2GB disk space for PostgreSQL data
- ~2GB for HammerDB image download

---

## Quick Start

### Step 1: Build All Images

```bash
cd /path/to/gx-poc

# Build all custom images (HammerDB and GX)
docker-compose build
```

This builds:
- `gx-hammerdb` - HammerDB with TPC-C scripts (~2GB base image download)
- `gx-service` - Great Expectations with PostgreSQL drivers

### Step 2: Start PostgreSQL

```bash
docker-compose up -d postgres
```

Wait for PostgreSQL to be healthy:
```bash
docker-compose logs -f postgres
# Look for: "database system is ready to accept connections"
# Press Ctrl+C to exit logs
```

### Step 3: Generate TPC-C Base Data

```bash
docker-compose up hammerdb
```

This will:
- Wait for PostgreSQL to be ready
- Create TPC-C schema with 10 warehouses (~1GB data)
- Takes approximately 5 minutes
- Container exits when complete

Verify the data:
```bash
docker-compose exec postgres psql -U postgres -d tpcc -c "\dt"
docker-compose exec postgres psql -U postgres -d tpcc -c "SELECT COUNT(*) FROM customer;"
# Expected: 300000 customers
```

### Step 4: Start Great Expectations Service

```bash
docker-compose up -d great_expectations
```

### Step 5: Initialize GX and Connect to PostgreSQL

```bash
docker-compose exec great_expectations python /app/scripts/init_gx.py
```

Expected output:
```
Initializing Great Expectations context...
GX Version: 1.9.1
Connecting to PostgreSQL at postgres...
Created datasource: tpcc_postgres
  Added table asset: warehouse_asset
  Added table asset: customer_asset
  ...
GX initialization complete!
```

### Step 6: Auto-Profile Data (Learn Rules Automatically)

```bash
docker-compose exec great_expectations python /app/scripts/profile_data.py
```

This **automatically learns** expectations from the actual data:
1. Queries PostgreSQL for column statistics (min, max, nulls, distinct counts)
2. Generates expectations based on discovered patterns:
   - NOT NULL for columns with 0 nulls
   - Value ranges for numeric columns (with 10% margin)
   - Uniqueness for ID columns
   - Row count ranges (with 20% margin)

**Auto-generated suites:**
| Suite | Expectations | Sample Learned Rules |
|-------|--------------|---------------------|
| `warehouse_auto` | 14 | w_id: 1-10, w_tax: 0.1-0.2 |
| `customer_auto` | 31 | c_discount: 0-0.55, c_balance: -12 to 11 |
| `orders_auto` | 15 | o_ol_cnt: 5-15, row count: 240k-360k |
| `order_line_auto` | 17 | ol_quantity: 3-11, ol_amount: 0-11000 |
| `item_auto` | 10 | i_id unique, i_price: 1-100 |

### Step 7: Run Baseline Validation (Should Pass)

```bash
docker-compose exec great_expectations python /app/scripts/run_validation.py
```

Expected output:
```
============================================================
VALIDATION SUMMARY
============================================================
  [PASS] warehouse_asset / warehouse_auto
  [PASS] customer_asset / customer_auto
  [PASS] orders_asset / orders_auto
  [PASS] order_line_asset / order_line_auto
  [PASS] item_asset / item_auto

Total: 5 passed, 0 failed, 0 skipped
```

---

## Data Drift Demonstration

The following scripts introduce data quality issues that GX should detect using the **auto-learned rules**.

### Drift Scenario 1: Statistical Outliers (Recommended First Test)

Introduces values outside the **auto-learned** ranges.

```bash
# Apply the drift
docker-compose exec postgres psql -U postgres -d tpcc \
  -f /scripts/data_drift/02_add_outliers.sql

# Run validation
docker-compose exec great_expectations python /app/scripts/run_validation.py
```

**What it does:**
- Sets 15 order quantities to 999 (above auto-learned max of ~11)
- Sets 2500 customer discounts to 90% (above auto-learned max of ~55%)
- Sets 1000 customer balances to -500,000 (below auto-learned min of ~-12)

**Expected result:**
```
  [PASS] warehouse_asset / warehouse_auto
  [FAIL] customer_asset / customer_auto
  [PASS] orders_asset / orders_auto
  [FAIL] order_line_asset / order_line_auto
  [PASS] item_asset / item_auto

Total: 3 passed, 2 failed, 0 skipped
```

**Restore data after testing:**
```bash
docker-compose exec postgres psql -U postgres -d tpcc -c "
UPDATE customer SET c_discount = RANDOM() * 0.5 WHERE c_discount > 0.5;
UPDATE customer SET c_balance = -10 + RANDOM() * 20 WHERE c_balance < -100000;
UPDATE order_line SET ol_quantity = 5 WHERE ol_quantity > 100;
"
```

---

### Drift Scenario 2: NULL Anomalies

Introduces unexpected NULL values in order_line columns.

```bash
# Apply the drift
docker-compose exec postgres psql -U postgres -d tpcc \
  -f /scripts/data_drift/01_add_null_anomalies.sql

# Run validation
docker-compose exec great_expectations python /app/scripts/run_validation.py
```

**What it does:**
- Sets 100 order_line amounts to NULL (auto-learned: no NULLs allowed)

**Expected result:**
```
  [FAIL] order_line_asset / order_line_auto

Total: 4 passed, 1 failed, 0 skipped
```

**Restore data after testing:**
```bash
docker-compose exec postgres psql -U postgres -d tpcc -c "
UPDATE order_line SET ol_amount = ol_quantity * 10 WHERE ol_amount IS NULL;
"
```

---

### Drift Scenario 3: Volume Anomalies

Tests row count expectations (auto-learned with 20% margin).

```bash
# Check current order count
docker-compose exec postgres psql -U postgres -d tpcc -c "SELECT COUNT(*) FROM orders;"

# Delete 25% of orders to trigger volume alert
docker-compose exec postgres psql -U postgres -d tpcc -c "
DELETE FROM orders WHERE o_id > 2250;
"

# Run validation
docker-compose exec great_expectations python /app/scripts/run_validation.py
```

**Expected result:**
```
  [FAIL] orders_asset / orders_auto (row count below learned minimum)
```

**Note:** Volume scenarios require re-running HammerDB to restore data, or use `docker-compose down -v` to reset completely.

---

## Reset Environment

### Full Reset (Recommended between tests)
```bash
# Stop all containers and remove Docker volumes
docker-compose down -v

# Delete GX generated data (local bind mount)
rm -rf gx/data/

# Verify Docker volumes are removed
docker volume ls | grep gx-poc
# Should return nothing

# Restart from Step 2
docker-compose up -d postgres
docker-compose up hammerdb
docker-compose up -d great_expectations
docker-compose exec great_expectations python /app/scripts/init_gx.py
docker-compose exec great_expectations python /app/scripts/profile_data.py
```

**Note:** The `-v` flag removes:
- `postgres_data` - All PostgreSQL data (TPC-C tables)

**Important:** You must manually delete `gx/data/` - it's a local bind mount, not a Docker volume.

### Quick Reset (Keep volumes, re-profile)
```bash
# Just re-run profiling to learn current data state
docker-compose exec great_expectations python /app/scripts/profile_data.py
```

---

## Troubleshooting

### HammerDB fails to connect
```bash
# Check PostgreSQL is healthy
docker-compose ps

# View HammerDB logs
docker-compose logs hammerdb
```

### "Database tpcc exists but is not empty" error

This happens if something created tables before HammerDB ran. Fix:
```bash
# Full reset
docker-compose down -v

# Ensure postgres/init/ folder is empty (no .sql files)
ls postgres/init/

# Restart from Step 2
docker-compose up -d postgres
docker-compose up hammerdb
```

### GX cannot connect to PostgreSQL
```bash
# Verify PostgreSQL is running
docker-compose ps postgres

# Test connection manually
docker-compose exec great_expectations python -c "
import os
from sqlalchemy import create_engine, text
engine = create_engine('postgresql+psycopg2://postgres:postgres@postgres:5432/tpcc')
with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM customer'))
    print(f'Customers: {result.fetchone()[0]}')
"
```

---

## Summary

| Step | Command | Result |
|------|---------|--------|
| 1. Build | `docker-compose build` | Images created |
| 2. Start DB | `docker-compose up -d postgres` | PostgreSQL running |
| 3. Load Data | `docker-compose up hammerdb` | 300k customers, 300k orders |
| 4. Start GX | `docker-compose up -d great_expectations` | GX service running |
| 5. Init GX | `python /app/scripts/init_gx.py` | Datasource + 9 table assets |
| 6. Profile | `python /app/scripts/profile_data.py` | 87 auto-learned expectations |
| 7. Validate | `python /app/scripts/run_validation.py` | 5 passed, 0 failed |
| 8. Add Drift | Run drift SQL scripts | Data modified |
| 9. Detect | `python /app/scripts/run_validation.py` | Failures detected |

---

## Files Reference

| File | Purpose |
|------|---------|
| `gx/scripts/init_gx.py` | Initialize GX context and PostgreSQL datasource |
| `gx/scripts/profile_data.py` | **Auto-profiler** - learns rules from data |
| `gx/scripts/run_validation.py` | Run validations against auto-learned suites |
| `scripts/data_drift/*.sql` | SQL scripts to introduce various anomalies |

---

## GX Data Directory Structure

The `gx/data/` directory is a bind mount that stores all GX-generated data. This directory is created automatically when you run the GX scripts and is excluded from git.

```
gx/data/
├── great_expectations.yml       # Main GX configuration file
├── expectations/                # Auto-generated expectation suites (JSON)
│   ├── warehouse_auto.json      # Learned rules for warehouse table
│   ├── customer_auto.json       # Learned rules for customer table
│   ├── orders_auto.json         # Learned rules for orders table
│   ├── order_line_auto.json     # Learned rules for order_line table
│   └── item_auto.json           # Learned rules for item table
├── validation_definitions/      # Links between data assets and suites
├── checkpoints/                 # Reusable validation checkpoints
├── plugins/                     # Custom GX plugins
│   └── custom_data_docs/        # Custom Data Docs styling
└── uncommitted/                 # Local data (not for version control)
    ├── config_variables.yml     # Environment-specific variables
    ├── data_docs/               # Generated HTML validation reports
    └── validations/             # Validation run results
```

### Directory Descriptions

| Directory/File | Purpose |
|----------------|---------|
| `great_expectations.yml` | Main configuration: datasources, stores, data docs sites |
| `expectations/` | JSON files containing auto-learned validation rules (one per table) |
| `validation_definitions/` | Defines which expectation suite validates which data asset |
| `checkpoints/` | Bundled validation jobs that can run multiple validations |
| `plugins/` | Custom renderers, styles, and extensions for Data Docs |
| `uncommitted/` | Runtime data that should never be committed to git |
| `uncommitted/data_docs/` | Static HTML reports showing validation results |
| `uncommitted/validations/` | Raw validation result data (JSON) |

### Expectation Suite Files

Each `*_auto.json` file in `expectations/` contains the auto-learned rules:

```json
{
  "name": "customer_auto",
  "expectations": [
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "c_id"}},
    {"type": "expect_column_values_to_be_between", "kwargs": {"column": "c_discount", "min_value": 0, "max_value": 0.55}},
    {"type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 240000, "max_value": 360000}}
  ]
}
```

These rules are automatically generated by `profile_data.py` based on actual data statistics.
