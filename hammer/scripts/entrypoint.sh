#!/bin/bash
set -e

echo "=== HammerDB TPC-C Schema Builder ==="
echo "Configuration:"
echo "  Host: ${PG_HOST}"
echo "  Port: ${PG_PORT}"
echo "  Database: ${PG_DATABASE}"
echo "  Warehouses: ${WAREHOUSE_COUNT}"
echo "  Virtual Users: ${NUM_VU}"
echo ""

echo "Waiting for PostgreSQL to be ready..."

# Wait for PostgreSQL using TCP connection check
while ! (echo > /dev/tcp/${PG_HOST}/${PG_PORT}) 2>/dev/null; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is accepting connections!"
# Give PostgreSQL a moment to fully initialize
sleep 3

# Run schema build
echo ""
echo "Building TPC-C schema with ${WAREHOUSE_COUNT} warehouses..."
echo "This may take several minutes..."
echo ""

# Find hammerdbcli location
if [ -f "/home/hammerdb/HammerDB-4.10/hammerdbcli" ]; then
    HAMMERDB_HOME="/home/hammerdb/HammerDB-4.10"
elif [ -f "/home/hammerdb/HammerDB-4.9/hammerdbcli" ]; then
    HAMMERDB_HOME="/home/hammerdb/HammerDB-4.9"
elif [ -f "/hammerdb/hammerdbcli" ]; then
    HAMMERDB_HOME="/hammerdb"
else
    echo "Searching for hammerdbcli..."
    HAMMERDB_CLI=$(find /home -name "hammerdbcli" 2>/dev/null | head -1)
    if [ -z "$HAMMERDB_CLI" ]; then
        HAMMERDB_CLI=$(find / -name "hammerdbcli" 2>/dev/null | head -1)
    fi
    if [ -z "$HAMMERDB_CLI" ]; then
        echo "ERROR: hammerdbcli not found!"
        exit 1
    fi
    HAMMERDB_HOME=$(dirname "$HAMMERDB_CLI")
fi

echo "Found HammerDB at: $HAMMERDB_HOME"
cd "$HAMMERDB_HOME"
./hammerdbcli auto /scripts/pg_tprocc_build.tcl

echo ""
echo "=== TPC-C Schema Build Complete ==="
echo ""

# Keep container running if requested
if [ "${KEEP_RUNNING:-false}" = "true" ]; then
    echo "Container staying alive (KEEP_RUNNING=true)"
    tail -f /dev/null
else
    echo "Build finished. Container exiting."
    exit 0
fi
