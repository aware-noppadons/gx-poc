#!/bin/bash
set -e

echo "=== Great Expectations Service Starting ==="

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL at ${GX_DATASOURCE_HOST}:${GX_DATASOURCE_PORT}..."
until PGPASSWORD=$GX_DATASOURCE_PASSWORD psql -h $GX_DATASOURCE_HOST -U $GX_DATASOURCE_USER -d $GX_DATASOURCE_DATABASE -c '\q' 2>/dev/null; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is ready!"

# Keep container running
echo "=== GX Service Ready ==="
echo "Available commands:"
echo "  python /app/scripts/init_gx.py      - Initialize GX context and datasource"
echo "  python /app/scripts/profile_data.py - Profile tables and create baseline expectations"
echo "  python /app/scripts/run_validation.py - Run validations"
echo "  jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --no-browser"
echo ""

# Keep container alive
tail -f /dev/null
