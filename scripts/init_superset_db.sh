#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD=airflow psql -h postgres -U airflow -d airflow -c '\q' 2>/dev/null; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is ready! Creating superset database if it doesn't exist..."

# Check if database exists, create if not
PGPASSWORD=airflow psql -h postgres -U airflow -d airflow -tc "SELECT 1 FROM pg_database WHERE datname = 'superset'" | grep -q 1 || \
PGPASSWORD=airflow psql -h postgres -U airflow -d airflow -c "CREATE DATABASE superset"

echo "✓ Superset database is ready!"