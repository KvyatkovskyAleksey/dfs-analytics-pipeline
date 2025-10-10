#!/bin/bash
set -e

# Use environment variables with defaults
DB_HOST="${SUPERSET_DB_HOST:-postgres}"
DB_PORT="${SUPERSET_DB_PORT:-5432}"
DB_USER="${SUPERSET_DB_USER:-airflow}"
DB_NAME="${SUPERSET_DB_NAME:-superset}"
# PGPASSWORD is already set by docker-compose environment

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT} to be ready..."
until PGPASSWORD="${PGPASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d postgres -c '\q' 2>/dev/null; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is ready! Creating ${DB_NAME} database if it doesn't exist..."

# Check if database exists, create if not
PGPASSWORD="${PGPASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'" | grep -q 1 || \
PGPASSWORD="${PGPASSWORD}" psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d postgres -c "CREATE DATABASE ${DB_NAME}"

echo "✓ Superset database is ready!"