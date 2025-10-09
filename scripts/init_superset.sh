#!/bin/bash
set -e

# Create superset home directory and subdirectories
echo "Creating Superset home directory structure..."
mkdir -p "${SUPERSET_HOME}"
mkdir -p "${SUPERSET_HOME}/uploads"

# Ensure proper permissions
chmod 755 "${SUPERSET_HOME}"
chmod 755 "${SUPERSET_HOME}/uploads"

# Initialize the database if not already done
if [ ! -f "${SUPERSET_HOME}/.db_initialized" ]; then
    echo "Initializing Superset database..."
    superset db upgrade

    # Create admin user (only if not exists)
    echo "Creating admin user..."
    superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@superset.com \
        --password admin || echo "Admin user already exists"

    # Initialize Superset
    echo "Initializing Superset..."
    superset init

    # Mark database as initialized
    touch "${SUPERSET_HOME}/.db_initialized"
    echo "✓ Superset database initialized successfully!"
else
    echo "Superset database already initialized, running migrations..."
    superset db upgrade
fi

# Start Superset web server
echo "Starting Superset on port 8088..."
exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 4 \
    --worker-class gthread \
    --threads 2 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"