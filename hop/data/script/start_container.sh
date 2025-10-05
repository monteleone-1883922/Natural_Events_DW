#!/bin/bash

# Script to start a container and wait for its completion
# Usage: ./start-and-wait.sh <service_name>

set -e

SERVICE_NAME=$1

if [ -z "$SERVICE_NAME" ]; then
    echo "Error: Please specify the service name"
    echo "Usage: $0 <service_name>"
    exit 1
fi

echo "Starting container $SERVICE_NAME..."

docker compose -f /docker-compose.yml up "$SERVICE_NAME"

echo "Container $SERVICE_NAME has completed."