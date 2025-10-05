#!/bin/bash

# Script to stop a container and wait for its termination
# Usage: ./stop-and-wait.sh <service_name>

set -e

SERVICE_NAME=$1

if [ -z "$SERVICE_NAME" ]; then
    echo "Error: Please specify the service name"
    echo "Usage: $0 <service_name>"
    exit 1
fi

echo "Stopping container $SERVICE_NAME..."
docker compose -f ../../docker-compose.yml stop "$SERVICE_NAME"
echo "Finished stopping container $SERVICE_NAME"