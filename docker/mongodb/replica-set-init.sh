#!/bin/bash

# MongoDB Replica Set Setup
# This script configures MongoDB for transaction support

set -e

echo "Setting up MongoDB replica set..."

# Wait for MongoDB to be ready
echo "Waiting for MongoDB..."
until mongosh --eval "db.adminCommand('ping')" --quiet; do
    sleep 1
done

# Check if replica set is already initialized
if mongosh --eval "rs.status().ok" --quiet 2>/dev/null; then
    echo "Replica set already initialized"
    exit 0
fi

# Initialize replica set
echo "Initializing replica set..."
mongosh --eval "
rs.initiate({
    _id: 'rs0',
    members: [
        { _id: 0, host: 'localhost:27017' }
    ]
});
"

# Wait for replica set to become primary
echo "Waiting for replica set to be ready..."
for i in {1..30}; do
    if mongosh --eval "rs.status().myState" --quiet | grep -q "1"; then
        echo "Replica set is ready"
        exit 0
    fi
    sleep 1
done

echo "Replica set setup failed"
exit 1
