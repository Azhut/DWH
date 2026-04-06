#!/bin/bash

# MongoDB Replica Set Setup
set -e

echo "Setting up MongoDB replica set..."

# Wait for MongoDB to be ready
echo "Waiting for MongoDB..."
until mongosh --eval "db.adminCommand('ping')" --quiet 2>/dev/null; do
    echo "MongoDB not ready yet, waiting..."
    sleep 2
done

# Check if replica set is already initialized
echo "Checking if replica set already initialized..."
if mongosh --quiet --eval "try { rs.status().ok } catch(e) { false }" 2>/dev/null | grep -q "1"; then
    echo "Replica set already initialized"
    exit 0
fi

# Initialize replica set
echo "Initializing replica set..."
mongosh --quiet --eval "
try {
    rs.initiate({
        _id: 'rs0',
        members: [
            { _id: 0, host: 'mongo:27017' }
        ]
    });
    print('Replica set initiation command sent');
} catch(e) {
    print('Error initiating replica set: ' + e);
    quit(1);
}
"

# Wait for replica set to become primary
echo "Waiting for replica set to be ready..."
for i in {1..60}; do
    if mongosh --quiet --eval "rs.isMaster().ismaster" 2>/dev/null | grep -q "true"; then
        echo "✓ Replica set is ready (primary elected)"
        exit 0
    fi
    echo "  Waiting... ($i/60)"
    sleep 2
done

echo "✗ Replica set setup failed: timeout waiting for primary"
exit 1