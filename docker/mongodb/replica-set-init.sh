#!/bin/bash
set -e

echo "Setting up MongoDB replica set..."

until mongosh --eval "db.adminCommand('ping')" --quiet 2>/dev/null; do
    echo "Waiting for MongoDB..."
    sleep 2
done

if mongosh --quiet --eval "try { rs.status().ok } catch(e) { 0 }" 2>/dev/null | grep -q "1"; then
    echo "Replica set already initialized"
    exit 0
fi

echo "Initializing replica set..."
mongosh --quiet --eval "
rs.initiate({
    _id: 'rs0',
    members: [{ _id: 0, host: '127.0.0.1:27017' }]
});
"