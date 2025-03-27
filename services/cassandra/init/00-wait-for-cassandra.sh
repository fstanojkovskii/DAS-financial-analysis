#!/bin/bash

echo "Waiting for Cassandra to be ready..."
while ! cqlsh -e "SHOW VERSION" cassandra >/dev/null 2>&1; do
    sleep 5
    echo "Waiting for Cassandra..."
done
echo "Cassandra is ready!" 