#!/bin/bash

CONNECTOR_NAME="third-party-mysql"
CONFIG_FILE="third-party-mysql.json"
CONNECT_URL="http://localhost:8083/connectors"

# echo "--- Deleting existing connector: $CONNECTOR_NAME ---"
# curl -s -X DELETE "$CONNECT_URL/$CONNECTOR_NAME" -o /dev/null

# # Give Kafka Connect 2 seconds to release resources
# sleep 2

echo "--- Recreating connector from $CONFIG_FILE ---"
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
     "$CONNECT_URL" -d @"$CONFIG_FILE"

echo -e "\n--- Current Status ---"
curl -s "$CONNECT_URL/$CONNECTOR_NAME/status" | jq