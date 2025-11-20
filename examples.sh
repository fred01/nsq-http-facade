#!/bin/bash

# Example usage script for nsq-http-facade
# This script demonstrates how to interact with the HTTP facade

# Configuration
API_URL="http://localhost:8080"
BEARER_TOKEN="your-secret-token"
TOPIC="test-topic"
CHANNEL="test-channel"

echo "=== NSQ HTTP Facade Examples ==="
echo ""

# Function to make authenticated requests
auth_curl() {
    curl -H "Authorization: Bearer ${BEARER_TOKEN}" "$@"
}

# Example 1: Publish a single message
echo "1. Publishing a single message to topic '${TOPIC}'..."
auth_curl -X POST "${API_URL}/api/topics/${TOPIC}/messages" \
  -H "Content-Type: application/json" \
  -d '{"data": {"message": "Hello, NSQ!", "timestamp": 1234567890}}'
echo -e "\n"

# Example 2: Publish multiple messages
echo "2. Publishing multiple messages to topic '${TOPIC}'..."
auth_curl -X POST "${API_URL}/api/topics/${TOPIC}/messages/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"id": 1, "text": "First message"},
      {"id": 2, "text": "Second message"},
      {"id": 3, "text": "Third message"}
    ]
  }'
echo -e "\n"

# Example 3: Set consumer RDY count
echo "3. Setting RDY count to 5 for topic '${TOPIC}' and channel '${CHANNEL}'..."
auth_curl -X POST "${API_URL}/api/consumers/${TOPIC}/${CHANNEL}/rdy" \
  -H "Content-Type: application/json" \
  -d '{"count": 5}'
echo -e "\n"

# Example 4: Get consumer status
echo "4. Getting consumer status for topic '${TOPIC}' and channel '${CHANNEL}'..."
auth_curl -X GET "${API_URL}/api/consumers/${TOPIC}/${CHANNEL}"
echo -e "\n"

# Example 5: Message lifecycle - finish a message
echo "5. Finishing a message (replace MESSAGE_ID with actual ID from SSE stream)..."
MESSAGE_ID="example-message-id"
auth_curl -X POST "${API_URL}/api/messages/${MESSAGE_ID}/finish"
echo -e "\n"

# Example 6: Message lifecycle - touch a message
echo "6. Touching a message to extend timeout..."
auth_curl -X POST "${API_URL}/api/messages/${MESSAGE_ID}/touch"
echo -e "\n"

# Example 7: Message lifecycle - requeue a message with delay
echo "7. Requeuing a message with 60 second delay..."
auth_curl -X POST "${API_URL}/api/messages/${MESSAGE_ID}/requeue?delay=60"
echo -e "\n"

# Example 8: Access admin API (pass-through)
echo "8. Checking NSQd stats via admin endpoint..."
auth_curl -X GET "${API_URL}/admin/stats?format=json"
echo -e "\n"

# Example 9: Consume messages via SSE (in background for 10 seconds)
echo "9. Consuming messages via SSE for 10 seconds..."
echo "   (Press Ctrl+C to stop earlier)"
timeout 10s auth_curl -N "${API_URL}/api/events?topic=${TOPIC}&channel=${CHANNEL}" || true
echo -e "\n"

echo "=== Examples Complete ==="
echo ""
echo "To consume messages continuously, run:"
echo "  curl -N -H \"Authorization: Bearer ${BEARER_TOKEN}\" \"${API_URL}/api/events?topic=${TOPIC}&channel=${CHANNEL}\""
