# nsq-http-facade

A simple HTTP REST facade for NSQ (NSQd) written in Go. This service provides HTTP endpoints to interact with NSQ, including publishing messages, consuming messages via Server-Sent Events (SSE), and controlling message lifecycle.

## Features

- **REST API** for NSQ operations
- **Producer endpoints** (PUB, MPUB) - publish single or multiple messages
- **Per-client NSQ consumers** - each HTTP client gets its own NSQ consumer for native load balancing
- **Consumer SSE endpoint** - consume messages in real-time via Server-Sent Events
- **Consumer control** - RDY flow control for consumers
- **Message lifecycle management** - touch, finish, and requeue messages with automatic expiry
- **Admin pass-through** - proxy requests to NSQd HTTP API
- **Secure authentication** - constant-time bearer token validation to prevent timing attacks
- **Memory leak prevention** - automatic cleanup of expired messages

## Installation

### From Source

```bash
go get github.com/fred01/nsq-http-facade
go build -o nsq-http-facade
```

### Using Docker

Build the Docker image:

```bash
docker build -t nsq-http-facade .
```

Run with Docker:

```bash
docker run -p 8080:8080 nsq-http-facade \
  -bearer-token=your-secret-token \
  -nsqd-address=nsqd:4150 \
  -nsqd-http-address=nsqd:4151
```

### Using Docker Compose

The easiest way to get started is using Docker Compose, which sets up NSQ and the HTTP facade:

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` and set a strong bearer token:
```bash
BEARER_TOKEN=your-strong-secret-token-here
```

3. Start the services:
```bash
docker-compose up
```

This will start:
- NSQ Lookupd (ports 4160, 4161)
- NSQd (ports 4150, 4151)
- NSQ Admin (port 4171)
- NSQ HTTP Facade (port 8080)

Access NSQ Admin UI at: http://localhost:4171

## Usage

Start the HTTP facade:

```bash
./nsq-http-facade -bearer-token=your-secret-token
```

### Command-line Flags

- `-bearer-token` - Bearer token for authentication (required)
- `-nsqd-address` - NSQd TCP address (default: `localhost:4150`)
- `-nsqd-http-address` - NSQd HTTP address (default: `localhost:4151`)
- `-http-address` - HTTP server listen address (default: `:8080`)

## API Endpoints

All endpoints require Bearer token authentication via the `Authorization` header:

```
Authorization: Bearer your-secret-token
```

### Producer Endpoints

#### Publish Single Message (PUB)

```http
POST /api/topics/{topic}/messages
Content-Type: application/json
Authorization: Bearer your-secret-token

{
  "data": "your message content as JSON"
}
```

Response:
```json
{
  "status": "ok",
  "topic": "your-topic"
}
```

#### Publish Multiple Messages (MPUB)

```http
POST /api/topics/{topic}/messages/batch
Content-Type: application/json
Authorization: Bearer your-secret-token

{
  "messages": [
    "message 1",
    "message 2",
    "message 3"
  ]
}
```

Response:
```json
{
  "status": "ok",
  "topic": "your-topic",
  "count": 3
}
```

### Consumer Endpoints

#### Consume Messages via SSE

```http
GET /api/events?topic={topic}&channel={channel}
Authorization: Bearer your-secret-token
```

This endpoint returns a stream of Server-Sent Events. Each event contains:

```json
{
  "id": "message-id",
  "timestamp": 1234567890,
  "attempts": 1,
  "body": "message content"
}
```

**Important Notes**:
- Messages received via SSE have `DisableAutoResponse()` enabled. You must explicitly finish, requeue, or touch each message using the message lifecycle endpoints.
- **Native NSQ load balancing**: Each HTTP client creates its own NSQ consumer. When multiple clients connect to the same topic/channel, NSQ distributes messages across them (one message per consumer), just like native NSQ clients.
- This enables horizontal scaling: add more HTTP clients to process messages in parallel.

#### Set Consumer RDY Count

```http
POST /api/consumers/{topic}/{channel}/rdy
Content-Type: application/json
Authorization: Bearer your-secret-token

{
  "count": 5
}
```

Response:
```json
{
  "status": "ok"
}
```

#### Get Consumer Status

```http
GET /api/consumers/{topic}/{channel}
Authorization: Bearer your-secret-token
```

Response:
```json
{
  "topic": "your-topic",
  "channel": "your-channel",
  "consumers": 3,
  "connections": 3,
  "messages": 100,
  "finished": 95,
  "requeued": 5
}
```

### Message Lifecycle Endpoints

#### Touch Message (Extend Timeout)

```http
POST /api/messages/{message-id}/touch
Authorization: Bearer your-secret-token
```

Response:
```json
{
  "status": "ok",
  "action": "touched"
}
```

#### Finish Message (Mark as Successfully Processed)

```http
POST /api/messages/{message-id}/finish
Authorization: Bearer your-secret-token
```

Response:
```json
{
  "status": "ok",
  "action": "finished"
}
```

#### Requeue Message (Fail/Retry)

```http
POST /api/messages/{message-id}/requeue?delay=60
Authorization: Bearer your-secret-token
```

Query parameter:
- `delay` (optional) - Delay in seconds before the message becomes available again. If not specified, the message is requeued immediately.

Response:
```json
{
  "status": "ok",
  "action": "requeued"
}
```

### Admin Endpoints

#### Pass-through to NSQd HTTP API

```http
GET/POST /admin/{nsqd-endpoint}
Authorization: Bearer your-secret-token
```

All requests to `/admin/*` are proxied to the NSQd HTTP API. For example:
- `/admin/stats` → `http://nsqd:4151/stats`
- `/admin/ping` → `http://nsqd:4151/ping`

## Example Usage

See the `examples.sh` script for comprehensive examples of all API endpoints.

### Publishing a Message

```bash
curl -X POST http://localhost:8080/api/topics/test-topic/messages \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-secret-token" \
  -d '{"data": {"hello": "world"}}'
```

### Consuming Messages via SSE

```bash
curl -N http://localhost:8080/api/events?topic=test-topic&channel=test-channel \
  -H "Authorization: Bearer your-secret-token"
```

### Finishing a Message

```bash
curl -X POST http://localhost:8080/api/messages/12345678/finish \
  -H "Authorization: Bearer your-secret-token"
```

## Architecture

The facade maintains:
- A single NSQ producer for publishing messages
- **Per-client NSQ consumers**: Each HTTP client connection creates its own NSQ consumer
- **Native NSQ load balancing**: Messages are distributed by NSQ across all consumers for a channel (just like native NSQ)
- **Active message registry**: Tracks in-flight messages with automatic expiry (5 minutes default)
- **Background cleanup**: Periodic cleanup of expired messages to prevent memory leaks

### Load Balancing Example

When you have 3 messages in a topic and 3 HTTP clients connected to the same channel:
1. Client 1 connects → creates NSQ consumer #1
2. Client 2 connects → creates NSQ consumer #2  
3. Client 3 connects → creates NSQ consumer #3
4. NSQ distributes the 3 messages: one to each consumer (client)
5. Each client processes its message independently and calls finish/requeue

This mirrors native NSQ behavior where each consumer gets a share of the messages.

## Security

All endpoints require authentication via Bearer token. Set a strong token using the `-bearer-token` flag when starting the service.

**Security Features**:
- **Constant-time token comparison**: Prevents timing attacks on the bearer token
- **Automatic message expiry**: Messages not processed within 5 minutes are requeued, preventing memory leaks
- **No default token in Docker**: The Dockerfile requires explicit token configuration

## Testing

### Unit Tests

Run the unit tests:
```bash
go test -v -short
```

Or using Make:
```bash
make test
```

### Integration Tests

The integration tests verify the load balancing behavior using real NSQ containers. These tests require Docker to be running.

Run integration tests:
```bash
go test -v -tags=integration -timeout=120s
```

Or using Make:
```bash
make integration-test
```

The integration tests verify:

**Load Balancing (TestLoadBalancingBehavior)**:
- Messages are distributed across multiple HTTP consumers (load balancing)
- Each consumer receives at least one message
- Total messages received equals total messages published
- Proper message lifecycle management (finish/requeue)

**Message Requeue (TestMessageRequeue)**:
- Messages can be requeued and re-delivered
- Requeued messages are received again by consumers
- Lifecycle transitions work correctly

**Message Touch (TestMessageTouch)**:
- Touch operation extends message timeout
- Messages can be touched multiple times before finishing
- Proper timeout extension behavior

**RDY Flow Control (TestRDYControl)**:
- RDY count can be set via API
- Consumer status endpoint returns correct information
- Flow control applies to all consumers for a topic/channel

**SSE Connection Close (TestSSEConnectionClose)**:
- SSE connections can be gracefully closed
- Unconsumed messages remain in the queue
- Reconnection works correctly after disconnect

**Test Scenario Example (Load Balancing)**:
1. Starts NSQd in a Docker container
2. Starts the HTTP facade
3. Opens 3 HTTP SSE consumer connections
4. Publishes 300 messages
5. Verifies all 3 consumers received messages (load balanced)
6. Verifies total count is exactly 300 messages

## License

MIT
