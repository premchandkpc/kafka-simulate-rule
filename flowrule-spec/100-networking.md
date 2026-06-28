# Networking Specification

## 1. Purpose

The networking layer provides pluggable transport adapters that connect the FlowRule runtime to external services. Transports handle protocol encoding, connection pooling, TLS, and retry at the network level.

## 2. Transport Interface

All transports implement:

```go
// Synchronous call with response
Caller interface {
    Call(ctx, target string, body []byte) ([]byte, error)
}

// Fire-and-forget emission
Emitter interface {
    Emit(ctx, target string, body []byte) error
}
```

## 3. HTTP Transport

### 3.1 Client
- Protocol: HTTP/1.1 and HTTP/2 (via `http.Transport`)
- Method: POST
- Content-Type: `application/json` (default)
- Connection pool: keep-alive, max 100 idle connections
- TLS: configurable via `*tls.Config`
- Timeout: per-request (from instruction)
- DNS caching: use Go's default (respects TTL)

### 3.2 Connection Pool
- `MaxIdleConns`: 100
- `MaxConnsPerHost`: 0 (unlimited)
- `IdleConnTimeout`: 90s
- `DisableCompression`: false
- Reuse for all requests (connection-per-host pooling)

### 3.3 Response Handling
- Status 2xx: success, body returned
- Status 4xx/5xx: error with status code
- Connection error: retryable
- Timeout: retryable

## 4. gRPC Transport

### 4.1 Client
- Protocol: gRPC over HTTP/2
- Service: `FlowRule` (defined in `flowrule.proto`)
- RPC: `Route(RouteRequest) returns (RouteResponse)`
- TLS: configurable credentials
- Stream: unary (request-response)

### 4.2 Protobuf Schema
```protobuf
message Envelope {
    bytes body = 1;
    map<string, string> headers = 2;
}

message RouteRequest {
    string rule_id = 1;
    Envelope message = 2;
}

message RouteResponse {
    Envelope response = 1;
}
```

## 5. Transport Adapter Contract

Each transport adapter must:
1. Accept a target string (URL, address, service name)
2. Send message body with headers
3. Respect context deadline
4. Return response body or error
5. Be thread-safe
6. Handle connection failures gracefully

## 6. TLS Requirements
- Minimum TLS 1.2, prefer 1.3
- Configurable CA certificate
- Optional client certificate
- Insecure skip verify: never in production
- Certificate hot-reload (future)

## 7. Future Transports
- Kafka producer (target = topic)
- NATS publisher (target = subject)
- AMQP (target = queue)
- Unix socket (target = path)
- In-process function call (target = function name)
