# Use Cases

## UC1: Payment Validation Pipeline

**Context:** An e-commerce platform needs to validate payments before order fulfillment.

**Flow:**
```
receive payment event
  → gate(amount > 10000) → route to manual review
  → gate(amount <= 10000) → route to fraud detection
  → parallel(fraud-check, credit-check) → collect
  → gate(result == pass) → route to fulfillment
  → gate(result == fail) → route to rejection handler
  → emit(notification)
```

**Value:** Payment routing logic is extracted from the application into a hot-reloadable rule. Changes to thresholds or flows do not require deployments.

## UC2: Multi-Environment Event Bridge

**Context:** A SaaS platform routes webhook events to customer-configured endpoints.

**Flow:**
```
receive webhook
  → map(customer_id)
  → key(customer_id) → partition for ordering
  → parallel(
       route-to-customer-endpoint,
       log-to-audit-trail
     ) → collect
  → gate(response != 200) → retry(3) ~> dead-letter-queue
  → emit(analytics-event)
```

**Value:** Each customer's routing is isolated by partition key. Failures don't block other customers. Retry with backoff is built-in.

## UC3: IoT Sensor Data Filtering

**Context:** An IoT platform ingests millions of sensor readings per second and routes only anomalous readings to the alerting system.

**Flow:**
```
receive sensor reading
  → gate(temperature > 100) → emit(alert)
  → gate(humidity < 10) → emit(alert)
  → gate(signal < -80) → emit(alert)
  → drop
```

**Value:** The DSL compiler optimizes this into a jump table. Most messages hit DROP in constant time. The runtime handles throughput without allocation pressure.

## UC4: Internal Service Orchestration

**Context:** A microservice needs to call three downstream services and merge results.

**Flow:**
```
receive order event
  → gate(priority == high) → next(premium-scheduler)
  → gate(priority == normal) → parallel(
       inventory-check,
       pricing-engine,
       shipping-calculator
     ) → collect → map(result)
```

**Value:** Parallel execution reduces total latency to the slowest service. Map transforms the merged result into the response format.

## UC5: Canary Deployment Router

**Context:** A platform gradually shifts traffic from service v1 to service v2.

**Flow:**
```
receive request
  → gate(user_id % 100 < 5) → next(service-v2)
  → next(service-v1)
```

**Value:** Routing decisions are data-driven (percentile of user ID). The gate uses a computed value. No load balancer reconfiguration needed.

## UC6: Dead Letter Recovery

**Context:** Operations needs to inspect and replay failed messages.

**Flow:**
```
// Failed messages land in DLQ with full context
DLQ entry: {
  rule_id: "order-routing",
  target: "http://fulfillment:8080",
  error: "connection timeout",
  retries: 3,
  body: { ... },
  timestamp: "..."
}

// Operations can replay to a fixed target
next(replay-target) @retry(5)
```

**Value:** DLQ preserves the full execution context. Replay targets are configurable. Retry policy can differ from original.

## UC7: Multi-Protocol Ingestion

**Context:** A service accepts events via HTTP webhooks and gRPC streams, routing both through the same rules.

**Flow:**
```
// HTTP path
POST /webhook → FlowRule engine

// gRPC path
rpc Ingest(Event) → FlowRule engine

// Same rule applies regardless of transport
gate(type == order) → route(orders)
gate(type == shipment) → route(shipping)
```

**Value:** Transport is decoupled from routing logic. The same compiled `.flow` program works for both HTTP and gRPC ingress.

## UC8: Plugin-Based Enrichment

**Context:** A service needs custom message enrichment before routing.

**Flow:**
```
receive event
  → gate(needs_enrichment == true)
  → [plugin: geoip-enrichment]  // WASM plugin
  → map(enriched)
  → next(downstream)
```

**Value:** WASM plugins provide language-independent custom logic. Plugin execution is sandboxed, timed, and observable.
