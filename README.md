# Saga Pattern Microservices

A choreography-based Saga implementation using Spring Boot 3, Spring Cloud Stream, Kafka (via Redpanda), PostgreSQL, Consul, and Debezium for the Outbox pattern.

## Table of Contents
- [Overview](#overview)
- [Architecture Diagram](#architecture-diagram)
- [Patterns Used](#patterns-used)
- [Service Deep Dives](#service-deep-dives)
  - [Order Service](#order-service)
  - [Customer Service](#customer-service)
  - [Inventory Service](#inventory-service)
  - [API Gateway](#api-gateway)
- [Kafka & Event Flow](#kafka--event-flow)
- [Outbox Pattern & CDC](#outbox-pattern--cdc)
- [Additional Notes & Best Practices](#additional-notes--best-practices)
- [Running the Project](#running-the-project)
- [References](#references)

---

## Overview

This project demonstrates a **distributed transaction (Saga)** without a central orchestrator. Services communicate solely through **Kafka topics**, using the **Outbox pattern** and **Change Data Capture (CDC)** via Debezium to ensure reliable, exactly-once event delivery.

The saga flow for placing an order:
1. Client POSTs to `/orders` → **Order Service** creates an `Order` (status `PENDING`) and writes an `ORDER_CREATED` event to its local `out_box` table.
2. Debezium picks up the insert and publishes it to Kafka (`ORDER.events` topic).
3. **Customer Service** consumes `ORDER_CREATED`, attempts to reserve the customer’s balance, and writes either `RESERVE_CUSTOMER_BALANCE_SUCCESSFULLY` or `RESERVE_CUSTOMER_BALANCE_FAILED` to its own `out_box`.
4. Debezium publishes the result back to `ORDER.events`.
5. **Order Service** and **Inventory Service** consume that result:
   - If balance reservation succeeded, **Inventory Service** tries to reserve product stock and emits `RESERVE_PRODUCT_STOCK_SUCCESSFULLY` or `_FAILED`.
   - If balance reservation failed, **Order Service** marks the order `CANCELED` (saga aborts).
6. If stock reservation succeeds → order status becomes `COMPLETED`.
   If stock reservation fails → order status becomes `CANCELED` and a `COMPENSATE_CUSTOMER_BALANCE` event is emitted.
7. **Customer Service** consumes the compensation event and refunds the balance.

All steps are **event-driven**, **idempotent**, and **transactionally safe**.

---

## Architecture Diagram

```
+----------------+      HTTP POST /orders      +-----------------+
|  Client        | ------------------------>   | API Gateway     |
+----------------+                             +--------+--------+
                                                 |               |
                                                 | lb://order-service
                                                 v
                                   +-----------------+
                                   | Order Service   |
                                   | (port 9090)     |
                                   +--------+--------+
                      ORDER_CREATED event        |               |
                      (outbox → Debezium)        |               |
                                  +--------------v--------------+
                                  |            Kafka            |
                                  |  ORDER.events topic         |
                                  +--------------+--------------+
                                                 |               |
                RESERVE_CUSTOMER_BALANCE_*       |               |
  +----------------+  <------------------------+   +--------+--------+
  | Customer Svc   |                           |   | Order Service |
  | (port 9091)    |                           |   | (updates order|
  +----------------+                           |   |  status)      |
        ^                                       +--------+--------+
        |                                           |
        |  RESERVE_PRODUCT_STOCK_*                  |
        |                                           v
  +----------------+                      +--------+--------+
  | Inventory Svc  | <------------------- | Inventory Service |
  | (port 9093)    |  (outbox → Debezium) | (port 9093)       |
  +----------------+                      +--------+--------+
        ^                                           |
        |  COMPENSATE_CUSTOMER_BALANCE              |
        +-------------------------------------------+
                             |
                             v
                     +-----------------+
                     | Customer Svc    |
                     | (refund balance)|
                     +-----------------+

Debezium Connectors (one per service):
- outbox_order_connector.json  → watches order_db.out_box
- outbox_customer_connector.json → watches customer_db.out_box
- outbox_inventory_connector.json → watches inventory_db.out_box

Consul provides service discovery for the API Gateway and inter-service lookup (if direct calls were used).
```

---

## Patterns Used

| Pattern | Description | Where Applied |
|---------|-------------|---------------|
| **Saga (Choreography)** | No central orchestrator; services react to events to advance or rollback the transaction. | All three services |
| **Outbox Pattern** | Guarantees atomicity between local DB writes and event publishing by storing events in the same transaction. | Each service’s `out_box` table |
| **Change Data Capture (CDC)** | Debezium reads PostgreSQL WAL to capture inserts into `out_box` and publish them to Kafka without service-level Kafka producers. | Debezium connectors |
| **Single Message Transform (SMT)** | Debezium’s `EventRouter` transforms an `out_box` row into a Kafka record: sets topic via `aggregate_type`, moves `type` to header `eventType`, uses `aggregate_id` as key. | All Debezium configs |
| **Idempotent Consumer** | Each service logs processed Kafka message IDs in a `message_log` table to safely handle duplicate deliveries. | `MessageLogRepository` in each service |
| **Service Discovery** | Consul registers services; API Gateway uses Consul to resolve `lb://<service>` to actual instances. | `spring-cloud-starter-consul-discovery` + Consul container |
| **API Gateway** | Spring Cloud Gateway routes traffic to services based on path, enabling a single entry point. | `api-gateway` module |

---

## Service Deep Dives

### Order Service
- **Port:** 9090
- **Responsibilities:**
  - Accept `POST /orders` with `{customerId, productId, quantity, price}`
  - Persist `Order` (status `PENDING`)
  - Write `ORDER_CREATED` event to `out_box`
  - Consume `ORDER.events` to handle:
    - `RESERVE_CUSTOMER_BALANCE_SUCCESSFULLY` → trigger inventory reservation (write outbox event)
    - `RESERVE_CUSTOMER_BALANCE_FAILED` → mark order `CANCELED`
    - `RESERVE_PRODUCT_STOCK_SUCCESSFULLY` → mark order `COMPLETED`
    - `RESERVE_PRODUCT_STOCK_FAILED` → mark order `CANCELED`, emit `COMPENSATE_CUSTOMER_BALANCE`
- **Key Files:**
  - `OrderController.java`
  - `OrderUseCase.java` (business logic)
  - `OrderEventHandler.java` (Kafka consumer)
  - `EventHandlerAdapter.java` (Spring `@Bean` wiring)
  - `OrderRepositoryAdapter.java` (saves Order + `exportOutBoxEvent`)
  - `OutBox.java`, `MessageLog.java` (outbox & idempotency tables)

### Customer Service
- **Port:** 9091
- **Responsibilities:**
  - Provide `POST /customers` to create a customer record
  - Consume `ORDER_CREATED` events from `ORDER.events`
    - Attempt to reserve balance (`reserveBalance()`)
    - Write `RESERVE_CUSTOMER_BALANCE_SUCCESSFULLY` or `_FAILED` to its outbox
  - Consume `COMPENSATE_CUSTOMER_BALANCE` events
    - Refund the previously reserved amount (`compensateBalance()`)
- **Key Files:**
  - `CustomerController.java`
  - `CustomerUseCase.java` (`reserveBalance`, `compensateBalance`)
  - `CustomerEventHandler.java`
  - `EventHandlerAdapter.java`
  - `CustomerRepositoryAdapter.java`

### Inventory Service
- **Port:** 9093
- **Responsibilities:**
  - Provide `POST /products` to create a product record
  - Consume `RESERVE_CUSTOMER_BALANCE_SUCCESSFULLY` events from `ORDER.events`
    - Attempt to reserve stock (`reserveProduct()`)
    - Write `RESERVE_PRODUCT_STOCK_SUCCESSFULLY` or `_FAILED` to its outbox
- **Key Files:**
  - `ProductController.java`
  - `ProductUseCase.java` (`reserveProduct`)
  - `InventoryEventHandler.java`
  - `EventHandlerAdapter.java`
  - `ProductRepositoryAdapter.java`

### API Gateway
- **Port:** 8080
- **Responsibilities:**
  - Route `/customer-service/**` → `lb://customer-service`
  - Route `/order-service/**` → `lb://order-service`
  - Route `/inventory-service/**` → `lb://inventory-service`
  - Uses Consul for service discovery; no direct Kafka interaction.
- **Key Files:**
  - `ApiGatewayApplication.java`
  - `application.yml` (gateway routes + Consul config)

---

## Kafka & Event Flow

### Infrastructure
- **Message broker:** Redpanda (Kafka‑compatible) running in Docker, exposed internally on port `9092` and externally on `19092`.
- **Binder:** Spring Cloud Stream with `spring-cloud-stream-binder-kafka` connects each service to Redpanda at `localhost:19092`.
- **Connectors:** Three Debezium connectors (one per service) run in a single `debezium/connect:2.4` container.

### Topics (as seen by services)
| Topic | Produced by | Consumed by | Purpose |
|-------|-------------|-------------|---------|
| `ORDER.events` | All services (via Debezium) | Order, Customer, Inventory | Core saga coordination – carries balance/reservation results and compensation requests |
| `CUSTOMER.events` | Customer Service (if any) | Order Service (not used in current bindings) | Reserved for customer‑specific events |
| `PRODUCT.events` | Inventory Service (if any) | Order Service (not used) | Reserved for product‑specific events |

> **Note:** In the current code, all three services bind their listeners to `ORDER.events`. The `aggregate_type` column still determines the routing, but the listeners all subscribe to the same topic and discriminate via the `eventType` header.

### EventType Header – In Depth
The `eventType` header is copied by Debezium’s `EventRouter` SMT from the `out_box.type` column. It tells the **consumer** *what happened* in the **producing** service, enabling the correct branch of the saga logic.

| Header Value | Originating Service | Meaning |
|--------------|---------------------|---------|
| `ORDER_CREATED` | Order Service | A new order has been created; try to reserve customer balance. |
| `RESERVE_CUSTOMER_BALANCE_SUCCESSFULLY` | Customer Service | Balance reservation succeeded; proceed to inventory reservation. |
| `RESERVE_CUSTOMER_BALANCE_FAILED` | Customer Service | Not enough balance; abort the saga (order → CANCELED). |
| `RESERVE_PRODUCT_STOCK_SUCCESSFULLY` | Inventory Service | Stock reserved successfully; order → COMPLETED. |
| `RESERVE_PRODUCT_STOCK_FAILED` | Inventory Service | Insufficient stock; abort and trigger compensation (order → CANCELED, emit COMPENSATE_CUSTOMER_BALANCE). |
| `COMPENSATE_CUSTOMER_BALANCE` | Order Service | Refund the previously reserved balance. |

**How it’s read:**
```java
var eventType = event.getHeaders().get("eventType", byte[].class);
var eventTypeString = new String(eventType, StandardCharsets.UTF_8);
switch (eventTypeString) {
    case RESERVE_CUSTOMER_BALANCE_SUCCESSFULLY -> { ... }
    // ...
}
```
Because the payload (`PlacedOrderEvent`) is identical for every step, the `eventType` header is the **only** way to know *which* stage of the saga the message represents.

---

## Outbox Pattern & CDC

### Why Outbox?
- Directly publishing to Kafka from the service would be a **second, non‑transactional write** after the DB commit → risk of missing events if the publish fails.
- Writing the event to a local `out_box` table **in the same transaction** as the business data guarantees atomicity: either both succeed or both roll back.

### How CDC Completes the Loop
1. Service commits transaction → PostgreSQL writes WAL entry for the `out_box` insert.
2. Debezium tails the WAL, converts the insert into a Kafka Connect source record.
3. The `EventRouter` SMT:
   - Sets Kafka **topic** = `${aggregate_type}.events` (e.g., `ORDER.events`)
   - Copies `out_box.type` → Kafka header `eventType`
   - Uses `out_box.aggregate_id` as the Kafka **message key**
   - Serializes the `payload` column (JSON) as the Kafka **value**
4. The record appears on the topic; any service listening can consume it.
5. After processing, the consumer may write a **new** row to its own `out_box`, restarting the cycle.

### Idempotency via MessageLog
Each service stores the Kafka message ID (`MessageHeaders.getId()`) in a `message_log` table after successful processing. On redelivery, the handler checks `if (!messageLogRepository.isMessageProcessed(messageId))` and skips already‑processed messages.

---

## Additional Notes & Best Practices

### Observability
- Each service logs key steps (receiving event, starting/finishing process, saving outbox).
- Consider adding metrics (Micrometer) and distributed tracing (Spring Cloud Sleuth + Zipkin/Jager) for production.

### Schema Evolution
- The `out_box.payload` column is `jsonb` (via Hibernate `JsonType`). Changes to the event structure should be backward‑compatible or handled with a schema registry.

### Partitioning & Ordering
- Using `aggregate_id` as the Kafka key guarantees that all events for a given aggregate (e.g., a specific order) go to the same partition, preserving order per‑aggregate.
- Different aggregates (different orders) can be processed in parallel.

### Failure Handling & Retries
- Debezium will retry on transient failures.
- Spring Cloud Stream provides built‑in retry/recovery mechanisms; the `@Transactional` on handler methods ensures that if processing fails, the message is not acknowledged and will be redelivered.
- The `message_log` guard prevents side‑effects from duplicate processing.

### Scaling
- Services are stateless aside from their DB; you can run multiple instances behind a load balancer (Consul provides the registration).
- Kafka consumer groups (managed by Spring Cloud Stream) allow multiple instances to share the workload.

### Security (Not Shown)
- In production, enable SASL/SSL for Kafka, encrypt database connections, and restrict Consul access.

### Testing
- The project can be run locally via Docker Compose (`docker compose up`).  
- Unit tests exist for Use Cases; integration tests would spin up embedded Kafka (or use Testcontainers) to verify the full saga flow.

---

## Running the Project

1. **Prerequisites**
   - Docker & Docker Compose
   - Java 21 (or use the wrapper)
   - Maven

2. **Start the stack**
   ```bash
   docker compose up -d
   ```
   This brings up:
   - Three PostgreSQL instances (`order_db`, `customer_db`, `inventory_db`)
   - Redpanda (Kafka) + Schema Registry + HTTP Proxy
   - Consul
   - Debezium (with all three connectors)

3. **Build & run services**
   ```bash
   ./mvnw clean package
   # Then run each service jar or launch from IDE
   java -jar order-service/target/*.jar
   java -jar customer-service/target/*.jar
   java -jar inventory-service/target/*.jar
   java -jar api-gateway/target/*.jar
   ```
   (Alternatively, use your IDE to run the main classes.)

4. **Test the flow**
   - Create a customer:
     ```bash
     curl -X POST http://localhost:8080/customer-service/customers \
          -H "Content-Type: application/json" \
          -d '{"name": "John Doe", "email": "john@example.com", "balance": 1000}'
     ```
   - Create a product:
     ```bash
     curl -X POST http://localhost:8080/inventory-service/products \
          -H "Content-Type: application/json" \
          -d '{"name": "Widget", "description": "A useful widget", "price": 50, "stocks": 10}'
     ```
   - Place an order (replace IDs with those returned above):
     ```bash
     curl -X POST http://localhost:8080/order-service/orders \
          -H "Content-Type: application/json" \
          -d '{"customerId": "<customer-id>", "productId": "<product-id>", "quantity": 2, "price": 50}'
     ```
   - Check logs of each service to see the saga progress.  
   - Verify final state:
     - Order status (`SELECT * FROM "order"` in `order_db`) → `COMPLETED`
     - Customer balance decreased accordingly (`SELECT * FROM customer` in `customer_db`)
     - Product stock reduced (`SELECT * FROM product` in `inventory_db`)

5. **Stop & clean up**
   ```bash
   docker compose down -v
   ```
---