# qPayFlow Microservices (`cmd/`)

This directory contains the entry points (`main.go`) and internal business modules for the **6 core microservices** that power the **qPayFlow Distributed Payment Processing Platform**.

Each service is designed around specific **Distributed System & Fintech Patterns** to handle concurrency, data consistency, failure recovery, and horizontal scalability.

---

## Service Overview & Architecture Roles

```
                           +----------------------+
                           |     API Gateway      | (Port 8000)
                           | (Ingress / Limiting) |
                           +----------+-----------+
                                      |
                     +----------------+----------------+
                     | (REST Proxy)                    | (REST Proxy)
                     v                                 v
          +--------------------+             +--------------------+
          |  Payment Service   |             |  Account Service   |
          |    (Port 8001)     |             |    (Port 8002)     |
          | (Outbox & Saga)    |             | (Ledger & Locks)   |
          +----+----------+----+             +--------------------+
               |          ^                            ^
 (Outbox Relay)|          | (Fraud Evaluated)          | (Saga Step)
               v          |                            |
          +----+----------+----+                       |
          |    Apache Kafka    |-----------------------+
          +----+----------+----+
               |          |
 (Payment Evt) |          | (Payment Completed/Failed)
               v          v
    +------------------+  +----------------------+  +----------------------+
    |  Fraud Service   |  | Notification Service |  |  Settlement Service  |
    |   (Port 8003)    |  |     (Port 8004)      |  |     (Port 8005)      |
    | (Velocity Rules) |  |   (HMAC Webhooks)    |  |  (Leader Election)   |
    +------------------+  +----------------------+  +----------------------+
```

### High-Level System Architecture & Communication Topology

The **qPayFlow** platform is architected around a hybrid communication topology that clearly separates **synchronous client-facing ingress** from **asynchronous event-driven core settlement**. At the edge, the **API Gateway** acts as the single point of entry, terminating external client traffic, enforcing distributed rate limiting via Redis sliding-window algorithms, injecting standard W3C `traceparent` headers for distributed observability, and proxying requests to internal domain microservices. This design ensures that malicious or bursty client traffic is throttled at the perimeter before consuming backend compute and database connection resources.

Within the internal network, operations that require immediate acknowledgment follow a fast synchronous path, while complex, multi-step transaction workflows transition seamlessly into an asynchronous **Saga Choreography** orchestrated through Apache Kafka. This separation guarantees high availability and ultra-low latency for incoming requests while isolating long-running validations, external third-party integrations, and background settlement processes from the critical request-response path.

### Transaction Lifecycle & Asynchronous Saga Choreography

The lifecycle of every payment in qPayFlow transitions through a strictly validated distributed workflow designed to prevent inconsistent states and data loss:

1. **Ingress & Idempotent Initiation:** When a client submits a payment request with an `Idempotency-Key` header, the request is intercepted by the API Gateway, validated, and forwarded to the **Payment Service**. The Payment Service runs a two-tier idempotency check (fast-path Redis cache + database unique index). If the request is new, it opens a local database transaction to persist the payment in a `PENDING` state while simultaneously writing a `PaymentCreated` record to the `outbox_events` table (**Transactional Outbox Pattern**). The transaction commits atomically, and the client receives an immediate `201 Created` response.
2. **Reliable Event Streaming:** An independent background **Outbox Relay Worker** continuously polls uncommitted outbox events and publishes them to the Kafka `payment-events` topic using the `source_account_id` as the message partition key. This guarantees strict in-order message delivery per account across distributed consumer instances.
3. **Real-Time Fraud Evaluation:** The **Fraud Service** consumes the `PaymentCreated` event from Kafka, executes sliding-window transaction velocity checks against Redis, enforces threshold rules ($10,000 max), and publishes a `FraudCheckedEvent` to the `fraud-events` topic.
4. **Saga Step Execution & Balance Settlement:** The Payment Service's Saga Consumer listens to the fraud evaluation outcome. If approved, it calls the **Account Service** to transfer funds under row-level database locks and **Circuit Breaker** protection (`pkg/resilience`). Upon successful balance transfer, the payment status transitions to `SUCCESS` (or `FAILED` if rejected/insufficient funds), and a terminal event (`PaymentCompleted` or `PaymentFailed`) is emitted back to Kafka.
5. **Downstream Notifications & Webhooks:** The **Notification Service** consumes terminal payment events, triggers mock SMS/Email notifications, and generates cryptographically signed **HMAC-SHA256 Webhooks** (`X-Signature-SHA256`) to notify merchant backends.

### Strict Financial Consistency & Double-Entry Ledger Principles

Financial accuracy is the primary non-negotiable invariant of qPayFlow. The **Account Service** serves as the system of record for account balances and ledger entries, adhering to core fintech architectural standards:

- **Double-Entry Bookkeeping:** No account balance is ever updated in isolation. Every transfer strictly generates a balanced pair of ledger entries: a `DEBIT` on the source account and a `CREDIT` on the destination account, guaranteeing the fundamental accounting invariant that $\sum \text{Debit} == \sum \text{Credit}$.
- **Deadlock-Free Concurrency Control:** In high-throughput distributed payment systems, concurrent cross-transfers between two accounts (e.g., Account A $\to$ Account B while Account B $\to$ Account A) can easily cause database deadlocks. The Account Service eliminates deadlocks by sorting account identifiers into a deterministic lexicographical sequence (`if fromAccID < toAccID`) before acquiring pessimistic row locks (`SELECT ... FOR UPDATE`).
- **Pluggable Locking Strategies:** The service provides experimental support for three concurrency mechanisms: **Pessimistic Locking** (PostgreSQL row locks), **Optimistic Locking** (version-based checks `WHERE version = $oldVersion`), and **Distributed Locking** (Redis `SETNX` with Lua script token release), enabling comparative benchmarking of latency and throughput under high contention.

### Background Reconciliation, Leader Election & Fault Tolerance

To ensure long-term data integrity and recover from unexpected distributed failures, the **Settlement Service** operates as an autonomous background audit daemon:

- **Redis Lease Leader Election:** In a multi-replica deployment, instances of the Settlement Service compete for leadership using atomic Redis leases (`SETNX leader:settlement:lock <node_id> EX 10` paired with periodic heartbeat renewal). Only the elected **Leader node** executes heavy scheduled reconciliation jobs, eliminating duplicate execution while providing automatic failover if the leader node terminates.
- **Double-Entry Ledger Integrity Audit:** The elected leader periodically executes SQL verification jobs to audit the invariant:
  $$\sum \text{Ledger Entries (Credit} - \text{Debit)} == \text{Account Balance}$$
  Any mathematical mismatch indicates database corruption or race conditions and triggers high-priority alerts.
- **Three-Way Reconciliation:** The service cross-references successful payment transactions against ledger records to detect anomalies such as missing ledger pairs or orphaned transactions. Combined with retry loops, exponential backoff with full jitter, and Dead Letter Queue (DLQ) routing in `pkg/kafka`, qPayFlow guarantees end-to-end fault tolerance, auditability, and zero data loss.

---

## 1. API Gateway (`cmd/api-gateway`)

- **Port:** `8000`
- **Primary Role:** Edge Ingress Proxy, API Rate Limiting, Request Logging, and Distributed Tracing Injection.
- **Distributed System Concepts Applied:**
  - **Distributed Rate Limiting:** Atomic sliding-window rate limiting using Redis `ZSET` Lua script (`ZREMRANGEBYSCORE`, `ZCARD`, `ZADD`, `EXPIRE`) to protect downstream services from burst traffic.
  - **W3C Distributed Tracing (`traceparent`):** Generates or extracts `traceparent` (`00-<trace_id>-<span_id>-01`) and propagates it downstream across HTTP headers.
  - **Reverse Proxy Routing:** Transparently forwards client traffic to `/payments` and `/accounts`.

---

## 2. Payment Service (`cmd/payment-service`)

- **Port:** `8001`
- **Primary Role:** Payment lifecycle management, idempotency enforcement, transactional outbox publishing, and Saga orchestration.
- **Distributed System Concepts Applied:**
  - **Two-Tier Idempotency:** Fast-path cache check via Redis (`idempotency:<key>`) with response caching + PostgreSQL unique index to guarantee zero double-charging.
  - **Transactional Outbox Pattern:** Atomic DB transaction writing `payments` (`PENDING`) and `outbox_events` (`PaymentCreated`).
  - **Outbox Relay Worker:** Background goroutine polling `outbox_events` (`status = 'PENDING'`) and reliably publishing to Kafka topic `payment-events` partitioned by `source_account_id`.
  - **Saga Choreography Consumer:** Listens to `fraud-events`, calls Account Service with **Circuit Breaker** protection (`pkg/resilience`), and transitions payment status to `SUCCESS` or `FAILED`.

---

## 3. Account Service (`cmd/account-service`)

- **Port:** `8002`
- **Primary Role:** Core account ledger, balance persistence, fund reservations, and high-concurrency transfers.
- **Distributed System Concepts Applied:**
  - **Deadlock Prevention (Lock Ordering):** Enforces deterministic alphabetical locking (`if fromAccID < toAccID`) before executing `SELECT ... FOR UPDATE` to avoid database deadlocks during concurrent cross-transfers.
  - **Double-Entry Bookkeeping:** Every transaction creates balanced `DEBIT` (source) and `CREDIT` (destination) entries in `balance_ledgers`, ensuring `Sum(Debit) == Sum(Credit)`.
  - **Pluggable Concurrency Controls:** Supports Pessimistic Locking (`FOR UPDATE`), Optimistic Locking (`version = version + 1`), and Redis Distributed Locks (`pkg/redis.AcquireLock`).

---

## 4. Fraud Service (`cmd/fraud-service`)

- **Port:** `8003`
- **Primary Role:** Asynchronous real-time fraud rule evaluation and anomaly detection.
- **Distributed System Concepts Applied:**
  - **Sliding-Window Velocity Check:** Evaluates transactions per minute per account in real-time via Redis `TxPipeline` on sorted sets.
  - **Transaction Threshold Guard:** Enforces single transaction limits (e.g. `$10,000` max limit).
  - **Event-Driven Feedback:** Consumes `PaymentCreated` events and emits `FraudCheckedEvent` to Kafka topic `fraud-events`.

---

## 5. Notification Service (`cmd/notification-service`)

- **Port:** `8004`
- **Primary Role:** Event-driven multi-channel notification dispatcher (SMS, Email, Merchant Webhooks).
- **Distributed System Concepts Applied:**
  - **Event-Driven Consumer:** Subscribes to terminal transaction events (`PaymentCompleted`, `PaymentFailed`).
  - **Cryptographic Webhook Signatures (HMAC-SHA256):** Computes HMAC-SHA256 signature (`X-Signature-SHA256`) and attaches timestamp headers to guarantee webhook authenticity and anti-tampering.
  - **Context Propagation:** Preserves `traceparent` across async Kafka boundary to outbound webhook calls.

---

## 6. Settlement Service (`cmd/settlement-service`)

- **Port:** `8005`
- **Primary Role:** Background distributed reconciliation, ledger verification, and leader-elected audit jobs.
- **Distributed System Concepts Applied:**
  - **Redis Lease Leader Election:** Nodes compete for leadership using `SETNX leader:settlement:lock <node_id> EX 10` with periodic heartbeat lease renewals. Only the active Leader node executes reconciliation jobs.
  - **Ledger Integrity Reconciliation:** Periodically verifies mathematical equality between current balances and ledger entries:
    $$\sum \text{Ledger Entries (Credit} - \text{Debit)} == \text{Account Balance}$$
  - **Three-Way Reconciliation:** Audits settled `payments` against paired ledger entries to detect discrepancies (`Missing In Ledger`, `Amount Mismatch`).

---

## How to Run the Services

### Prerequisites

Ensure the core infrastructure containers (Postgres, Redis, Kafka) are running:

```bash
cd deployments/docker
docker compose up -d postgres redis kafka
```

Apply database migrations:
```bash
# Using Postgres CLI or migration tool against localhost:5432
psql -U qpayflow -d qpayflow_db -h localhost -f ../../scripts/migrations/000001_init_schema.up.sql
```

---

### Method A: Run All Services via Docker Compose (Recommended)

Run all 6 microservices together in containerized mode:

```bash
cd deployments/docker
docker compose --profile full up --build -d
```

Check status:
```bash
docker compose ps
```

---

### Method B: Run Individual Services Locally (Native Go)

Open separate terminal windows for each service and execute:

#### 1. API Gateway
```bash
go run ./cmd/api-gateway
# Listening on http://localhost:8000
```

#### 2. Payment Service
```bash
go run ./cmd/payment-service
# Listening on http://localhost:8001
```

#### 3. Account Service
```bash
go run ./cmd/account-service
# Listening on http://localhost:8002
```

#### 4. Fraud Service
```bash
go run ./cmd/fraud-service
# Listening on http://localhost:8003
```

#### 5. Notification Service
```bash
go run ./cmd/notification-service
# Listening on http://localhost:8004
```

#### 6. Settlement Service
```bash
go run ./cmd/settlement-service
# Listening on http://localhost:8005
```

---

## Service Ports & Health Endpoints Summary

| Service | Port | Health Endpoint | Key Responsibilities |
| :--- | :--- | :--- | :--- |
| **API Gateway** | `8000` | `GET http://localhost:8000/health` | Rate limiting, routing, tracing context |
| **Payment Service** | `8001` | `GET http://localhost:8001/health` | Idempotency, Outbox pattern, Saga consumer |
| **Account Service** | `8002` | `GET http://localhost:8002/health` | Double-Entry ledger, deadlock-free transfers |
| **Fraud Service** | `8003` | N/A (Event Worker) | Velocity checks, fraud evaluation |
| **Notification Service**| `8004` | N/A (Event Worker) | HMAC signed webhooks, alerts |
| **Settlement Service** | `8005` | `GET http://localhost:8005/health` | Leader election, 3-way ledger reconciliation |
