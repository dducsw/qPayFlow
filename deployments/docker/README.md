# qPayFlow Docker Compose Environment

Environment configuration and container orchestration for **qPayFlow**.

## Overview of Services

| Service | Category | Image / Source | Default Port | UI / Endpoint |
|---|---|---|---|---|
| `postgres` | Infrastructure | `postgres:16-alpine` | `5432` | `localhost:5432` |
| `redis` | Infrastructure | `redis:7-alpine` | `6379` | `localhost:6379` |
| `kafka` | Infrastructure | `apache/kafka:4.1.0` | `9092` | `localhost:9092` |
| `kafka-ui` | Management | `provectuslabs/kafka-ui:latest` | `8080` | `http://localhost:8080` |
| `prometheus` | Observability | `prom/prometheus:v2.51.0` | `9090` | `http://localhost:9090` |
| `grafana` | Observability | `grafana/grafana:10.4.0` | `3000` | `http://localhost:3000` (`admin`/`admin`) |
| `jaeger` | Observability | `jaegertracing/all-in-one:1.55` | `16686` / `4317` | `http://localhost:16686` |
| `api-gateway` | Microservice | `cmd/api-gateway` | `8000` | `http://localhost:8000` |
| `payment-service` | Microservice | `cmd/payment-service` | `8001` | `http://localhost:8001` |
| `account-service` | Microservice | `cmd/account-service` | `8002` | `http://localhost:8002` |
| `fraud-service` | Microservice | `cmd/fraud-service` | `8003` | `http://localhost:8003` |
| `notification-service` | Microservice | `cmd/notification-service` | `8004` | `http://localhost:8004` |

---

## Usage Guide

### 1. Copy Environment Variables
```bash
cp .env.example .env
```

### 2. Start Infrastructure Services Only (Recommended for local Go dev)
```bash
docker compose up -d postgres redis kafka kafka-ui prometheus grafana jaeger
```

### 3. Start Full Application Stack (Infra + Microservices)
```bash
docker compose --profile full up -d --build
```

### 4. Stop Services & Clean Volumes
```bash
# Stop containers
docker compose down

# Stop containers and wipe persisted data volumes
docker compose down -v
```
