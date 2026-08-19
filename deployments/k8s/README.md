# qPayFlow Kubernetes Deployment (k3d / K8s Cluster)

This directory contains production-grade **Kubernetes Manifests** organized by tool/service (Tool-based Modular Structure) for deploying the **qPayFlow** platform on **k3d (Lightweight K3s in Docker)** or any Kubernetes cluster (EKS, GKE, AKS, Minikube).

---

## 📁 Directory Structure by Tool & Service

```
deployments/k8s/
├── k3d-config.yaml              # Multi-node k3d cluster config (1 Server + 2 Agents, port mappings)
├── 00-base/                     # Core Kubernetes foundations
│   ├── namespace.yaml           # Namespace `qpayflow`
│   ├── configmap.yaml           # Environment variables, service hosts & ports
│   └── secret.yaml              # Database credentials & JWT secret
├── 01-postgres/                 # PostgreSQL Relational Database
│   ├── pvc.yaml                 # Persistent Volume Claim (2Gi)
│   ├── statefulset.yaml         # StatefulSet (1 replica, pg_isready health checks)
│   └── service.yaml             # ClusterIP Service (Port 5432)
├── 02-redis/                    # Redis Cache & Distributed Lock
│   ├── pvc.yaml                 # Persistent Volume Claim (1Gi)
│   ├── deployment.yaml          # Deployment (AOF persistence, ping health checks)
│   └── service.yaml             # ClusterIP Service (Port 6379)
├── 03-kafka/                    # Event Streaming & Management
│   ├── pvc.yaml                 # Persistent Volume Claim (3Gi)
│   ├── statefulset.yaml         # Apache Kafka KRaft StatefulSet
│   ├── service.yaml             # ClusterIP Service (Port 9092, 9093)
│   └── kafka-ui.yaml            # Kafka UI Web Dashboard (Port 8080)
├── 04-observability/            # Monitoring & Distributed Tracing
│   ├── prometheus.yaml          # Prometheus Deployment, ConfigMap & Service (Port 9090)
│   ├── grafana.yaml             # Grafana Deployment, Datasource Config & Service (Port 3000)
│   └── jaeger.yaml              # Jaeger All-in-One OTLP Tracing (Port 16686, 4317)
├── 05-apps/                     # 5 Core Application Microservices
│   ├── api-gateway.yaml         # REST API Gateway (2 replicas, rolling update)
│   ├── payment-service.yaml     # Payment Core Service (2 replicas, HTTP + gRPC)
│   ├── account-service.yaml     # Account & Ledger Service (2 replicas, HTTP + gRPC)
│   ├── fraud-service.yaml       # Realtime Fraud Engine (2 replicas, HTTP + gRPC)
│   └── notification-service.yaml# Notification Worker (2 replicas)
├── 06-ingress/                  # Ingress Controller Routing
│   └── ingress.yaml             # Routes host traffic to API Gateway, UIs & Observability
├── 07-hpa/                      # Autoscaling (Horizontal Pod Autoscaler)
│   └── hpa.yaml                 # HPA rules (2-10 replicas based on CPU/RAM threshold)
└── README.md                    # Deployment guide & instructions
```

---

## 🛠️ Prerequisites

1. **Docker**: `docker --version` (≥ 24.x)
2. **k3d**: `k3d --version` (≥ 5.x) — Install via `winget install Rancher.k3d` or `brew install k3d`
3. **kubectl**: `kubectl version --client`

---

## 🚀 Step-by-Step Deployment Guide

### Step 1: Create Multi-Node k3d Cluster

Create a local K8s cluster with **1 Control-plane (Server)** and **2 Worker Nodes (Agents)**:

```bash
k3d cluster create --config deployments/k8s/k3d-config.yaml
```

Verify nodes status:
```bash
kubectl get nodes
```

---

### Step 2: Build & Import Docker Images into k3d

Build Docker images for all 5 microservices from the project root:

```bash
# Build 5 microservices
docker build -f deployments/docker/Dockerfile.service --build-arg SERVICE_NAME=api-gateway -t qpayflow/api-gateway:latest .
docker build -f deployments/docker/Dockerfile.service --build-arg SERVICE_NAME=payment-service -t qpayflow/payment-service:latest .
docker build -f deployments/docker/Dockerfile.service --build-arg SERVICE_NAME=account-service -t qpayflow/account-service:latest .
docker build -f deployments/docker/Dockerfile.service --build-arg SERVICE_NAME=fraud-service -t qpayflow/fraud-service:latest .
docker build -f deployments/docker/Dockerfile.service --build-arg SERVICE_NAME=notification-service -t qpayflow/notification-service:latest .

# Directly import images into k3d cluster (no Docker Hub push required)
k3d image import \
  qpayflow/api-gateway:latest \
  qpayflow/payment-service:latest \
  qpayflow/account-service:latest \
  qpayflow/fraud-service:latest \
  qpayflow/notification-service:latest \
  -c qpayflow-cluster
```

---

### Step 3: Apply All Manifests

Apply all manifests recursively to the `qpayflow` namespace:

```bash
kubectl apply -R -f deployments/k8s/
```

Or apply individual tool components sequentially:
```bash
kubectl apply -f deployments/k8s/00-base/
kubectl apply -f deployments/k8s/01-postgres/
kubectl apply -f deployments/k8s/02-redis/
kubectl apply -f deployments/k8s/03-kafka/
kubectl apply -f deployments/k8s/04-observability/
kubectl apply -f deployments/k8s/05-apps/
kubectl apply -f deployments/k8s/06-ingress/
kubectl apply -f deployments/k8s/07-hpa/
```

---

### Step 4: Verify Deployment Status

```bash
# Check all Pods, Services, Deployments, and StatefulSets
kubectl get all -n qpayflow

# Check HPA autoscaling status
kubectl get hpa -n qpayflow

# Tail logs for a specific service (e.g. payment-service)
kubectl logs -f -l app=payment-service -n qpayflow
```

---

## 🌐 Local Access Endpoints

| Service | Local URL | Description |
| :--- | :--- | :--- |
| **API Gateway** | [http://localhost:8000](http://localhost:8000) or [http://localhost/](http://localhost/) | Main REST API Gateway |
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) or [http://localhost/kafka-ui](http://localhost/kafka-ui) | Kafka Topic & Message Dashboard |
| **Grafana** | [http://localhost:3000](http://localhost:3000) (admin / admin) | Metrics & System Dashboards |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | Metric Collection & Alerting Engine |
| **Jaeger UI** | [http://localhost:16686](http://localhost:16686) | Distributed Tracing UI |

---

## 🛑 Teardown & Cleanup

When finished:
```bash
# Delete entire k3d cluster
k3d cluster delete qpayflow-cluster
```
