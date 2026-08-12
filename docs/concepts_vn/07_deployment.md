# Bài 07: Triển Khai Hạ Tầng & Containerization (Docker, Kubernetes & HPA)

> **Tóm tắt bài viết**: Phân tích kiến trúc triển khai hệ thống thanh toán `qPayFlow` trên nền tảng **Docker** và **Kubernetes (K8s)**, cơ chế tự động mở rộng (Horizontal Pod Autoscaler - HPA), kỹ thuật **Zero-Downtime Deployment (Rolling Update / Canary)** và quản lý cấu hình bảo mật.

---

## 1. Môi Trường Triển Khai Trong Fintech

Trong hệ thống thanh toán, việc triển khai ứng dụng phải đảm bảo các tiêu chí khắt khe:

1. **Zero Downtime (Không gián đoạn)**: Khi deploy phiên bản code mới, luồng thanh toán của khách hàng không được bị gián đoạn hay ngắt kết nối.
2. **Auto-Scaling (Tự động mở rộng)**: Hệ thống phải tự động tăng số lượng instance (Pods) khi lưu lượng giao dịch tăng đột biến (Flash Sale) và tự động giảm Pods khi thấp điểm để tối ưu chi phí hạ tầng.
3. **Isolation & Security**: Đảm bảo phân tách môi trường, mã hóa ConfigMap/Secrets chứa password DB, TLS Certificate, và API Keys.

---

## 2. Kiến Trúc Triển Khai Kubernetes (K8s Architecture)

![K8s Deployment Topology](../diagrams/07_1_k8s_deployment.svg)

### Các thành phần chính trong K8s:
- **Ingress Controller (NGINX / Envoy)**: Tiếp nhận HTTPS Request từ bên ngoài, giải mã SSL/TLS (TLS Termination) và điều hướng vào các Service nội bộ.
- **Microservice Deployments (Go / Java)**: Đóng gói dưới dạng Stateless Pods, tự động mở rộng theo HPA dựa trên chỉ số CPU và RAM utilization.
- **StatefulSets (Postgres, Kafka, Redis)**: Đảm bảo tính nhất quán dữ liệu và định danh mạng cố định cho các datastore có lưu trữ trạng thái.

---

## 3. Kỹ Thuật Đóng Gói Docker Multi-Stage Build

Để tối ưu kích thước image và nâng cao bảo mật cho Go Microservices trong `qPayFlow`:

```dockerfile
# Stage 1: Build binary với Golang Alpine
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/payment-service ./cmd/payment

# Stage 2: Minimal Runtime Image (Distroless / Scratch)
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=builder /app/payment-service /payment-service
USER nonroot:nonroot
ENTRYPOINT ["/payment-service"]
```

> **Lợi ích**: Kích thước Image chỉ $\sim 15\text{MB}$ (thay vì $800\text{MB}$), không chứa shell `/bin/sh` giúp triệt tiêu rủi ro bị hacker chèn lệnh độc hại.

---

## 4. Horizontal Pod Autoscaler (HPA) & Rolling Update

### 4.1. Cấu hình HPA tự động Scale Pods
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: payment-service-hpa
  namespace: qpayflow
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: payment-service
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

### 4.2. Chiến lược Rolling Update (Zero Downtime)
Khi cập nhật phiên bản mới, Kubernetes sẽ cập nhật từng Pod một mà không dừng toàn bộ hệ thống:

```yaml
spec:
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 25%        # Cho phép tạo thêm tối đa 25% Pod mới
      maxUnavailable: 0     # Luôn đảm bảo 100% Pod cũ sẵn sàng phục vụ
```

---

## 5. Tài Liệu Tham Khảo (Reputable References)

1. **Kubernetes Documentation**: [Production Best Practices & Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)
2. **AWS EKS Architecture**: [AWS EKS Best Practices Guide](https://aws.github.io/aws-eks-best-practices/)
3. **Google Cloud**: [Containerizing Go Applications with Distroless](https://cloud.google.com/blog/products/containers-kubernetes/distroless-docker-images)
4. **Cisco DevNet**: [Zero-Downtime Deployment Strategies with Kubernetes](https://developer.cisco.com/)

