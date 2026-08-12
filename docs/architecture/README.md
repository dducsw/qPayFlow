# Đặc Tả Kiến Trúc, Dịch Vụ & Dữ Liệu Nền Tảng Thanh Toán (qPayFlow System Architecture Specification)

> **Tóm tắt tài liệu**: Tài liệu này đóng vai trò là **Đặc tả Kỹ thuật Hệ thống (System Architecture Specification)** chính thức của dự án **qPayFlow**. Tài liệu mô tả chi tiết Tổng quan Kiến trúc phân tán, Ranh giới & Chức năng của các Microservices, Mô hình Dữ liệu (Database Schema DDL & ERD), và Quy trình xử lý Luồng giao dịch tài chính an toàn.

---

## 1. Tổng Quan Kiến Trúc Hệ Thống (System Architecture Overview)

**qPayFlow** là nền tảng xử lý thanh toán phân tán (Distributed Payment Processing Platform) được thiết kế theo kiến trúc **Cloud-Native Event-Driven Microservices**. Hệ thống đặt mục tiêu đạt độ khả dụng cao (High Availability), tính mở rộng theo chiều ngang (Horizontal Scalability), bảo vệ số dư tài khoản theo chuẩn ngân hàng (Zero Financial Data Loss & Double-Entry Bookkeeping), và xử lý giao dịch với độ trễ thấp ($p99 < 500\text{ms}$).

### 1.1. Sơ Đồ Tổng Quan Kiến Trúc (Architecture Topology Diagram)

![qPayFlow System Architecture Specification](../diagrams/00_arch_system_spec.svg)

### 1.2. Các Nguyên Tắc Kiến Trúc Cốt Lõi (Architectural Guiding Principles)

1. **Zero Financial Loss (Không mất tiền)**: Mọi biến động số dư phải được thực thi trong môi trường giao dịch an toàn (ACID Transaction) và tuân thủ **Nguyên tắc Sổ kép (Double-Entry Bookkeeping)**: $\sum \text{DEBIT} = \sum \text{CREDIT}$.
2. **Strict Idempotency (Tính vô hiệu tuyệt đối)**: Mọi request ghi (Write/Transfer) bắt buộc phải truyền `Idempotency-Key` để triệt tiêu hoàn toàn nguy cơ trừ tiền 2 lần (Double Charging) khi gặp sự cố rớt mạng.
3. **Decoupled Asynchronous Processing (Phân tách bất đồng bộ)**: Phân tách luồng xử lý đồng bộ API Gateway với luồng xử lý bất đồng bộ thông qua **Apache Kafka Event Bus** và **Debezium CDC (Change Data Capture)**.
4. **Resilience & Fault Tolerance (Khả năng chịu lỗi)**: Áp dụng các mẫu thiết kế Circuit Breaker, Exponential Backoff + Jitter Retry, Dead Letter Queue (DLQ) và Priority Load Shedding để bảo vệ hệ thống khi bị quá tải.

---

## 2. Đặc Tả Chi Tiết Các Microservices (Microservices Specification)

Hệ thống được tổ chức thành **6 Core Microservices** với ranh giới nghiệp vụ (Bounded Contexts) rõ ràng:

![Microservices Specification Specification](../diagrams/00_microservices_spec.svg)

---

### 2.1. API Gateway Service
- **Ngôn ngữ & Tech Stack**: Go (Golang) + Fiber / Gin Framework + Redis Cluster.
- **Ranh giới Chức năng (Responsibilities)**:
  - Tiếp nhận và giải mã TLS/HTTPS Traffic từ Client (Mobile / Web).
  - Xác thực Token (JWT Auth / OAuth2) & Định danh Client.
  - **Distributed Rate Limiting**: Sử dụng thuật toán Sliding Window Counter bằng Lua Script trên Redis Cluster.
  - **Idempotency Fast-Path Check**: Kiểm tra sự tồn tại của `Idempotency-Key` trong Redis Cache trước khi chuyển request vào bên trong.
  - **Distributed Tracing Header Injection**: Tạo và truyền header `traceparent` (W3C Trace Context) cho tất cả các request downstream.
  - **Load Shedding**: Chủ động ngắt kết nối các request thứ yếu (Analytics, Notification) khi CPU/RAM vượt $85\%$.

---

### 2.2. Payment Service
- **Ngôn ngữ & Tech Stack**: Go (Golang) + GORM / pgx + PostgreSQL.
- **Ranh giới Chức năng (Responsibilities)**:
  - Quản lý Vòng đời Giao dịch Thanh toán (Payment Lifecycle).
  - Điều phối **Payment State Machine**: `PENDING` $\rightarrow$ `PROCESSING` $\rightarrow$ `SUCCESS` / `FAILED` / `REFUNDED`.
  - **Transactional Outbox Pattern**: Thực hiện DB Transaction ghi đồng thời vào bảng `payments`, `transactions` và `outbox_events`.
  - **Outbox Worker Latency Measurement**: Cột `created_at` trong `outbox_events` đóng vai trò là mốc thời gian bắt buộc để đo đạc lag/latency khi so sánh luồng Polling Worker vs Debezium CDC (Experiment #5).
  - **Strict Idempotency Key Consistency**: Đồng bộ kiểu dữ liệu `VARCHAR(64)` giữa `payments.idempotency_key` (Foreign Key) và `idempotency_keys.key` (Primary Key).
  - Không trực tiếp thay đổi số dư ví; việc thay đổi số dư được phát ra dưới dạng Event `PaymentCreated` gửi cho Account Service.

---

### 2.3. Account & Core Ledger Service
- **Ngôn ngữ & Tech Stack**: Go (Golang) hoặc Java 17 / Spring Boot 3 + PostgreSQL RDBMS.
- **Ranh giới Chức năng (Responsibilities)**:
  - Quản lý Tài khoản (Accounts), Ví điện tử (Wallets) và Sổ cái Kế toán (Core Ledger).
  - **Double-Entry Bookkeeping Engine**: Đảm bảo mọi giao dịch đều sinh ra cặp bản ghi `DEBIT` (Nợ) và `CREDIT` (Có) bất biến trong bảng `ledger_entries`.
  - **At-Least-Once Ledger Idempotency**: Áp dụng Ràng buộc Duy nhất `UNIQUE(transaction_id, account_id, entry_type)` trực tiếp tại tầng Database cho bảng `ledger_entries` để ngăn ngừa tuyệt đối trùng bút toán khi Kafka Consumer retry event.
  - **Audit Trail & Time-series Reconciliation**: Bắt buộc cột `created_at` tại `ledger_entries` để phục vụ đối soát số dư theo cửa sổ thời gian và truy vết lịch sử biến động.
  - **Concurrency Control**: Sử dụng kết hợp **Optimistic Locking** (`version` column trong SQL), **Pessimistic Locking** (`SELECT FOR UPDATE`), và **Redis Distributed Lock (Redlock)** để chống Race Conditions và Double-Spending khi có hàng nghìn giao dịch song song vào cùng một tài khoản.

---

### 2.4. Fraud Detection Service
- **Ngôn ngữ & Tech Stack**: Go (Golang) + Redis Cluster (Data Structure: ZSET & Hashes).
- **Ranh giới Chức năng (Responsibilities)**:
  - Đánh giá rủi ro giao dịch theo thời gian thực (Real-Time Fraud Engine).
  - **Velocity Rules**: Kiểm tra tần suất giao dịch trong cửa sổ thời gian trượt (Sliding Window Counter trên Redis): Ví dụ không quá 5 giao dịch / phút cho cùng một thiết bị.
  - **Amount Thresholds & Risk Scoring**: Cảnh báo giao dịch giá trị bất thường hoặc vị trí địa lý lạ (IP Risk).
  - Phát Event `FraudCheckPassed` hoặc `FraudCheckFailed` (Kích hoạt luồng Saga Compensation).

---

### 2.5. Settlement & Reconciliation Service
- **Ngôn ngữ & Tech Stack**: Go (Golang) / Java 17 + PostgreSQL.
- **Ranh giới Chức năng (Responsibilities)**:
  - Quản lý luồng Quyết toán (Settlement) giữa Khách hàng, Ngân hàng đối tác và Merchant.
  - **Three-Way Reconciliation Engine**: Chạy Batch Job định kỳ (00:30 AM) đọc file EOD (CSV / MT940 từ SFTP Ngân hàng) để so sánh 3 chiều: *Internal Ledger* vs *Bank Statement* vs *Merchant Account*.
  - Tự động phân loại sai lệch (`MATCHED`, `MISSING_IN_PARTNER`, `MISSING_IN_INTERNAL`, `AMOUNT_MISMATCH`) và trigger lệnh đền bù (Auto-Credit Compensation).

---

### 2.6. Notification Service
- **Ngôn ngữ & Tech Stack**: Go (Golang) + Worker Pools + RabbitMQ / Kafka Consumer.
- **Ranh giới Chức năng (Responsibilities)**:
  - Tiêu thụ các sự kiện `PaymentCompleted`, `PaymentFailed`, `PaymentRefunded` từ Kafka.
  - Gửi Push Notification (Firebase FCM), SMS (Twilio/Kính Ngân hàng), Email (SendGrid) tới người dùng.
  - Bắn Webhooks sự kiện đến Merchant (kèm chữ ký số **HMAC-SHA256** bảo mật).

---

## 3. Đặc Tả Mô Hình Dữ Liệu & Schemas (Data Architecture & Database Schemas)

### 3.1. Sơ Đồ Quan Hệ Thực Thể (Entity-Relationship Diagram - ERD)

![Entity-Relationship Diagram (ERD Schema)](../diagrams/00_erd_schema.svg)

---

### 3.2. Chi Tiết Bảng Dữ Liệu SQL DDL (Database Table Definitions)

#### Bảng `accounts` (Tài khoản & Số dư)
```sql
CREATE TABLE accounts (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    balance NUMERIC(18, 4) NOT NULL DEFAULT 0.0000 CHECK (balance >= 0),
    version BIGINT NOT NULL DEFAULT 1, -- Optimistic Locking version column
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- 'ACTIVE', 'FROZEN', 'CLOSED'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_accounts_user_id ON accounts(user_id);
```

#### Bảng `ledger_entries` (Sổ cái Hạch toán Nợ/Có Bất biến)
```sql
CREATE TABLE ledger_entries (
    id BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL,
    account_id BIGINT NOT NULL REFERENCES accounts(id),
    entry_type VARCHAR(10) NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount NUMERIC(18, 4) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    balance_after NUMERIC(18, 4) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_ledger_entries UNIQUE (transaction_id, account_id, entry_type)
);

CREATE INDEX idx_ledger_entries_account_id ON ledger_entries(account_id);
CREATE INDEX idx_ledger_entries_tx_id ON ledger_entries(transaction_id);
CREATE INDEX idx_ledger_entries_created_at ON ledger_entries(created_at);
```

#### Bảng `payments` (Giao dịch Thanh toán)
```sql
CREATE TABLE payments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(64) NOT NULL UNIQUE REFERENCES idempotency_keys(key),
    user_id BIGINT NOT NULL,
    amount NUMERIC(18, 4) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'PROCESSING', 'SUCCESS', 'FAILED', 'REFUNDED'
    payment_method VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_payments_user_status ON payments(user_id, status);
```

#### Bảng `transactions` (Giao dịch Chuyển khoản)
```sql
CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id UUID REFERENCES payments(id),
    source_account_id BIGINT REFERENCES accounts(id),
    target_account_id BIGINT REFERENCES accounts(id),
    amount NUMERIC(18, 4) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transactions_payment_id ON transactions(payment_id);
```

#### Bảng `idempotency_keys` (Quản lý Khóa Vô hiệu)
```sql
CREATE TABLE idempotency_keys (
    key VARCHAR(64) PRIMARY KEY,
    user_id BIGINT NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PROCESSING', -- 'PROCESSING', 'COMPLETED'
    response_code INT,
    response_body JSONB,
    locked_until TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

#### Bảng `outbox_events` (Transactional Outbox Events)
```sql
CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'PROCESSED'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_outbox_events_status ON outbox_events(status, id);
CREATE INDEX idx_outbox_events_created_at ON outbox_events(created_at);
```

#### Bảng `reconciliations` (Kết quả Đối soát)
```sql
CREATE TABLE reconciliations (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL,
    transaction_id VARCHAR(100) NOT NULL,
    discrepancy_type VARCHAR(30) NOT NULL, -- 'MATCHED', 'MISSING_IN_PARTNER', 'MISSING_IN_INTERNAL', 'AMOUNT_MISMATCH'
    internal_amount NUMERIC(18, 4),
    partner_amount NUMERIC(18, 4),
    status VARCHAR(20) NOT NULL DEFAULT 'UNRESOLVED', -- 'UNRESOLVED', 'AUTO_HEALED', 'MANUAL_REVIEW'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

---

## 4. Quy Trình Xử Lý Giao Dịch Tài Chính (Financial Processing Workflows)

### 4.1. Luồng Thanh Toán & Kiểm Tra Idempotency (End-to-End Payment Flow)

![Payment Flow Sequence Diagram](../diagrams/00_payment_flow_sequence.svg)

---

## 5. Tài Liệu Tham Khảo (Reputable References)

1. **Stripe Architecture**: [Designing Payment Systems & Financial Invariants](https://stripe.com/blog/engineering)
2. **Uber Engineering**: [Building Zero-Sum Double-Entry Ledger at Scale](https://www.uber.com/en-VN/blog/payment-processing/)
3. **Debezium Project**: [Transactional Outbox & Change Data Capture Guide](https://debezium.io/documentation/)
4. **Martin Kleppmann**: *Designing Data-Intensive Applications* (O'Reilly).
5. **System Design Interview Vol 2 (Alex Xu & Gergely Orosz)**: Chapter 4 — *Payment System Architecture*.

