# Bài 18: Giao Tiếp gRPC Trong Kiến Trúc Microservices

> **Tóm tắt bài viết**: Phân tích lý do **gRPC (HTTP/2 + Protocol Buffers)** trở thành chuẩn mực giao tiếp đồng bộ nội bộ giữa các Microservices tài chính, so sánh hiệu năng với **REST JSON (HTTP/1.1)**, cơ chế truyền lan thời hạn thực thi (**gRPC Deadline & Context Cancellation**), và giải quyết bài toán mất cân bằng tải kết nối dài (**HTTP/2 Long-Lived Connection Load Balancing**).

---

## 1. Vấn Đề: Tại Sao REST JSON (HTTP/1.1) Quá Chậm Cho Giao Tiếp Nội Bộ?

Trong kiến trúc Microservices, một yêu cầu thanh toán từ người dùng có thể kích hoạt từ 5 đến 10 cuộc gọi đồng bộ nội bộ giữa các service. Nếu sử dụng chuẩn REST JSON truyền thống:

| Điểm Yếu Của REST JSON (HTTP/1.1) | Tác Động Tiêu Cực Đến Hệ Thống Microservices |
| :--- | :--- |
| **Chi phí kết nối & Handshake lặp lại** | Phải mở liên tục kết nối TCP/TLS mới hoặc bị tắc nghẽn Head-of-Line Blocking tại tầng HTTP. |
| **Payload cồng kềnh dạng Text** | JSON mang theo rất nhiều ký tự thừa (dấu ngoặc kép, key name lặp lại hàng triệu lần). |
| **Tiêu tốn CPU Serialize/Parse** | CPU máy chủ phải chuyển đổi liên tục giữa String Text và Data Structures trong RAM. |

---

## 2. Giải Pháp gRPC: Sức Mạnh Của HTTP/2 & Protocol Buffers

gRPC (Google Remote Procedure Call) kết hợp hai công nghệ tối tân để tối ưu hóa triệt để tốc độ truyền tải:

![gRPC Concurrency & Transport](../diagrams/04_1_concurrency_models.svg)

### 2.1. Nền tảng truyền tải HTTP/2
- **Binary Framing Layer**: Thay vì truyền văn bản ASCII thuần, HTTP/2 phân tách thông điệp thành các khung nhị phân (Binary Frames: `HEADERS`, `DATA`).
- **Multiplexing (Ghép kênh song song)**: Cho phép truyền hàng trăm request và response đồng thời trên **DUY NHẤT một kết nối TCP** mà không cần chờ đợi nhau.
- **HPACK Header Compression**: Nén bảng mã Header, loại bỏ 85% chi phí băng thông lặp lại của các metadata.

### 2.2. Định dạng Protocol Buffers (Protobuf)
- Dữ liệu được mã hóa thành các chuỗi nhị phân siêu nhỏ gọn dựa trên cơ chế **Varint** và **Field Tags**.
- Có Schema hợp đồng nghiêm ngặt (file `.proto`), cho phép tự sinh mã nguồn (Code Generation) với Type Safety tuyệt đối ở thời điểm biên dịch (Compile-time).

```protobuf
syntax = "proto3";

package qpayflow.account.v1;

option go_package = "github.com/qpayflow/pkg/api/account/v1;accountv1";

service AccountService {
  rpc GetAccountBalance (GetAccountBalanceRequest) returns (GetAccountBalanceResponse);
  rpc TransferFunds (TransferFundsRequest) returns (TransferFundsResponse);
}

message TransferFundsRequest {
  string idempotency_key = 1;
  string source_account_id = 2;
  string target_account_id = 3;
  int64 amount_cents = 4;
  string currency = 5;
}

message TransferFundsResponse {
  string transaction_id = 1;
  string status = 2;
  int64 new_source_balance_cents = 3;
  int64 created_at_unix = 4;
}
```

---

## 3. Các Chế Độ Truyền Tải Trong gRPC

| Chế Độ RPC | Mô Hình Tương Tác | Trường Hợp Ứng Dụng Trong qPayFlow |
| :--- | :--- | :--- |
| **1. Unary RPC** | 1 Request $\longrightarrow$ 1 Response | Truy vấn số dư tài khoản, tạo giao dịch thanh toán chuẩn |
| **2. Server Streaming RPC** | 1 Request $\longrightarrow$ Dòng $N$ Responses liên tục | Tải danh sách lịch sử giao dịch lớn, streaming file đối soát EOD |
| **3. Client Streaming RPC** | Dòng $N$ Requests $\longrightarrow$ 1 Response | Upload file sao kê ngân hàng từng phần (Chunked upload) |
| **4. Bidirectional Streaming RPC** | Dòng $N$ Requests $\longleftrightarrow$ Dòng $N$ Responses | Kênh giao tiếp thời gian thực, đồng bộ trạng thái socket song song |

---

## 4. Góc Chuyên Sâu: Phân Tích Kỹ Thuật & Tình Huống Thực Tế

### Case 1: Cái bẫy mất cân bằng tải (Load Imbalance Trap) của gRPC trên Kubernetes

**Bối cảnh sự cố:** Payment Service có 3 Pods gọi sang Account Service có 10 Pods qua Kubernetes ClusterIP Service. Dù lưu lượng giao dịch tăng gấp 10 lần, kỹ sư nhận thấy **chỉ có 3 Pods của Account Service gánh 100% tải (CPU 95%)**, trong khi 7 Pods còn lại hoàn toàn rảnh rỗi (CPU 2%)!

**Nguyên nhân gốc rễ:**
- Kubernetes Service mặc định hoạt động ở **Layer 4 (Transport Layer - TCP)**.
- Khi gRPC Client kết nối tới K8s Service, nó mở **1 kết nối TCP duy nhất dài hạn (Long-Lived HTTP/2 Connection)** tới 1 Pod ngẫu nhiên.
- Mọi request tiếp theo đều chạy Multiplexing qua đường ống TCP đã mở sẵn đó $\rightarrow$ Không bao giờ được phân bổ sang các Pods mới!

```
[Payment Pod 1] ═════ duy nhất 1 kết nối TCP ═════> [Account Pod 1 (CPU 100% QUÁ TẢI)]
[Payment Pod 2] ═════ duy nhất 1 kết nối TCP ═════> [Account Pod 2 (CPU 100% QUÁ TẢI)]
[Payment Pod 3] ═════ duy nhất 1 kết nối TCP ═════> [Account Pod 3 (CPU 100% QUÁ TẢI)]
                                                    [Account Pods 4..10 (CPU 0% RẢNH RỖI)]
```

**2 giải pháp giải quyết triệt để:**
1. **Client-Side Load Balancing (gRPC Headless Service)**:
   - Sử dụng K8s Headless Service (`clusterIP: None`) kết hợp DNS Resolver của gRPC trong Go (`grpc.Dial("dns:///account-service:50051", grpc.WithDefaultServiceConfig('{"loadBalancingConfig": [{"round_robin":{}}]}'))`).
   - Client tự resolve danh sách IP của cả 10 Pods và mở 10 kết nối TCP con để chia đều từng request qua thuật toán Round-Robin.
2. **L7 Proxy / Service Mesh (Envoy / Istio)**:
   - Đặt một Envoy Sidecar Proxy đứng trước để giải mã khung HTTP/2 và điều phối từng Request Frame (L7 Load Balancing) sang các Pods còn trống.

---

### Case 2: Truyền lan thời hạn thực thi (Deadline & Context Cancellation) chống lãng phí tài nguyên

**Bối cảnh:** Client trên Mobile App bấm Hủy giao dịch hoặc bị ngắt kết nối mạng sau 1 giây.

**Cơ chế bảo vệ bằng gRPC Deadline Propagation:**
- Tại API Gateway, ta thiết lập thời hạn tối đa:
  ```go
  ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
  defer cancel()
  ```
- Khi truyền `ctx` vào lệnh gọi gRPC, gRPC tự động chuyển đổi thành header `grpc-timeout: 2S`.
- **Nếu Client ngắt kết nối**: Go Context tự động phát tín hiệu `ctx.Done()`.
- Lệnh gọi gRPC lập tức truyền tín hiệu hủy này sang tất cả các Microservices hạ tầng (Payment, Account, Fraud).
- Các câu lệnh SQL trong Postgres đang chạy dở sẽ lập tức nhận lệnh `Query Cancelled by User`, giải phóng connection pool và CPU ngay lập tức trong $0.5\text{ms}$.

---

### Case 3: Quy tắc tiến hóa Schema Protobuf (Backward & Forward Compatibility)

**Bối cảnh:** Nâng cấp Service định nghĩa file `.proto` mà không làm sập các Service khác đang chạy phiên bản cũ.

**Quy tắc bất di bất dịch của Protocol Buffers:**
1. **KHÔNG BAO GIỜ THAY ĐỔI TAG NUMBER**: Con số sau dấu `=` (ví dụ `string account_id = 2;`) là định danh nhị phân duy nhất. Đổi số tag sẽ làm hỏng hoàn toàn dữ liệu!
2. **Khi xóa một trường, bắt buộc dùng từ khóa `reserved`**:
   ```protobuf
   message PaymentRequest {
     reserved 4, 8;
     reserved "old_cvv_field", "legacy_token";
   }
   ```
   Ngăn chặn các lập trình viên tương lai vô tình dùng lại Tag số 4 cho một trường mới, gây xung đột giải mã dữ liệu cũ.

---

## 5. Tài Liệu Tham Khảo (Reputable References)

1. **gRPC Authors**: [gRPC Core Concepts and Architecture](https://grpc.io/docs/what-is-grpc/core-concepts/)
2. **Google Protocol Buffers Guide**: [Language Guide (proto3) & Schema Best Practices](https://protobuf.dev/programming-guides/proto3/)
3. **Envoy Proxy Documentation**: [gRPC Bridge and HTTP/2 Multiplexing Load Balancing](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_protocols/grpc)
4. **Martin Kleppmann**: *Designing Data-Intensive Applications* — Chapter 4: Formats for Binary Encoding.
5. **Alan Shreve**: [gRPC Load Balancing on Kubernetes (Kubernetes Blog)](https://kubernetes.io/blog/2018/11/07/grpc-load-balancing-on-kubernetes-without-tears/)
