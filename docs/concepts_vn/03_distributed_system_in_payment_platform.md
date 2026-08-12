# Bài 03: Các Thách Thức Hệ Thống Phân Tán Trong Nền Tảng Thanh Toán

> **Tóm tắt bài viết**: Phân tích chuyên sâu 4 thiết kế mẫu (Design Patterns) bắt buộc phải có để giải quyết rủi ro mất tiền, double-spending, race condition và rớt mạng trong hệ thống thanh toán phân tán: **Idempotency Key**, **Distributed Lock / Locking Mechanics**, **Transactional Outbox Pattern**, và **Reconciliation Engine**.

---

## 1. Mối Rủi Ro "Trừ Tiền 2 Lần" (Double Charging) & Timeout Dilemma

Trong môi trường phân tán, khi Client gửi một lệnh thanh toán 100$ và bị **Network Timeout**, Client đứng trước bài toán tiến thoái lưỡng nan:
- Nếu **gửi lại (Retry)**: Rủi ro tài khoản bị trừ tiền 2 lần (200$) nếu request đầu tiên thực chất đã thành công tại Server nhưng response bị rớt trên đường về.
- Nếu **không gửi lại**: Rủi ro đơn hàng không được giao dù tiền đã bị trừ.

![Idempotency Key Pattern Flow](../diagrams/03_1_idempotency_flow.svg)

---

## 2. Pattern 1: Idempotency Key Pattern (Xử Lý Trùng Lặp Giao Dịch)

**Idempotency (Tính vô hiệu):** Là khả năng thực thi một hành động nhiều lần nhưng kết quả cuối cùng không thay đổi so với thực thi đúng một lần.

### Cơ chế hoạt động:
1. Client tạo một chuỗi ngẫu nhiên duy nhất (UUID v4) truyền vào Header: `Idempotency-Key: 9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d`.
2. Server tạo một bảng `idempotency_keys` trong DB hoặc Redis với Constraint `UNIQUE(idempotency_key)`:

```sql
CREATE TABLE idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    user_id BIGINT NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'PROCESSING', 'COMPLETED'
    response_code INT,
    response_body JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    locked_until TIMESTAMP WITH TIME ZONE
);
```

3. **Luồng xử lý tại Server**:
   - Giai đoạn 1: Thử Insert `(key, status='PROCESSING')`.
   - Nếu vi phạm UNIQUE constraint:
     - Nếu `status == 'PROCESSING'`: Trả về lỗi `409 Conflict` (Giao dịch đang được xử lý song song).
     - Nếu `status == 'COMPLETED'`: Trả về ngay `response_body` đã lưu từ trước **mà không chạy lại logic trừ tiền**.
   - Giai đoạn 2: Thực thi logic tài chính trong DB Transaction.
   - Giai đoạn 3: Cập nhật `status = 'COMPLETED'` và lưu `response_body`.

---

## 3. Pattern 2: Distributed Lock & Concurrency Control (Chống Double-Spending)

Khi hai giao dịch xảy ra **đồng thời (Concurrent)** trên cùng một tài khoản (ví dụ: User vừa quét mã QR thanh toán vừa rút tiền tại ATM):

### Các phương pháp xử lý Khóa (Locking):

![Concurrency Control Strategies](../diagrams/03_2_concurrency_control.svg)

1. **Optimistic Locking (Khóa Lạc quan)**:
   Sử dụng cột `version` trong SQL.
   ```sql
   UPDATE accounts 
   SET balance = balance - 100, version = version + 1 
   WHERE id = 42 AND version = 5;
   ```
   Nếu `Rows Affected == 0`, giao dịch bị xung đột $\rightarrow$ Retry hoặc Abort.

2. **Pessimistic Locking (Khóa Bi quan)**:
   Khóa dòng dữ liệu trực tiếp trong SQL Transaction bằng `FOR UPDATE`.
   ```sql
   BEGIN;
   SELECT balance FROM accounts WHERE id = 42 FOR UPDATE;
   -- Xử lý trừ tiền trên RAM
   UPDATE accounts SET balance = balance - 100 WHERE id = 42;
   COMMIT;
   ```

3. **Distributed Lock (Redis / Redlock)**:
   Sử dụng lệnh Redis `SET lock:account:42 my_random_token NX PX 5000` để đảm bảo chỉ 1 Node Microservice được quyền ghi vào tài khoản tại một thời điểm.

---

## 4. Pattern 3: Transactional Outbox Pattern

Một lỗi phổ biến là viết code vừa update DB vừa publish Event ra Kafka trong cùng một hàm:

```go
// ❌ CODE LỖI NGUY HẠI TÀI CHÍNH
func ProcessPayment(tx *sql.Tx, payment Payment) error {
    // Step 1: Update Balance
    if err := updateBalanceInDB(tx, payment); err != nil {
        return err
    }
    tx.Commit() // DB đã commit tiền!

    // Step 2: Publish Event to Kafka
    // ⚠️ Nếu Server bị crash hoặc rớt mạng ở ĐÂY -> Kafka KHÔNG nhận được event!
    // Hệ thống bị mất đồng bộ giữa Ledger và Notification/Recon!
    return kafkaProducer.Send("payment-events", payment)
}
```

### Giải pháp: Transactional Outbox Pattern

![Transactional Outbox Pattern](../diagrams/03_3_transactional_outbox.svg)

Mọi Event cần bắn ra Kafka sẽ được **Insert trực tiếp vào bảng `outbox_messages` trong CÙNG DB Transaction** với lệnh trừ tiền. Một tiến trình độc lập (CDC Debezium hoặc Outbox Poller) sẽ đọc bảng `outbox_messages` để push sang Kafka với cơ chế **At-Least-Once Delivery**.

---

## 5. Pattern 4: Reconciliation Engine & EOD Settlement (Đối Soát Cuối Ngày)

Dù hệ thống có thiết kế chặt chẽ đến đâu, sai lệch dữ liệu vẫn có thể phát sinh do lỗi phía Ngân hàng đối tác. Do đó, **Reconciliation (Đối soát)** là hàng rào bảo vệ cuối cùng.

1. **Thu thập File EOD (End-Of-Day)**: Nhận file giao dịch (CSV/MT940/SFTP) từ Ngân hàng lúc 00:30 sáng.
2. **Matching Engine**: So sánh 3 chiều (3-Way Matching):
   - Bản ghi tại **qPayFlow Gateway**.
   - Bản ghi tại **Core Ledger**.
   - Bản ghi tại **File Ngân hàng**.
3. **Phân loại Sai lệch (Discrepancy Resolution)**:
   - *Missing at Bank*: Giao dịch thành công ở qPayFlow nhưng Ngân hàng không có $\rightarrow$ Đặt trạng thái nghi vấn, gửi ticket tra soát.
   - *Missing at qPayFlow*: Tiền đã trừ ở Ngân hàng nhưng qPayFlow chưa ghi nhận $\rightarrow$ Trigger Compensating Credit (Nạp tiền bổ sung cho khách hàng).

---

## 6. Tài Liệu Tham Khảo (Reputable References)

1. **Stripe Engineering**: [How We Build Idempotent APIs for Payment Processing](https://stripe.com/blog/idempotency)
2. **MemoBank**: [Choosing an Architecture for Core Banking Systems](https://medium.com/memobank/choosing-an-architecture-85750e1e5a03)
3. **Uber Engineering**: [Reliable Processing of Financial Transactions at Scale](https://www.uber.com/en-VN/blog/payment-processing/)
4. **Microservices.io**: [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
5. **ByteByteGo**: [Payment System Design & Idempotency Key Guide](https://bytebytego.com/)
