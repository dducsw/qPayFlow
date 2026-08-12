# Bài 09: Event Sourcing & CQRS Trong Hệ Thống Thanh Toán (Command Query Responsibility Segregation)

> **Tóm tắt bài viết**: Phân tích hai mẫu kiến trúc cao cấp **Event Sourcing** và **CQRS**, cách lưu trữ trạng thái dưới dạng các sự kiện bất biến (Append-Only Event Store), tái tạo trạng thái tài khoản (State Replay), và phân tách mô hình Đọc/Ghi để đạt hiệu năng tối ưu.

---

## 1. Khái Niệm Event Sourcing & CQRS

Trong kiến trúc CRUD truyền thống, ta lưu trữ trạng thái hiện tại (Current State) của tài khoản: `Account { id: 42, balance: 100 }`. Khi có giao dịch mới, ta dùng câu lệnh `UPDATE` để đè lên giá trị cũ $\rightarrow$ **Mất đi lịch sử biến động chi tiết**.

- **Event Sourcing**: Không bao giờ `UPDATE` hay `DELETE` dữ liệu. Trạng thái ứng dụng được tính toán bằng cách replay lại toàn bộ chuỗi các **Sự kiện Bất biến (Immutable Events)** phát sinh từ quá khứ đến hiện tại:
  $$\text{Current Balance} = \sum_{i=1}^{N} \text{Event}_i$$
- **CQRS (Command Query Responsibility Segregation)**: Tách biệt hoàn toàn luồng **Ghi dữ liệu (Command Model)** và luồng **Đọc dữ liệu (Query Model)** sang các datastore chuyên biệt.

---

## 2. Kiến Trúc CQRS & Event Sourcing

![Event Sourcing & CQRS Architecture](../diagrams/09_1_event_sourcing_cqrs.svg)

### Nguyên lý hoạt động:
1. **Command Side (Ghi)**: Nhận lệnh chuyển tiền, kiểm tra Business Rules, và chỉ thực hiện lệnh `INSERT` sự kiện mới (`MoneyDeposited`, `MoneyWithdrawn`, `TransferCompleted`) vào **Event Store (PostgreSQL)**.
2. **Event Broker**: Đẩy Event sang Apache Kafka.
3. **Query Side (Đọc)**: Tiến trình **Event Projector Worker** tiêu thụ Event từ Kafka để tính toán trước (Materialize View) và lưu vào **Redis Cache** hoặc **Elasticsearch** cho luồng tra cứu số dư/lịch sử giao dịch nhanh chóng với $p99 < 5\text{ms}$.

---

## 3. Cơ Chế Snapshotting Trong Event Sourcing

Nếu một tài khoản đã thực hiện $1,000,000$ giao dịch, việc replay lại $1,000,000$ event mỗi khi cần kiểm tra số dư sẽ gây gián đoạn hiệu năng nghiêm trọng.

**Giải pháp: Snapshotting Pattern**
Định kỳ cứ sau $1,000$ event (hoặc cuối mỗi ngày), hệ thống tạo một bản chụp trạng thái **Snapshot**:

$$\text{Current State} = \text{Snapshot at Event \#1000} + \sum_{i=1001}^{1050} \text{Event}_i$$

```json
{
  "account_id": "ACC_42",
  "snapshot_version": 1000,
  "balance": 5250.00,
  "created_at": "2026-08-12T23:59:59Z"
}
```

---

## 4. Ưu & Nhược Điểm Khi Áp Dụng Trong Fintech

### Ưu điểm:
- **Audit Log Hoàn Hảo**: Theo vết 100% mọi sự thay đổi tài chính trong quá khứ mà không thể bị sửa đổi hay xóa bỏ.
- **Time Travel & Debugging**: Cho phép tái tạo lại chính xác trạng thái tài khoản tại bất kỳ thời điểm nào trong quá khứ để phục vụ công tác điều tra gian lận hay tra soát.
- **High Performance Read**: CQRS giúp luồng đọc không bị ảnh hưởng bởi luồng ghi.

### Nhược điểm:
- Độ phức tạp cao (Complexity Spike). Thách thức về **Eventual Consistency** giữa Read Side và Write Side.

---

## 5. Tài Liệu Tham Khảo (Reputable References)

1. **Martin Fowler**: [Event Sourcing Pattern & Architecture](https://martinfowler.com/eaaDev/EventSourcing.html)
2. **Greg Young**: [CQRS Documents & Event Store Design](https://cqrs.wordpress.com/)
3. **AWS Architecture Blog**: [Build Event-Driven Architectures with Event Sourcing and CQRS](https://aws.amazon.com/blogs/architecture/)
4. **Microsoft Learn**: [CQRS Pattern & Event Sourcing Pattern Guides](https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs)

