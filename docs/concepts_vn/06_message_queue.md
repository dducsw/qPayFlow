# Bài 06: Event-Driven Architecture & Message Brokers (Apache Kafka & RabbitMQ)

> **Tóm tắt bài viết**: Phân tích vai trò của Event-Driven Architecture trong hệ thống thanh toán, so sánh **Apache Kafka** và **RabbitMQ**, các đảm bảo ngữ nghĩa giao thông điệp (Delivery Semantics: At-Least-Once, Exactly-Once), và cơ chế xử lý lỗi với Dead Letter Queue (DLQ).

---

## 1. Vai Trò Của Event-Driven Architecture Trong Thanh Toán

Trong kiến trúc Monolith truyền thống, khi một giao dịch thanh toán hoàn tất, service phải thực hiện một loạt các thao tác đồng bộ (Synchronous calls): gửi SMS, tích điểm thưởng, cập nhật báo cáo, bắn webhook cho Merchant. Nếu một trong các thao tác này bị chậm hoặc lỗi, toàn bộ luồng thanh toán bị nghẽn.

Chuyển sang **Event-Driven Architecture (EDA)**:

![Event Driven Architecture](../diagrams/06_1_event_driven_architecture.svg)

- **Decoupling (Tách biệt hoàn toàn)**: Payment Executor chỉ cần ghi Event `PaymentCompleted` vào Message Broker và trả về phản hồi lập tức cho khách hàng.
- **Resilience**: Nếu Notification Service bị crash, thông điệp vẫn nằm an toàn trên Queue. Khi service khôi phục, nó tự khôi phục việc đọc tiếp mà không làm mất thông điệp.

---

## 2. So Sánh Apache Kafka vs RabbitMQ

| Tiêu chí | Apache Kafka | RabbitMQ |
| :--- | :--- | :--- |
| **Kiến trúc Cốt lõi** | **Distributed Commit Log**. Message được ghi append-only vào đĩa cứng và lưu trữ lâu dài (Retention). | **Smart Broker / Dumb Consumer**. Message nằm trên RAM/Disk cho đến khi Consumer Ack thì xóa. |
| **Mô hình Đọc (Read Model)** | **Pull-based** (Consumer chủ động kéo dữ liệu theo Offset). | **Push-based** (Broker chủ động đẩy message cho Consumer). |
| **Thứ tự Thông điệp (Ordering)** | Đảm bảo thứ tự tuyệt đối **trên cùng một Partition**. | Đảm bảo thứ tự theo từng Queue đơn lẻ. |
| **Khả năng Scale & Throughput** | Cực cao (Hàng triệu msgs/sec nhờ Sequential I/O và Zero-Copy). | Trung bình đến Cao (Hàng chục nghìn msgs/sec). |
| **Use Case trong Fintech** | Event Sourcing, Audit Logging, Real-time Stream Analytics, Outbox Events. | Task Queue, Routing phức tạp (Topic/Direct Exchanges), Transient Messages. |

---

## 3. Ngữ Nghĩa Giao Thông Điệp (Delivery Semantics)

Trong thanh toán, việc xử lý thông điệp liên quan trực tiếp đến tiền tệ:

![Delivery Semantics Classification](../diagrams/06_2_delivery_semantics.svg)

1. **At-Most-Once (Tối đa một lần)**:
   Consumer Commit Offset *trước* khi xử lý logic. Nếu Consumer crash giữa chừng $\rightarrow$ Thông điệp bị mất vĩnh viễn. ❌ **Cấm dùng cho thanh toán!**

2. **At-Least-Once (Ít nhất một lần)**:
   Consumer xử lý logic thành công rồi mới Commit Offset. Nếu Consumer crash trước khi Commit Offset $\rightarrow$ Thông điệp sẽ được gửi lại (Re-delivered).
   > **Quy tắc vàng trong Fintech**: Luôn sử dụng **At-Least-Once Delivery** kết hợp với **Idempotent Consumer** (xem lại Bài 03) tại phía xử lý để triệt tiêu rủi ro trùng lặp.

3. **Exactly-Once Processing (Chính xác một lần)**:
   Đạt được nhờ Kafka Transactional API (`InitTransactions`, `SendOffsetsToTransaction`) phối hợp giữa Producer, Broker và DB Consumer trong cùng một giao dịch nguyên tố (Atomic Transaction).

---

## 4. Message Partitioning & Sắp Xếp Thứ Tự (Ordering Guarantee)

Kafka đảm bảo thứ tự thông điệp **duy nhất trong cùng một Partition**. Nếu hai sự kiện của cùng một tài khoản nằm ở 2 Partition khác nhau, chúng có thể bị đọc ngược thứ tự!

```go
// Quyết định Partition Key khi Produce Event sang Kafka
message := &kafka.Message{
    Topic: "wallet-events",
    // 🔑 BẮT BUỘC dùng AccountID làm Partition Key!
    // Giúp mọi biến động của Account 42 luôn rơi vào CÙNG 1 Partition!
    Key:   []byte("account_42"),
    Value: payloadJSON,
}
```

---

## 5. Xử Lý Lỗi & Dead Letter Queue (DLQ) Pattern

Khi Consumer gặp một message bị lỗi không thể xử lý (ví dụ: Payload bị hỏng, lỗi Schema, hoặc DB bị lock vĩnh viễn), Consumer **không nên bị kẹt mãi mãi (Block Queue)**.

![Dead Letter Queue Flow](../diagrams/06_3_dlq_flow.svg)

### Quy trình DLQ Standard:
1. Thử lại (Retry) với **Exponential Backoff** 3 lần trên `payment-events-retry` topic.
2. Nếu vẫn thất bại sau 3 lần retry, đẩy message sang **Dead Letter Queue (`payment-events-dlq`)**.
3. Bắn cảnh báo (Alert) tới PagerDuty/Slack cho đội Ops.
4. Đội kỹ thuật kiểm tra và dùng tool để **Reprocess** lại thông điệp từ DLQ sau khi đã fix bug.

---

## 6. Tài Liệu Tham Khảo (Reputable References)

1. **Confluent**: [Apache Kafka Architecture & Design Fundamentals](https://docs.confluent.io/platform/current/kafka/architecture.html)
2. **Martin Kleppmann**: [Turning the Database Inside Out with Event Sourcing & Kafka](https://www.confluent.io/blog/turning-the-database-inside-out-with-apache-kafka/)
3. **Uber Engineering**: [Building Reliable Event-Driven Systems with Kafka](https://www.uber.com/en-VN/blog/reliable-kafka-event-driven-architecture/)
4. **RabbitMQ Documentation**: [Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms)
5. **Kafkajs / Sarama**: [Kafka Exactly-Once Semantics Deep Dive](https://kafka.apache.org/documentation/#design_exactlyaspending)

