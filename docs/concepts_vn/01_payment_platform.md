# Bài 01: Kiến Trúc Nền Tảng Thanh Toán (Payment Platform Architecture)

> **Tóm tắt bài viết**: Phân tích kiến trúc tổng quan của một hệ thống thanh toán (Payment Platform), các thành phần cốt lõi (Payment Gateway, Processor, Core Ledger), quy trình xử lý luồng giao dịch tài chính (Payment Lifecycle), và nguyên tắc Hạch toán Sổ kép (Double-Entry Bookkeeping).

---

## 1. Vì Sao Hệ Thống Thanh Toán Lại Cực Kỳ Phức Tạp?

Hệ thống thanh toán nhìn bề ngoài có vẻ đơn giản: *"Người dùng nhấn nút Chuyển tiền, và số dư thay đổi"*. Tuy nhiên, ẩn sau đó là các yêu cầu khắt khe về mặt kiến trúc:

1. **Reliability (Độ tin cậy tuyệt đối)**: Giao dịch phải thành công hoặc thất bại một cách rõ ràng. Không bao giờ được ở trạng thái lấp lửng (mất tiền nhưng chưa nhận được hàng).
2. **Strict Consistency (Tính nhất quán nghiêm ngặt)**: Bản ghi tài chính phải chính xác 100%. Không chấp nhận Eventual Consistency cho số dư ví/tài khoản.
3. **High Availability & Scalability**: Xử lý hàng nghìn đến hàng chục nghìn giao dịch/giây (TPS) trong các đợt Flash Sale hoặc Tết.
4. **Security & Fraud Detection**: Phát hiện gian lận real-time, mã hóa thông tin thẻ (PCI DSS Compliance, Tokenization).
5. **Reconciliation & Compliance**: Đối soát dữ liệu đa bên với các Ngân hàng đối tác, Thẻ quốc tế (Visa/Mastercard) và tuân thủ quy định Ngân hàng Nhà nước.

---

## 2. Các Thành Phần Cốt Lõi (Core Components)

Một hệ thống thanh toán tiêu chuẩn (như Stripe, Adyen hay qPayFlow) bao gồm 5 khối chức năng chính:

![Payment Platform Core Components](../diagrams/01_1_payment_components.svg)

### Chi tiết vai trò:
- **Payment Gateway**: Điểm tiếp nhận request từ Client, thực hiện Authentication, Rate Limiting, TLS Encryption và Idempotency Key validation.
- **Risk & Fraud Engine**: Kiểm tra dấu hiệu gian lận (Device Fingerprint, IP Risk, Velocity Rules) trước khi cho phép giao dịch.
- **Payment Executor / Orchestrator**: Quản lý State Machine của giao dịch (`PENDING` $\rightarrow$ `PROCESSING` $\rightarrow$ `SUCCESS` / `FAILED`).
- **Processor Connector**: Tích hợp với các kênh thanh toán bên ngoài (Acquiring Bank, Card Networks VISA/Mastercard, Ví MoMo/ZaloPay, NAPAS).
- **Core Ledger Service**: Nơi lưu giữ nguồn sự thật (Source of Truth) về tài khoản, số dư và mọi biến động nợ/có.

---

## 3. Vòng Đời Giao Dịch Thanh Toán (Payment Lifecycle)

Trong thanh toán thẻ và ví điện tử, luồng giao dịch tiêu chuẩn trải qua 4 bước:

![Payment Lifecycle](../diagrams/01_2_payment_lifecycle.svg)

1. **Authorization (Ủy quyền)**: Kiểm tra thẻ/tài khoản có đủ tiền không và phong tỏa (Hold) số tiền giao dịch.
2. **Capture (Thâu thu)**: Xác nhận rút số tiền đã phong tỏa chuyển cho Merchant.
3. **Clearing (Bù trừ)**: Các bên trao đổi thông tin chi tiết giao dịch để tính toán nghĩa vụ tài chính cuối ngày.
4. **Settlement (Quyết toán)**: Tiền thực tế được chuyển từ Ngân hàng phát hành (Issuing Bank) sang Ngân hàng thanh toán (Acquiring Bank).

---

## 4. Nguyên Tắc Hạch Toán Sổ Kép (Double-Entry Bookkeeping)

Trong kỹ thuật ngân hàng, **không bao giờ được dùng câu lệnh `UPDATE balance = balance - 100` đơn thuần**. Mọi biến động tiền tệ phải được ghi lại dưới dạng bản ghi bất biến (Immutable Ledger Entries) tuân theo **Nguyên tắc Sổ kép (Double-Entry)**.

### Phương trình Kế toán Cơ bản:
$$\text{Assets (Tài sản)} = \text{Liabilities (Nợ phải trả)} + \text{Equity (Vốn chủ sở hữu)}$$

### Quy tắc Ghi Sổ:
- Mọi giao dịch phải gồm ít nhất 2 dòng: **Một dòng DEBIT (Nợ)** và **một dòng CREDIT (Có)**.
- Tổng giá trị $\sum \text{DEBIT}$ phải luôn **bằng tuyệt đối** tổng giá trị $\sum \text{CREDIT}$.

#### Ví dụ: User A chuyển 50 USD cho User B trong ví qPayFlow:

| Account ID | Entry Type | Amount (USD) | Description |
| :--- | :--- | :--- | :--- |
| `ACC_USER_A` | **DEBIT (Nợ)** | 50.00 | Giảm tài sản / tiền ví User A |
| `ACC_USER_B` | **CREDIT (Có)** | 50.00 | Tăng tài sản / tiền ví User B |

> **Lợi ích**: Nếu có sự cố văng lỗi DB hay tranh chấp tài chính, chỉ cần tính `SUM(DEBIT) - SUM(CREDIT)` của toàn bộ Ledger để phát hiện ngay lập tức sai lệch dòng tiền.

---

## 5. Tài Liệu Tham Khảo (Reputable References)

1. **Stripe Architecture**: [The Evolution of a Payment System - Stripe Resources](https://stripe.com/en-gb/resources/architecture/the-evolution-of-a-payment-system)
2. **Pragmatic Engineer (Gergely Orosz)**: [Designing a Payment System](https://newsletter.pragmaticengineer.com/p/designing-a-payment-system)
3. **RedHat Architecture**: [Integrating a Modern Payments Architecture](https://www.redhat.com/architect/portfolio/detail/12-integrating-a-modern-payments-architecture)
4. **System Design Handbook**: [Design a Payment System Guide](https://www.systemdesignhandbook.com/guides/design-a-payment-system/)
5. **Grammarly Engineering**: [Billing and Payments Platform Architecture](https://www.grammarly.com/blog/engineering/billing-and-payments-platform/)

