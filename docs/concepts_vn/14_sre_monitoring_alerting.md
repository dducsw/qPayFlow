# Bài 14: SRE Principles Trong Fintech (SLI/SLO, Error Budgets & 4 Golden Signals)

> **Tóm tắt bài viết**: Phân tích việc ứng dụng các nguyên lý Kỹ thuật Tin cậy Hệ thống (**Site Reliability Engineering - SRE**) trong nền tảng thanh toán tài chính, thiết lập ma trận **SLI / SLO / SLA**, quản trị ngân sách lỗi với thuật toán **Multi-Window Multi-Burn-Rate Alerting**, giám sát **4 Golden Signals** trên Prometheus/Grafana, và kỹ thuật loại bỏ hiện tượng **Alert Fatigue**.

---

## 1. Bộ Ba Khái Niệm Nền Tảng: SLI, SLO & SLA Trong Ngân Hàng

Trong ngành kỹ thuật tài chính, "hệ thống chạy ổn định" là một khái niệm mơ hồ. SRE định lượng độ tin cậy thông qua 3 thước đo chính xác:

| Khái Niệm | Bản Chất Kỹ Thuật | Ví Dụ Cụ Thể Trong qPayFlow |
| :--- | :--- | :--- |
| **SLI (Service Level Indicator)** | Tỷ lệ phần trăm thực tế đo lường được theo thời gian thực | Tỷ lệ request thanh toán trả về HTTP 200/201 và latency $p99 < 300\text{ms}$ trong 30 ngày qua là $99.96\%$. |
| **SLO (Service Level Objective)** | Mục tiêu cam kết nội bộ giữa đội Product & SRE | Tỷ lệ thành công phải đạt $\ge 99.95\%$ trên chu kỳ rolling window 30 ngày. |
| **SLA (Service Level Agreement)** | Cam kết pháp lý với khách hàng, có chế tài phạt tiền | Nếu Availability $< 99.9\%$, qPayFlow đền bù $10\%$ phí dịch vụ hàng tháng cho đối tác. |

> **Quy tắc vàng**: Luôn thiết lập $\text{SLO} > \text{SLA}$ (Ví dụ: $\text{SLO} = 99.95\%$, $\text{SLA} = 99.90\%$). Khoảng cách chênh lệch này là "vùng đệm an toàn" giúp đội ngũ kỹ thuật phát hiện và khắc phục sự cố trước khi bị phạt vi phạm hợp đồng SLA.

---

## 2. Quản Lý Ngân Sách Lỗi (Error Budget & Multi-Burn-Rate Alerting)

### 2.1. Khái niệm Error Budget
Ngân sách lỗi là tỷ lệ thời gian tối đa hệ thống được phép không hoàn hảo trong một chu kỳ:

$$\text{Error Budget} = 100\% - \text{SLO}$$

- Với $\text{SLO} = 99.95\% \rightarrow \text{Error Budget} = 0.05\%$ (tương đương tối đa **$21.6$ phút downtime/tháng**).
- **Quy tắc quản trị sản phẩm**:
  - Khi Error Budget $> 0\%$: Đội phát triển được phép tự do triển khai (Deploy) các tính năng mới và thử nghiệm rủi ro.
  - Khi Error Budget bị cạn kiệt ($0\%$): Đóng băng mọi hoạt động deploy tính năng mới (Feature Freeze). 100% nhân lực tập trung vào sửa lỗi, viết test và nâng cấp hạ tầng tin cậy.

### 2.2. Thuật toán Multi-Window Multi-Burn-Rate Alerting
Cách cảnh báo truyền thống (bắn alert khi CPU $> 80\%$ hoặc có lỗi trong 5 phút) tạo ra vô số cảnh báo rác. SRE hiện đại sử dụng **Burn Rate** (tốc độ tiêu hao ngân sách lỗi):

$$\text{Burn Rate} = \frac{\text{Tỷ lệ lỗi thực tế}}{1 - \text{SLO}}$$

| Hệ Số Burn Rate | % Ngân Sách Lỗi Tiêu Hao | Cửa Sổ Đo Lường (Long & Short) | Mức Độ Cảnh Báo & Hành Động (Action) |
| :---: | :--- | :--- | :--- |
| **14.4x** | 2% ngân sách trong 1 giờ | 1 Giờ & 5 Phút | 🚨 **P1 Page (Gọi On-Call đánh thức kỹ sư ngay)** |
| **6x** | 5% ngân sách trong 6 giờ | 6 Giờ & 30 Phút | 🚨 **P1 Page (Gọi On-Call)** |
| **1x** | 10% ngân sách trong 3 ngày | 3 Ngày & 6 Giờ | ⚠️ **P2 Ticket (Tạo Jira task gửi kênh Slack)** |

![SRE Monitoring & Alerting Pipeline](../diagrams/14_1_sre_monitoring.svg)

---

## 3. Cấu Hình Prometheus PromQL Alert Chuẩn SRE Cho qPayFlow

```yaml
groups:
- name: qpayflow-financial-slo-alerts
  rules:
  # Cảnh báo khẩn cấp: Tiêu hao 2% ngân sách lỗi trong 1 giờ (Burn Rate 14.4x)
  - alert: PaymentGateway_ErrorBudget_FastBurn
    expr: |
      (
        sum(rate(http_requests_total{job="payment-service", status=~"5.."}[1h])) 
        / 
        sum(rate(http_requests_total{job="payment-service"}[1h]))
      ) > (1 - 0.9995) * 14.4
      AND
      (
        sum(rate(http_requests_total{job="payment-service", status=~"5.."}[5m])) 
        / 
        sum(rate(http_requests_total{job="payment-service"}[5m]))
      ) > (1 - 0.9995) * 14.4
    for: 2m
    labels:
      severity: page
      tier: core-banking
    annotations:
      summary: "CRITICAL: Payment Service Error Budget is burning 14.4x faster than normal!"
      description: "Over 2% of the monthly error budget consumed in the last 1 hour. Immediate on-call intervention required."
```

---

## 4. Góc Chuyên Sâu: Phân Tích Kỹ Thuật & Tình Huống Thực Tế

### Case 1: Định nghĩa SLI thanh toán chuẩn xác (Authorization Success Rate vs Synthetic Ping)

**Bối cảnh:** Nhóm vận hành đo Availability bằng cách gửi request ping HTTP `/healthz` mỗi 5 giây vào Gateway. Báo cáo cuối tháng cho thấy Availability đạt $99.99\%$, nhưng khách hàng lại liên tục phàn nàn vì không thanh toán được tiền.

**Sai lầm tai hại:**
- Endpoint `/healthz` chỉ kiểm tra xem tiến trình Go còn sống hay không. Nó không thể phát hiện lỗi cạn kiệt Connection Pool của Database hay lỗi ngắt kết nối với cổng Ngân hàng đối tác.

**Định nghĩa SLI tài chính chuẩn xác trong qPayFlow:**
$$\text{SLI}_{\text{Availability}} = \frac{\sum \text{Successful Financial Authorizations (HTTP 200 \& Card Approved)}}{\sum \text{Valid User Payment Requests (Loại trừ lỗi nhập sai OTP/CVV của User)}} \times 100\%$$

$$\text{SLI}_{\text{Latency}} = \frac{\sum \text{Financial Requests responded in } < 300\text{ms}}{\sum \text{Total Financial Requests}} \times 100\%$$

Chỉ đo lường trên các giao dịch thực của người dùng cuối mới phản ánh trung thực chất lượng dịch vụ kinh doanh.

---

### Case 2: Kỹ thuật triệt tiêu hiện tượng "Alert Fatigue" (Chai lỳ cảnh báo)

**Bối cảnh:** Kênh Slack `#alerts-prod` nhận trung bình 300 thông báo mỗi ngày. Các kỹ sư bật chế độ Mute thông báo và bỏ lỡ sự cố sập database vào đêm thứ Bảy.

**4 quy tắc tái cấu trúc cảnh báo chuẩn Google SRE:**
1. **Chuyển từ Cause-based sang Symptom-based Alerting**: Không bắn alert khi *"CPU Node $> 85\%$"*. Chỉ bắn alert khi *"Người dùng đang nhận mã lỗi HTTP 500 hoặc $p99$ Latency bị tụt dốc"*.
2. **Quy tắc Actionable Alert**: Mọi cảnh báo gửi tới On-call Engineer bắt buộc phải có tài liệu hướng dẫn khắc phục nhanh (**Runbook URL**) đính kèm. Nếu một alert phát ra mà kỹ sư không cần làm gì $\rightarrow$ Lập tức xóa bỏ alert đó!
3. **Phân luồng nghiêm ngặt**:
   - **Page (Gọi điện / PagerDuty)**: Chỉ dành cho các sự cố đang làm cháy ngân sách lỗi nhanh (Cần xử lý trong $< 15$ phút).
   - **Email / Jira Ticket**: Dành cho các lỗi nhỏ, tiêu hao ngân sách chậm, có thể xử lý vào giờ hành chính ngày hôm sau.

---

### Case 3: Quy trình Phân tích Sự cố không đổ lỗi (Blameless Post-Mortem)

**Bối cảnh:** Một kỹ sư vô tình cấu hình sai biến môi trường khiến toàn bộ giao dịch qua thẻ Visa bị từ chối trong 45 phút.

**Văn hóa SRE Blameless:**
- Thay vì chỉ trích cá nhân: *"Tại sao anh A lại cấu hình ẩu?"*.
- Đặt câu hỏi về mặt hệ thống: *"Tại sao quy trình CI/CD cho phép một cấu hình sai sót vượt qua môi trường Staging vào Production mà không có khâu Validation tự động?"*.
- **Kết quả đầu ra của Post-Mortem**:
  1. Thêm bước **Schema Validation** tự động trong Helm Chart/K8s trước khi deploy.
  2. Bổ sung **Automated Smoke Test** giả lập giao dịch Visa ngay sau khi Pod mới khởi động.
  3. Cải thiện tài liệu Runbook để thời gian phát hiện và rollback giảm từ 45 phút xuống dưới 2 phút.

---

## 5. Tài Liệu Tham Khảo (Reputable References)

1. **Betsy Beyer, Chris Jones, Niall Richard Murphy**: *Site Reliability Engineering: How Google Runs Production Systems (Google SRE Book)* — Chapter 4: Service Level Objectives & Chapter 6: Monitoring Distributed Systems.
2. **Prometheus Official Guides**: [Alerting Based on SLOs and Multi-Window Burn Rates](https://prometheus.io/docs/practices/alerting/)
3. **Google Cloud Architecture**: [SRE Workbook: Implementing Service Level Objectives](https://sre.google/workbook/table-of-contents/)
4. **Grafana Labs**: [The 4 Golden Signals Dashboard Best Practices](https://grafana.com/docs/grafana/latest/dashboards/)
5. **Datadog Engineering**: [Monitoring and Alerting on Error Budgets at Scale](https://www.datadoghq.com/blog/sre-metrics-slo-sli-error-budget/)
