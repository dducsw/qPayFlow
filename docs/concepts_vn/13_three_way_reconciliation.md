# Bài 13: Thuật Toán Đối Soát 3 Chiều (Three-Way Reconciliation Engine)

> **Tóm tắt bài viết**: Phân tích chi tiết thuật toán đối soát 3 chiều (**Three-Way Reconciliation**) giữa Sổ cái nội bộ (Internal Ledger), Cổng thanh toán đối tác (Stripe / Bank EOD File) và Tài khoản Merchant, các mẫu sai lệch tài chính và quy trình tự động xử lý đền bù (Discrepancy Resolution).

---

## 1. Khái Niệm Đối Soát 3 Chiều (Three-Way Reconciliation)

Trong các giao dịch tài chính ngân hàng, một giao dịch thành công phải được xác nhận nhất quán từ **3 nguồn dữ liệu độc lập**:

1. **Internal Core Ledger (Sổ cái qPayFlow)**: Ghi nhận biến động số dư tài khoản người dùng và nhật ký giao dịch nội bộ.
2. **Partner Gateway Statement (File EOD Ngân hàng / Stripe)**: File đối soát định kỳ (CSV / MT940 / CAMT.053) gửi từ Ngân hàng lúc 00:30 sáng chứa danh sách các giao dịch thực tế đã qua cổng.
3. **Merchant Account Settlement (Sổ kế toán Merchant)**: Ghi nhận nghĩa vụ quyết toán và số tiền Merchant thực nhận sau khi trừ phí giao dịch.

---

## 2. Kiến Trúc Thuật Toán Đối Soát

![Three-Way Reconciliation Matching Engine](../diagrams/13_1_three_way_reconciliation.svg)

### Các bước thực thi thuật toán:

```
Step 1: Batch Ingestion -> Tải file EOD từ Ngân hàng qua SFTP & Đọc dữ liệu Ledger DB
Step 2: Key Hash Matching -> Map các bản ghi theo (TransactionID, ReferenceNo, Amount, Currency)
Step 3: Discrepancy Classification -> Phân loại 4 trạng thái đối soát
Step 4: Auto-Healing / Manual Review -> Trigger giao dịch bù trừ hoặc tạo ticket tra soát
```

---

## 3. Các Trạng Thái Phân Loại Sai Lệch (Discrepancy Matrix)

| Trạng thái | Nội bộ qPayFlow | Partner Bank File | Merchant Ledger | Hành động xử lý (Resolution) |
| :--- | :---: | :---: | :---: | :--- |
| **MATCHED** | SUCCESS (100$) | SUCCESS (100$) | SUCCESS (100$) | ✅ Chấp nhận quyết toán (Auto-Approve). |
| **MISSING_IN_PARTNER** | SUCCESS (100$) | ❌ KHÔNG CÓ | PENDING | ⚠️ Đơn hàng nghi vấn. Gửi ticket tra soát sang Ngân hàng. |
| **MISSING_IN_INTERNAL** | ❌ KHÔNG CÓ | SUCCESS (100$) | ❌ KHÔNG CÓ | ⚠️ Khách bị trừ tiền ở Bank nhưng qPayFlow bị lỗi network. Trigger **Auto-Credit Compensation** cho khách. |
| **AMOUNT_MISMATCH** | SUCCESS (100$) | SUCCESS (90$) | SUCCESS (100$) | ❌ Khác biệt số tiền (Lỗi tỷ giá/phí). Chuyển sang đội Risk xử lý thủ công. |

---

## 4. Pseudocode Thuật Toán Matching 3 Chiều

```go
// Go Pseudocode: Core 3-Way Matching Engine
func ReconcileThreeWay(internalTx Map[string]Tx, partnerTx Map[string]Tx, merchantTx Map[string]Tx) []Discrepancy {
    var discrepancies []Discrepancy

    for txID, intTx := range internalTx {
        partTx, inPartner := partnerTx[txID]
        merchTx, inMerchant := merchantTx[txID]

        if !inPartner {
            discrepancies = append(discrepancies, Discrepancy{Type: "MISSING_IN_PARTNER", TxID: txID})
            continue
        }

        if intTx.Amount != partTx.Amount || intTx.Amount != merchTx.Amount {
            discrepancies = append(discrepancies, Discrepancy{Type: "AMOUNT_MISMATCH", TxID: txID})
            continue
        }

        // Matched successfully
        MarkAsReconciled(txID)
    }

    return discrepancies
}
```

---

## 5. Tài Liệu Tham Khảo (Reputable References)

1. **Stripe Documentation**: [Reconciliation and Settlement Reports Guide](https://stripe.com/docs/reports/reconciliation)
2. **Pragmatic Engineer (Gergely Orosz)**: [Designing Financial Reconciliation Systems](https://newsletter.pragmaticengineer.com/)
3. **Adyen Docs**: [Financial Reconciliation & Payout Guides](https://docs.adyen.com/reporting/reconciliation)
4. **SWIFT Standard**: [MT940 & CAMT.053 Bank Statement Specifications](https://www.swift.com/)

