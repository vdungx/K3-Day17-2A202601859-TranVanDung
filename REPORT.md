# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Văn Dũng · **Mã sinh viên:** 2A202601859 · **Lớp:** K3 / AICB-P2T2 · **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output đầy đủ của ba lượt chạy cuối</summary>

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 25.5s
run 2/3 … 25.2s
run 3/3 … 25.1s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
──────────────────────────────────────────────────────────────────────────
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
──────────────────────────────────────────────────────────────────────────
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

KIỂM TRA KHÁC
──────────────────────────────────────────────────────────────────────────
dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
  số file parquet                           ✓ 5,000 → 14
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

</details>

Tổng kết: **4/4 tiêu chí chính đạt; cả hai bài mở rộng đạt**. Sau hai lượt chạy bổ sung (lượt 4 và 5), số hàng và checksum vẫn giữ nguyên: training `12.480 / 8dd7c98653`, feature `9.100 / 3db448685c`, chunks `31.200 / 92d8e50131`, quarantine `312 / ebb89036fb`.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Baseline sau ba lượt có 38.750 hàng thay vì 12.480; cả 12.480 ticket đều bị lặp. Clear/retry làm bảng tiếp tục tăng dù source Silver vẫn có đúng một hàng mỗi ticket. |
| **Nguyên nhân** | Model incremental không khai báo `unique_key` và strategy nên dbt ghi thêm kết quả bằng INSERT. Bảng có grain entity, trong khi cùng một `ticket_id` có thể xuất hiện ở nhiều ngày ingest do CDC `op='u'`; vì vậy retry hoặc backfill cùng partition tạo bản sao thay vì cập nhật entity cũ. |
| **Cách khắc phục** | Khai báo `unique_key='ticket_id'`, `incremental_strategy='merge'`; giữ filter `run_date`. Đặt DAG `catchup=False`, `max_active_runs=1` để tránh chạy bù ngoài ý muốn và ghi đồng thời. Hai tham số DAG giảm khả năng kích hoạt lỗi, không thay thế tính idempotent ở model. |
| **Bằng chứng** | Sau sửa: 12.480 hàng, không ticket lặp; checksum `8dd7c98653` giống nhau qua ba lượt verify và vẫn giữ nguyên ở lượt 4–5. DAG được đọc thành `False / 1`. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Baseline chỉ có 8.645 thay vì 9.100 cặp `(event_date, customer_id)`, thiếu 455 cặp và tập trung ở những ngày cũ. |
| **P99 độ trễ đo được** | **2,725833 ngày**. P50 = 0,128090 ngày; P95 = 1,813693 ngày; max = 2,944688 ngày; 5,0509% event tới muộn hơn một ngày. |
| **Lookback đã chọn** | **3 ngày**, bằng `ceil(P99)`. Window này bao phủ mức phục vụ P99 và, với tập seed hiện tại, cũng lớn hơn max thực đo. |
| **Nguyên nhân** | Filter cũ chỉ nhận `event_date > max(event_date)` của target. Watermark tiến theo event time, nên event xảy ra ở ngày cũ nhưng ingest sau khi watermark đã đi qua sẽ bị loại ở mọi lượt sau. Chỉ đổi `>` thành `>=` vẫn chỉ tính lại một ngày và không đủ cho độ trễ gần ba ngày. |
| **Cách khắc phục** | Tính lại các ngày từ `max(event_date) - interval 3 day`; khai báo khóa kép `['event_date','customer_id']` và strategy `delete+insert` để kết quả aggregate mới thay thế kết quả cũ thay vì cộng dồn. |
| **Bằng chứng** | Sau sửa: đủ 9.100 cặp; checksum `3db448685c` ổn định qua ba lượt và lượt 4–5; training vẫn đủ 12.480. |

Chọn P99 thay vì max giúp giới hạn lượng lịch sử phải quét lại trong mọi lần chạy. Max bảo vệ cả outlier nhưng một outlier cực đoan có thể làm chi phí thường trực tăng mạnh. Trong dữ liệu đo được, `ceil(P99)` tình cờ cũng bao phủ max; nếu phân bố production thay đổi cần theo dõi tỷ lệ late và điều chỉnh SLA/window.

---

## 3 · Kiểu dữ liệu `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Baseline có 6.606 priority sai/NULL và quarantine rỗng dù pipeline/test cũ vẫn pass. Source gồm 6.846 số hợp lệ, 7.142 nhãn chuỗi hợp lệ và 312 bản ghi lỗi thật. |
| **Nguyên nhân** | Backend đổi biểu diễn từ số sang nhãn nhưng ý nghĩa không đổi. `try_cast` biến toàn bộ nhãn hợp lệ thành NULL, đồng thời lại nhận `0`, `5`, `-1` vì chúng là số. Contract bị tắt và bộ test không kiểm tra miền 1–4 nên schema drift không làm pipeline báo lỗi. |
| **Ba nhóm giá trị** | `1..4`: giữ nguyên; `urgent/high/medium/low`: map về `1/2/3/4`; `P1`, `P2`, `unknown`, số ngoài miền, rỗng, NULL: trả NULL và đưa vào quarantine. |
| **Cách khắc phục** | Dùng một macro CASE chung cho Silver và quarantine. Lọc bản ghi lỗi **trước** `row_number`, sau đó mới chọn trạng thái hợp lệ mới nhất của ticket. Bật contract; thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `silver_tickets` vẫn đủ 12.480 ticket với phân bố priority `(1:3134, 2:3029, 3:3115, 4:3202)`; quarantine đúng 312 hàng gồm 78 thiếu/rỗng, 118 số ngoài miền, 116 nhãn lạ; dbt test **11/11 pass**. |

Bronze nên giữ payload thô để có dấu vết điều tra và khả năng replay. Việc chuẩn hóa/định tuyến lỗi thuộc Silver. Không nên chặn cả DAG vì 312 bản ghi hỏng: các event và chunk hợp lệ vẫn phải được phục vụ, còn quarantine tạo hàng đợi quan sát được để xử lý riêng.

---

## 4 · Bài mở rộng

### A — Dashboard và small-file problem

| | |
|---|---|
| **Nguyên nhân** | 5.000 file nhỏ không partition buộc engine mở toàn bộ file; `strftime(event_time, ...)` không sargable nên không tận dụng partition/min-max statistics. |
| **Cách khắc phục** | Compact thành 14 partition `event_date`, sort `customer_name, event_time`, row group 2.048; query dùng hive partitioning và predicate `event_date = DATE '2026-08-09'`. |
| **Bằng chứng** | 130.683 hàng không đổi; file `5.000 → 14`; rows scanned `5.000.000 → 9.324` (**536,3×**); result hash giữ nguyên `4379e4c5d9f3`. |

### B — Consumer bị kill giữa batch

| | |
|---|---|
| **Nguyên nhân** | Commit offset trước khi write tạo at-most-once: crash làm mất batch đã được đánh dấu xử lý. Ngoài ra write phải được commit bền vững trước offset; chỉ đổi thứ tự trong Python nhưng chưa commit transaction vẫn có thể mất các batch trước khi `os._exit`. |
| **Cách khắc phục** | Dùng `event_id` làm primary key; multi-row upsert `ON CONFLICT DO UPDATE`; commit transaction DuckDB sau mỗi batch; sau đó mới tới điểm crash và commit offset. Đây là at-least-once + idempotent write. |
| **Bằng chứng** | Lượt chuẩn 20.000/20.000; crash ở batch 7 với offset 3.000; restart đọc 17.000 message và kết quả cuối vẫn 20.000 hàng/20.000 event_id. Không mất, không trùng, C = A. |

Chọn `DO UPDATE` thay vì `DO NOTHING` để message replay có payload đã thay đổi vẫn cập nhật trạng thái mới nhất.

---

## 5 · Tổng kết

| Nhiệm vụ | Điều sẽ kiểm tra trước tiên khi tiếp nhận hệ thống mới |
|---|---|
| 1 | Xác định grain/natural key và SQL write thật sự mà incremental materialization sinh ra khi retry. |
| 2 | So sánh event time với ingestion time, đo percentile độ trễ rồi kiểm tra watermark/lookback. |
| 3 | Kiểm tra contract, phân bố raw value và đường đi của bản ghi không hợp lệ thay vì chỉ nhìn trạng thái job pass. |
