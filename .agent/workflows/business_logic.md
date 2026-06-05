---
description: # TÀI LIỆU ĐẶC TẢ KỸ THUẬT ETL (UBER HVFHV 2021 - SCALA SPARK) *Định hướng thiết kế logic và xử lý dữ liệu dành cho AI Agent / Data Engineer*
---

## BỐI CẢNH & LƯỢC ĐỒ DỮ LIỆU (DATA SCHEMA)
Hệ thống xử lý tập dữ liệu **TLC High Volume For-Hire Vehicle (HVFHV) 2021**. Bài toán yêu cầu kết hợp dữ liệu giao dịch lớn (Fact) với dữ liệu danh mục nhỏ (Dimension) để tính toán các chỉ tiêu vận hành.

**1. Bảng Fact (Dữ liệu chuyến đi - Dung lượng lớn, định dạng Parquet)**
Các cột cốt lõi phục vụ tính toán:
* `request_datetime`, `pickup_datetime`: Phục vụ tính thời gian chờ.
* `trip_miles`, `trip_time`: Phục vụ tính vận tốc và quãng đường.
* `PULocationID`, `DOLocationID`: ID khu vực đón/trả.
* `base_passenger_fare`, `driver_pay`: Phục vụ phân tích tài chính.
* `shared_request_flag`, `shared_match_flag`: Cờ đánh dấu chuyến đi ghép.

**2. Bảng Dimension (Zone Lookup - Rất nhỏ, định dạng CSV)**
* `LocationID`: Khóa ngoại để JOIN với bảng Fact.
* `Borough`, `Zone`: Tên quận và khu vực chi tiết.

---

## 1. CHẨN ĐOÁN (DIAGNOSE)
* **Data Skew (Lệch dữ liệu):** Dữ liệu tập trung cực kỳ dày đặc ở khu vực Manhattan. Nếu thực hiện JOIN thông thường, các task xử lý phân vùng Manhattan sẽ bị nghẽn (straggler) hoặc gây lỗi `Out Of Memory (OOM)`.
* **Data Anomalies (Dữ liệu rác):** Chắc chắn tồn tại các bản ghi lỗi từ hệ thống GPS (`trip_miles` < 0, `trip_time` <= 0). Cần xử lý ngay từ đầu nguồn để không làm hỏng các hàm Aggregate.

## 2. ĐỀ XUẤT (PROPOSE)
Bắt buộc áp dụng kiến trúc **Broadcast Hash Join**. 
* *Lý do:* Bảng Dimension (Zone) chỉ có ~265 dòng. Việc dùng hàm `broadcast()` đẩy bảng này vào bộ nhớ của toàn bộ Executor sẽ giúp triệt tiêu hoàn toàn quá trình Shuffle dữ liệu bảng Fact qua mạng. 
* *Đánh đổi:* Tốn thêm vài chục KB RAM trên mỗi node, nhưng giải quyết dứt điểm bài toán Data Skew và tăng tốc I/O.

## 3. THỰC THI (EXECUTE) - LOGIC & 5 CHỈ TIÊU ETL
Thay vì viết toàn bộ script, Agent cần bám sát logic xử lý Scala/Spark SQL sau:

**Bước 1: Cleansing (Data Quality)**
Lọc bỏ các bản ghi không hợp lệ trước khi thực hiện bất kỳ phép toán nào:
```scala
val cleanedDF = tripsDF.filter($"trip_miles" > 0 && $"trip_time" > 0 && $"base_passenger_fare" >= 0)
```

**Bước 2: Broadcast Join**
```scala
import org.apache.spark.sql.functions.broadcast
val enrichedDF = cleanedDF.join(broadcast(zoneDF), $"PULocationID" === $"LocationID", "left")
```

**Bước 3: Tính toán 5 Chỉ tiêu nghiệp vụ (KPIs)**
1.  **Total Trips by Pickup Zone (Mật độ thị trường):** Group by theo `Borough` và `Zone`, sau đó `count()`.
2.  **Average Wait Time (Chất lượng dịch vụ):** Tính chênh lệch `pickup_datetime - request_datetime` ra phút, sau đó lấy `avg()`.
3.  **Driver Pay vs Passenger Fare (Tài chính):** Ép kiểu `pickup_datetime` sang Date (`to_date`), group by ngày và `sum()` cho `driver_pay`, `base_passenger_fare`.
4.  **Average Trip Speed (Hiệu năng vận hành):** Tạo cột speed = `trip_miles / (trip_time / 3600)`, sau đó lấy `avg()` để đánh giá tình trạng kẹt xe.
5.  **Shared Ride Match Rate (Hiệu suất thuật toán):** Đếm số lượng record có `shared_match_flag == "Y"` chia cho tổng số record yêu cầu đi chung (`shared_request_flag == "Y"`).

---

## 4. KIỂM SOÁT (CONTROL)
Để vận hành ổn định trên Production, yêu cầu bắt buộc thiết lập 2 cơ chế:

1.  **Fault Tolerance (Cơ chế ghi đè chống trùng lặp):** Khi ghi dữ liệu đầu ra (Sink), tuyệt đối không dùng mode `Append`. Phải cấu hình ghi đè động để đảm bảo tính Idempotency (dù Job chạy lại bao nhiêu lần thì dữ liệu không bị nhân đôi).
    *Cấu hình Spark (Scala):* `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")`
    Khi write, lưu ý dùng `.mode("overwrite")` và `.partitionBy("trip_date")`.
2.  **Data Quality (Cơ chế cảnh báo):** Những record bị loại ra ở Bước 1 (Cleansing) không được âm thầm vứt bỏ. Hãy rẽ nhánh luồng dữ liệu lỗi này ghi vào một bảng **Dead Letter Queue (DLQ)**. Bắn Alert (Slack/Email) nếu lượng data trong DLQ vượt quá 1% tổng volume trong ngày.