<RULE[scala-spark-etl.md]>

# Scala Spark ETL Rules

## Khi nào áp dụng
Quy tắc này áp dụng **tự động** khi viết, refactor, hoặc review code Scala cho các luồng ETL sử dụng Apache Spark.

## Hành vi bắt buộc và Best Practices

### 1. Quản lý SparkSession
- Không tạo nhiều `SparkSession`. Sử dụng một `SparkSession` duy nhất trong suốt vòng đời của ứng dụng.
- Dùng `SparkSession.builder.getOrCreate()` để tái sử dụng session hiện có.
- Khai báo implicits `import spark.implicits._` ngay sau khi khởi tạo SparkSession để hỗ trợ encoders cho DataFrame/Dataset.

### 2. Sử dụng Dataset/DataFrame thay cho RDD
- Luôn ưu tiên sử dụng `DataFrame` (hoặc `Dataset` có kiểu dữ liệu mạnh ở Scala) thay vì `RDD`. `DataFrame/Dataset` tận dụng Catalyst Optimizer và Tungsten execution engine để tối ưu hóa hiệu năng.
- RDD chỉ nên được sử dụng cho các phép toán mức thấp không thể thể hiện qua các API của DataFrame/Dataset.

### 3. Hạn chế sử dụng UDF (User Defined Functions)
- Luôn cố gắng sử dụng các hàm có sẵn trong `org.apache.spark.sql.functions._` trước khi quyết định viết UDF. Các hàm built-in đã được tối ưu hóa bởi Catalyst Optimizer.
- Nếu bắt buộc phải dùng UDF, hãy chú ý đến vấn đề hiệu năng và quá trình serialization. Tránh xử lý logic quá phức tạp bên trong UDF.

### 4. Tối ưu hóa Transformations
- **Lọc sớm (Filter Early):** Áp dụng `filter()` hoặc `where()` càng sớm càng tốt (ngay sau khi đọc dữ liệu nếu có thể) để giảm lượng dữ liệu cần xử lý trong các bước tiếp theo.
- **Tránh `groupByKey`:** Khi dùng RDD, sử dụng `reduceByKey` hoặc `aggregateByKey`. Khi dùng DataFrame, API `groupBy().agg()` tự động tối ưu hóa việc này.
- **Tối ưu hóa `join`:** 
  - Nếu một DataFrame rất nhỏ, hãy sử dụng Broadcast Join (`broadcast(dfSmall)`) để tránh shuffle dữ liệu toàn cụm.
  - Đảm bảo các cột dùng để join không có quá nhiều giá trị null hoặc lặp lại để tránh data skew.

### 5. Quản lý bộ nhớ (Caching & Persisting)
- Sử dụng `cache()` hoặc `persist()` khi một DataFrame/Dataset được sử dụng lại **nhiều lần** (từ 2 lần trở lên) trong flow ETL để tránh tính toán lại.
- Luôn gọi `unpersist()` sau khi không còn sử dụng DataFrame đó nữa để giải phóng bộ nhớ cho cụm.
- Chọn StorageLevel phù hợp (ví dụ `MEMORY_AND_DISK`) nếu dữ liệu quá lớn không thể chứa hoàn toàn trong RAM.

### 6. Partitioning và Shuffle
- Điều chỉnh số lượng partitions phù hợp. Sử dụng `repartition()` để tăng hoặc phân phối lại số lượng partitions khi dữ liệu bị lệch (skew) nhưng lưu ý thao tác này gây ra full shuffle.
- Luôn ưu tiên sử dụng `coalesce()` thay vì `repartition()` khi muốn **giảm** số lượng partitions, vì `coalesce` không gây ra shuffle toàn bộ dữ liệu.

### 7. Đọc và Ghi dữ liệu
- Ưu tiên sử dụng định dạng columnar như **Parquet** hoặc **ORC** cho việc lưu trữ trung gian và đầu ra vì tính hiệu quả về dung lượng và hỗ trợ predicate pushdown.
- Tránh tạo ra quá nhiều file nhỏ (Small Files Problem). Nên `coalesce` hoặc `repartition` trước khi lưu dữ liệu vào Data Lake/HDFS để có kích thước file tối ưu (khoảng 128MB - 256MB/file).

### 8. Cấu trúc mã nguồn ETL (Mẫu Thiết Kế)
- Chia tách rõ ràng logic ETL thành các khối độc lập để dễ bảo trì và unit test:
  - `extract()`: Chuyên trách đọc dữ liệu từ các nguồn (Source).
  - `transform()`: Nhận vào tham số là các DataFrame và trả về DataFrame. Không chứa side-effect (không đọc/ghi ra ngoài).
  - `load()`: Chuyên trách ghi dữ liệu cuối cùng vào đích (Sink).

### 9. Xử lý lỗi và Logging
- **Sử dụng thư viện Logging chuẩn:** Khi submit Spark job ở mode `cluster`, hàm `println` sẽ in ra stdout của các Executor và rất khó để gom/xem log tập trung. Do đó, BẮT BUỘC phải sử dụng thư viện logging chuẩn (như `Log4j` thông qua `org.apache.log4j.Logger` hoặc `SLF4J`).
- **Tránh lỗi Serialization cho Logger:** Khai báo đối tượng Logger sử dụng `@transient lazy val` để tránh lỗi TaskNotSerializable khi Spark truyền hàm từ Driver xuống Executor:
  ```scala
  @transient lazy val logger = org.apache.log4j.Logger.getLogger(getClass.getName)
  ```
- **Mức độ Logging:** Bố trí các dòng log (INFO, WARN, ERROR) hợp lý ở các bước chính: bắt đầu đọc dữ liệu, số record đọc được, cảnh báo khi có null data, và khi ghi thành công để dễ dàng debug qua YARN logs hoặc Spark History Server.
- **Xử lý Exception:** Xử lý các exception một cách an toàn bằng `Try`, `Success`, `Failure` của Scala đối với các tác vụ I/O có nguy cơ gây lỗi.

</RULE[scala-spark-etl.md]>
