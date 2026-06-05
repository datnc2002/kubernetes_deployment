# spark-etl

> Tổng hợp kiến thức về hệ thống Config-Driven Spark ETL chạy trên Kubernetes.
> Cập nhật lần cuối: 2026-05-25

---

## Architecture

### Config-Driven Hybrid ETL
- **Ngày**: 2026-05-25
- **Chi tiết**: Sử dụng 1 Docker image duy nhất cho toàn bộ các luồng ETL. Dùng `ConfigMap` chứa `etl-config.json` truyền vào container để định nghĩa logic (`sql` mode cho câu truy vấn động, `custom` mode cho hàm code Scala phức tạp). Giúp đổi logic mà không cần rebuild image.
- **Files liên quan**: `k8s-deployment.yaml`, `ETLConfig.scala`

### K8s Batch Processing thay vì Deployment
- **Ngày**: 2026-05-25
- **Chi tiết**: Đối với các tác vụ ETL, sử dụng K8s `Job` thay vì `Deployment` (như Node.js app cũ) vì Job tự động chạy một tác vụ đến khi hoàn thành rồi dừng (Run-to-completion).
- **Files liên quan**: `k8s-deployment.yaml`

---

## Bugs & Solutions

### Spark Lazy Evaluation bị phá vỡ (Full Scan)
- **Ngày**: 2026-05-25
- **Vấn đề**: Gọi `df.count()` ngay sau bước Extract khiến Spark đọc toàn bộ dữ liệu chỉ để log số lượng dòng, tốn kém tài nguyên và thời gian (đặc biệt trên GCS/S3).
- **Root cause**: Lệnh `count()` là một Action trong Spark, nó sẽ kích hoạt việc tính toán (execution) ngay lập tức.
- **Fix**: Xoá lệnh `df.count()`. Để log thông tin mà vẫn giữ lazy evaluation, chỉ in cấu trúc bảng bằng `df.schema.simpleString`.
- **Files liên quan**: `Extractor.scala`

### Pod Zombie sau khi Job chạy xong
- **Ngày**: 2026-05-25
- **Vấn đề**: K8s Job chạy xong (Completed) nhưng Pod vẫn nằm đó chiếm dụng K8s Control Plane.
- **Root cause**: Thiếu cấu hình dọn dẹp sau khi Job kết thúc.
- **Fix**: Thêm `ttlSecondsAfterFinished: 300` vào spec của Job để K8s dọn dẹp Pod 5 phút sau khi xong.
- **Files liên quan**: `k8s-deployment.yaml`

### K8s Job Update bị lỗi (Immutable field)
- **Ngày**: 2026-05-25
- **Vấn đề**: Bước apply Job trong CI/CD pipeline thất bại vì đặc tả của Job trong K8s là bất biến (immutable), không thể đè lên cấu hình cũ.
- **Root cause**: `kubectl apply` cố sửa đổi một resource không cho phép sửa đổi.
- **Fix**: Luôn chạy `kubectl delete job <job-name> --ignore-not-found=true` trước khi apply file manifest mới.
- **Files liên quan**: `deploy.yml`

### Lỗi shutdown do System.exit(1)
- **Ngày**: 2026-05-25
- **Vấn đề**: Gọi `System.exit(1)` trong Spark K8s app gây gián đoạn đột ngột.
- **Root cause**: Thoát thẳng JVM ngăn các shutdown hooks (dọn dẹp temp, đóng kết nối) của Spark chạy.
- **Fix**: Dùng `throw new RuntimeException(...)` để ném lỗi, Spark runtime sẽ bắt và xử lý shutdown gracefully.
- **Files liên quan**: `SparkETLApp.scala`

---

## How-To

### Tiêm biến an toàn vào K8s Manifest bằng envsubst
- **Ngày**: 2026-05-25
- **Bước thực hiện**:
  1. Sử dụng biến môi trường dạng `$IMAGE_TAG` trong file yaml mẫu.
  2. Trong CI/CD, dùng lệnh `envsubst '$IMAGE_TAG' < k8s-deployment.yaml > final.yaml` để thay giá trị.
  3. Lệnh này chỉ thay thế đúng biến được truyền (`$IMAGE_TAG`), bỏ qua các biến khác có cú pháp `${}` trong ConfigMap để tránh lỗi.
- **Files liên quan**: `deploy.yml`

---

## Patterns

### Custom Transform Registry Pattern
- **Ngày**: 2026-05-25
- **Chi tiết**: Để linh hoạt xử lý logic ETL phức tạp không viết được bằng SQL, áp dụng registry pattern bằng Map (`Map[String, DataFrame => DataFrame]`) để lưu và gọi hàm transform theo tên cấu hình trong JSON, tránh việc phải hardcode `if/else` chằng chịt.
- **Ví dụ code**:
  ```scala
  val registry = Map("calculate_profit" -> calculateProfit _)
  val transformFunc = registry(config.transformName)
  val outputDf = transformFunc(inputDf)
  ```
- **Files liên quan**: `CustomTransforms.scala`, `Transformer.scala`

### Spark Logging Schema
- **Ngày**: 2026-05-25
- **Chi tiết**: Dùng `logger.info(s"Schema: ${df.schema.simpleString}")` thay vì `df.printSchema()`. Lý do là `printSchema` in thẳng ra stdout (khó kiểm soát level log) và in theo dạng cây tốn dòng, trong khi `simpleString` gọn hơn và qua được SLF4J/Log4j.
- **Files liên quan**: `Transformer.scala`, `Extractor.scala`
