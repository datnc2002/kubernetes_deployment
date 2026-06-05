# spark-etl

> Tổng hợp kiến thức về hệ thống Config-Driven Spark ETL chạy trên Kubernetes (Kubeadm VMs).
> Cập nhật lần cuối: 2026-06-06

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

### Spark local[*] trên K8s = Cluster-safe
- **Ngày**: 2026-06-05
- **Chi tiết**: Dùng `--master local[*]` bên trong Pod K8s là an toàn và độc lập — không phải Client mode, không phụ thuộc máy local. Driver + Executor chạy cùng 1 JVM trong Pod, tận dụng toàn bộ CPU của container. Phù hợp với node nhỏ (2 vCPU/2GB RAM) vì tránh overhead tạo nhiều Executor Pod. Tắt máy tính cá nhân, Job vẫn chạy bình thường trên Worker node.
- **Files liên quan**: `k8s-deployment.yaml`

### GCS Auth trên Kubeadm bằng K8s Secret
- **Ngày**: 2026-06-05
- **Chi tiết**: Cụm K8s tự quản lý (Kubeadm) không có Workload Identity như GKE. Giải pháp: Tạo GCP Service Account → tải `key.json` → lưu vào K8s Secret `gcp-key` → mount vào Pod tại `/etc/gcp/key.json`. Cấu hình Spark: `spark.hadoop.google.cloud.auth.service.account.json.keyfile=/etc/gcp/key.json`. **Tuyệt đối không push key.json lên GitHub.**
- **Files liên quan**: `k8s-deployment.yaml`, `history-server.yaml`, `GCP_GKE_SPARK_GUIDE.md`

### Spark History Server thay Spark UI 4040
- **Ngày**: 2026-06-05
- **Chi tiết**: K8s Job chạy xong thì Pod bị xoá → không thể xem Spark UI (port 4040). Giải pháp: Bật Event Log (`spark.eventLog.enabled=true`) ghi lên GCS, sau đó dùng History Server (Deployment chạy 24/7) đọc Event Log để hiển thị UI tại port 18080. Xem bằng `kubectl port-forward svc/spark-history-server 18080:18080`.
- **Files liên quan**: `history-server.yaml`, `k8s-deployment.yaml`

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
- **Fix**: Thêm `ttlSecondsAfterFinished: 300` vào spec của Job để K8s dọn dẹp Pod 5 phút sau khi xong. **Chú ý**: Nếu đặt TTL quá ngắn (300s) mà CI/CD wait timeout lâu hơn (600s), Pod sẽ bị xoá trước khi CI/CD lấy log → lỗi `NotFound`. Nên đặt TTL ≥ timeout CI/CD.
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

### GCS "No FileSystem for scheme gs" + ClassNotFoundException
- **Ngày**: 2026-06-05
- **Vấn đề**: Spark báo `UnsupportedFileSystemException: No FileSystem for scheme "gs"`, sau khi fix thêm config thì báo `ClassNotFoundException: com.google.api.client.http.HttpRequestInitializer`.
- **Root cause**: 2 lớp lỗi: (1) Thiếu cấu hình `spark.hadoop.fs.gs.impl` để map scheme `gs://` → Java class. (2) File `gcs-connector.jar` bản thường (non-shaded) không chứa các class phụ thuộc như google-api-client, guava.
- **Fix**: Thêm 2 dòng config: `spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem` và `spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS`. Trong Dockerfile, đổi sang bản **`-shaded.jar`** (bao gồm toàn bộ dependency).
- **Files liên quan**: `Dockerfile`, `k8s-deployment.yaml`, `history-server.yaml`

### History Server CrashLoopBackOff
- **Ngày**: 2026-06-05
- **Vấn đề**: Pod History Server liên tục sập (CrashLoopBackOff), port-forward báo `connection refused`.
- **Root cause**: Script `start-history-server.sh` chạy History Server ở chế độ daemon (background). K8s thấy tiến trình chính kết thúc → tưởng Pod đã xong → giết Pod → vòng lặp crash.
- **Fix**: Thay command thành `["/opt/spark/bin/spark-class", "org.apache.spark.deploy.history.HistoryServer"]` để tiến trình chạy ở **foreground**, K8s sẽ giữ Pod sống.
- **Files liên quan**: `history-server.yaml`

### GCS Connector JAR Permission
- **Ngày**: 2026-06-05
- **Vấn đề**: Spark âm thầm bỏ qua gcs-connector.jar dù file đã tồn tại trong image.
- **Root cause**: Lệnh `ADD` trong Dockerfile lưu file với quyền root, user `spark` (uid 185) không đọc được.
- **Fix**: Thêm `RUN chmod 644 /opt/spark/jars/gcs-connector-*.jar` sau lệnh ADD.
- **Files liên quan**: `Dockerfile`

---

## How-To

### Tiêm biến an toàn vào K8s Manifest bằng envsubst
- **Ngày**: 2026-05-25
- **Bước thực hiện**:
  1. Sử dụng biến môi trường dạng `$IMAGE_TAG` trong file yaml mẫu.
  2. Trong CI/CD, dùng lệnh `envsubst '$IMAGE_TAG' < k8s-deployment.yaml > final.yaml` để thay giá trị.
  3. Lệnh này chỉ thay thế đúng biến được truyền (`$IMAGE_TAG`), bỏ qua các biến khác có cú pháp `${}` trong ConfigMap để tránh lỗi.
- **Files liên quan**: `deploy.yml`

### Upload dữ liệu từ local lên GCS
- **Ngày**: 2026-06-05
- **Bước thực hiện**:
  1. Tạo bucket: `gcloud storage buckets create gs://bucket-name/ --uniform-bucket-level-access`
  2. Upload Parquet: `gsutil -m cp -r local/path/*.parquet gs://bucket/raw/trips/` (cờ `-m` = multi-thread, nhanh hơn)
  3. Upload CSV: `gcloud storage cp local.csv gs://bucket/raw/zone_lookup/file.csv`
  4. Tạo thư mục ảo: `gsutil cp /dev/null gs://bucket/spark-events/.keep`
- **Files liên quan**: `GCP_GKE_SPARK_GUIDE.md`

### Tạo K8s Secret cho GCP key.json
- **Ngày**: 2026-06-05
- **Bước thực hiện**:
  1. Tạo SA: `gcloud iam service-accounts create spark-etl-sa`
  2. Gán role: `gcloud projects add-iam-policy-binding PROJECT --member=serviceAccount:... --role=roles/storage.objectAdmin`
  3. Tải key: `gcloud iam service-accounts keys create key.json --iam-account=...`
  4. Tạo K8s Secret: `kubectl create secret generic gcp-key --from-file=key.json=key.json -n spark-etl`
  5. Manifest mount: `volumeMount /etc/gcp` + `volume secret gcp-key`
- **Files liên quan**: `k8s-deployment.yaml`, `history-server.yaml`, `GCP_GKE_SPARK_GUIDE.md`

### Debug K8s Job bị crash
- **Ngày**: 2026-06-05
- **Bước thực hiện**:
  1. `kubectl get pods -n spark-etl` → xem trạng thái (CrashLoopBackOff, Error, ContainerCreating...)
  2. `kubectl get events -n spark-etl --sort-by='.metadata.creationTimestamp'` → xem sự kiện gần nhất
  3. `kubectl logs job/<job-name> -n spark-etl` → xem log ứng dụng Spark
  4. Nếu Job bị xoá do TTL: tạm tăng/xoá `ttlSecondsAfterFinished` để giữ Pod lại debug
- **Files liên quan**: `k8s-deployment.yaml`, `deploy.yml`

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

### CI/CD Deploy History Server + Job
- **Ngày**: 2026-06-05
- **Chi tiết**: History Server là `Deployment` (kubectl apply tự update). ETL Job là `Job` (phải delete trước rồi apply mới). Cần envsubst cho **cả 2 file manifest** vì cả hai đều dùng biến `${IMAGE_TAG}`.
- **Files liên quan**: `deploy.yml`, `k8s-deployment.yaml`, `history-server.yaml`

### Dockerfile: Shaded vs Non-shaded JAR
- **Ngày**: 2026-06-05
- **Chi tiết**: Khi ADD thư viện connector (GCS, S3, JDBC...) vào Spark image, luôn chọn bản **shaded** (có suffix `-shaded.jar`). Bản non-shaded chỉ chứa code chính, thiếu transitive dependencies → runtime ClassNotFoundException. Bản shaded gộp tất cả dependency vào 1 file, giải quyết triệt để.
- **Files liên quan**: `Dockerfile`

### Foreground vs Background process trong K8s
- **Ngày**: 2026-06-05
- **Chi tiết**: K8s theo dõi PID 1 (tiến trình chính) của container. Nếu tiến trình chính thoát ra (dù đã spawn daemon ở background), K8s sẽ tưởng container xong và giết Pod. Khi dùng các script daemon (ví dụ `start-*.sh` của Spark/Hadoop), phải thay bằng cách chạy trực tiếp class Java (`spark-class`) để giữ foreground.
- **Files liên quan**: `history-server.yaml`
