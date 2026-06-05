# 🚀 Hướng dẫn Triển khai Spark ETL lên Kubeadm (VMs) với GCP GCS

Hướng dẫn này đã được viết lại cho hạ tầng Kubernetes tự quản lý (dựng bằng `kubeadm` trên VM) và sử dụng Google Cloud Storage (GCS) thông qua JSON Key (thay vì GKE Workload Identity).

---

## Bước 1: Tạo GCS Bucket (Đã hoàn thành ✅)

```bash
gcloud storage buckets create gs://k8s-deployment-bucket-datnc/ --uniform-bucket-level-access
```

## Bước 2: Upload Dữ liệu từ Local lên GCS (Đã hoàn thành ✅)

```bash
gsutil cp -r "C:\Users\Lenovo\Downloads\archive\trip\*" gs://k8s-deployment-bucket-datnc/raw/trips/
gcloud storage cp "C:\Users\Lenovo\Downloads\archive\taxi_zone_lookup.csv" gs://k8s-deployment-bucket-datnc/raw/taxi_zone_lookup/taxi_zone_lookup.csv
gsutil cp - gs://k8s-deployment-bucket-datnc/output/kpi/
gsutil cp /dev/null gs://k8s-deployment-bucket-datnc/spark-events/.keep
gsutil cp /dev/null gs://k8s-deployment-bucket-datnc/dlq/.keep
```

---

## Bước 3: Tạo Service Account và K8s Secret cho GCS Access

Vì bạn dùng K8s tự quản lý, Pod không tự động có quyền truy cập GCP. Chúng ta cần tạo một Service Account (SA), lấy key JSON và lưu vào Kubernetes Secret để Pod sử dụng.

### 3.1. Tạo GCP Service Account & Cấp quyền
*Thực hiện trên máy local có `gcloud` hoặc GCP Cloud Shell:*

```bash
# 1. Tạo Service Account
gcloud iam service-accounts create spark-etl-sa \
    --display-name="Spark ETL Service Account" \
    --project=YOUR_PROJECT_ID

# 2. Cấp quyền truy cập GCS (Storage Object Admin)
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:spark-etl-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# 3. Tạo key JSON và tải về máy tính (lưu tên là key.json)
gcloud iam service-accounts keys create key.json \
    --iam-account=spark-etl-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### 3.2. Tạo K8s Secret trên Cluster của bạn
*Copy file `key.json` lên Master node (hoặc dùng `kubectl` từ local trỏ lên Master node).*

```bash
# Tạo namespace trước (nếu chưa có)
kubectl create namespace spark-etl

# Tạo K8s secret từ file key.json
kubectl create secret generic gcp-key \
    --from-file=key.json=key.json \
    --namespace=spark-etl
```

*(Lưu ý: Bạn tuyệt đối không được push file `key.json` lên GitHub).*

Tôi đã cập nhật `k8s-deployment.yaml` và `history-server.yaml` để tự động mount secret `gcp-key` này vào container tại `/etc/gcp/key.json`.

---

## Bước 4: Mở rộng Worker Nodes (Kubeadm)

Hiện tại bạn có 2 worker nodes (1 CPU / 2GB RAM). 
Để Spark chạy hiệu quả và chia tải với History Server, bạn có thể tạo thêm 1-2 Worker node với cấu hình **2 vCPU / 2GB RAM** (hoặc 4GB RAM).

### 4.1. Khởi tạo VM mới trên GCP (Compute Engine)
Tạo 2 VM instances (chọn OS Ubuntu, loại `e2-small` - 2vCPU, 2GB RAM hoặc `e2-medium` - 2vCPU, 4GB RAM). Đảm bảo 2 VM này cùng mạng VPC với Master node của bạn.

### 4.2. Cài đặt Docker & Kubeadm trên VM mới
*Tham khảo tài liệu cài đặt của bạn (Cài đặt containerd/docker, kubeadm, kubelet, kubectl).*

### 4.3. Join Node vào Cluster
Trên Master Node, lấy lệnh join:
```bash
kubeadm token create --print-join-command
```

Sau đó chạy lệnh vừa tạo được (ví dụ `kubeadm join <master-ip>:6443 --token...`) trên các Worker Node mới.
Trên Master Node, kiểm tra nodes:
```bash
kubectl get nodes
```

---

## Bước 5: Cấu hình GitHub Secrets cho CI/CD

Vào GitHub Repository → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**.

Bạn cần tạo 3 secrets sau để GitHub Actions có thể build Image và Deploy:

| Secret Name | Mô tả / Giá trị |
|---|---|
| `DOCKER_USERNAME` | Tên đăng nhập Docker Hub của bạn |
| `DOCKER_PASSWORD` | Password (hoặc Access Token) Docker Hub |
| `KUBE_CONFIG_BASE64` | Nội dung file kubeconfig của Master Node (mã hoá Base64) |

### Cách lấy KUBE_CONFIG_BASE64 từ Master Node:
Đăng nhập vào Master Node của bạn và chạy lệnh sau:
```bash
cat ~/.kube/config | base64 -w 0
```
Copy toàn bộ output (một chuỗi dài không có dấu xuống dòng) và dán vào secret `KUBE_CONFIG_BASE64` trên GitHub.

> **Lưu ý quan trọng**: Vì GitHub Runner chạy trên hạ tầng của GitHub, địa chỉ API Server của K8s trong `~/.kube/config` (chỗ `server: https://<IP>:6443`) phải là **IP Public** (External IP) của Master Node, và cổng 6443 của Master Node phải được mở firewall cho phép GitHub truy cập. 

---

## Bước 6: Push Code & Chạy CI/CD

```bash
cd d:\K8s_deployment

# Thêm code mới vào git
git add .

# Commit
git commit -m "feat: setup Kubeadm config with GCP Secret for Spark ETL"

# Push lên main
git push origin main
```

Quy trình CI/CD sẽ tự động:
1. Build image mới nhất.
2. Push lên Docker Hub.
3. Thay thế biến môi trường và apply manifest lên Cụm Kubeadm của bạn.

---

## Bước 7: Xem Spark History Server (Spark UI)

Do Job K8s sẽ tự xoá Pod khi hoàn thành, bạn dùng Spark History Server để xem UI lịch sử của Job.

### 7.1 Port-forward từ máy bạn (nếu có kubectl kết nối tới Master)
```bash
kubectl port-forward -n spark-etl svc/spark-history-server 18080:18080
```
Mở trình duyệt: `http://localhost:18080`

### 7.2 Mở qua NodePort (Cách thay thế nếu không muốn port-forward)
Nếu muốn mở giao diện trực tiếp, bạn sửa file `history-server.yaml`, đổi `type: ClusterIP` thành `type: NodePort` và mở firewall ở port đó trên Worker node.

---

## Bước 8: Kiểm tra Kết quả 5 KPIs trên GCS

Sau khi Job chạy thành công, kết quả sẽ được ghi vào GCS:

```bash
# Xem thư mục chứa các KPIs
gsutil ls gs://k8s-deployment-bucket-datnc/output/kpi/

# Xem chi tiết từng KPI
gsutil ls gs://k8s-deployment-bucket-datnc/output/kpi/total_trips_by_zone/
gsutil ls gs://k8s-deployment-bucket-datnc/output/kpi/avg_wait_time/
gsutil ls gs://k8s-deployment-bucket-datnc/output/kpi/driver_pay_vs_fare/
gsutil ls gs://k8s-deployment-bucket-datnc/output/kpi/avg_trip_speed/
gsutil ls gs://k8s-deployment-bucket-datnc/output/kpi/shared_ride_match_rate/

# Xem dữ liệu lỗi bị đẩy ra DLQ (nếu có)
gsutil ls gs://k8s-deployment-bucket-datnc/dlq/
```
