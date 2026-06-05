# Hành trình Debug Spark ETL trên Kubeadm với Google Cloud Storage (GCS)

> Nhâm nhi cốc cà phê và cùng nhìn lại cách chúng ta đưa một Spark ETL Job từ máy local lên môi trường Kubernetes (Kubeadm) và kết nối với GCS nhé.

Hôm nay là một ngày khá dài! Bạn đã có một luồng Spark ETL chạy ngon lành trên máy, nhưng "chạy được trên máy em" chưa bao giờ là đích đến cuối cùng. Mục tiêu của chúng ta là đưa nó lên một cụm Kubernetes tự quản lý (Kubeadm) với các node khá khiêm tốn (2 vCPU, 2GB RAM), và thay vì lưu file cục bộ, chúng ta phải ghi dữ liệu lên Google Cloud Storage (GCS) - đồng thời phải có một Spark History Server để soi lại UI sau khi Job kết thúc.

Nghe thì có vẻ đơn giản, cứ nhét vào Docker rồi ném lên K8s là xong? Không đâu, "ác quỷ nằm ở chi tiết", và hôm nay chúng ta đã đụng độ vài con quỷ như thế.

---

## Phần 1: Approach & Reasoning

**Tại sao tôi lại chọn cách tiếp cận này?**

Điểm xuất phát của chúng ta là một cụm Kubeadm tự build bằng VM, không phải là GKE (Google Kubernetes Engine). Điều này cực kỳ quan trọng vì nó quyết định cách chúng ta xác thực với Google Cloud.

Nếu dùng GKE, tôi chỉ cần búng tay và dùng **Workload Identity** (mapping K8s Service Account với Google Service Account). Nhưng vì chúng ta ở trên Kubeadm, tôi phải dùng phương pháp cổ điển: **Tạo file JSON key, bỏ vào K8s Secret, và mount nó vào Pod**. 

Đối với bản thân luồng ETL, tôi chọn chạy nó dưới dạng K8s `Job` thay vì `Deployment`. Tại sao? K8s Job có đặc tính *Run-to-completion* — nó sinh ra để làm một việc, làm xong là tự sát. Rất hoàn hảo cho ETL. Nhưng chính vì Pod tự chết đi khi chạy xong, chúng ta mất luôn cổng 4040 để xem Spark UI. Đó là lý do tôi phải dựng thêm một `Deployment` riêng cho **Spark History Server** chạy 24/7, đọc log event từ GCS để hiển thị UI của các Job đã hoàn thành.

## Phần 2: Roads Not Taken

**Những con đường đã bị bỏ qua:**

1. **Chạy Spark bằng K8s Native Mode (Cluster/Client mode):**
   - *Ban đầu tôi nghĩ:* "Mình đang ở trên K8s, vậy tại sao không dùng native Spark-on-K8s (cấu hình `--master k8s://...`) để Driver Pod tự spawn ra các Executor Pods?"
   - *Tại sao loại bỏ:* Vì tài nguyên cụm quá eo hẹp! Node worker của bạn chỉ có 2 vCPU và 2GB RAM. Nếu bắt K8s chia nhỏ cái RAM bé xíu đó ra cho 1 Driver và 1 Executor, overhead của hệ điều hành và network sẽ ăn hết.
   - *Quyết định cuối:* Tôi dùng `--master local[*]`. Spark sẽ dồn toàn bộ 1.5GB RAM và 2 CPU vào **một Pod duy nhất** và chạy đa luồng. Một Pod làm tất ăn cả.

2. **Ghi Event Log ra ổ đĩa cục bộ của Pod:**
   - Nếu ghi log ra ổ đĩa của Pod, khi Pod chết, log cũng bốc hơi. Mất log thì History Server lấy gì mà đọc?
   - Do đó, tôi bắt buộc cấu hình `spark.eventLog.dir` trỏ thẳng lên một thư mục trên `gs://`.

## Phần 3: How Things Connect

Hãy tưởng tượng hệ thống của chúng ta như một nhà hàng:

1. **K8s Secret (Kho chìa khóa):** Nó giữ file `key.json`. Đây là thẻ nhân viên để vào kho thực phẩm (GCS).
2. **K8s Job (Đầu bếp thời vụ):** Được CI/CD triệu hồi mỗi khi có task. Nó đeo thẻ nhân viên (mount Secret), vào kho lấy nguyên liệu (đọc GCS), xào nấu (Transform), ghi kết quả lại vào kho, ghi chép sổ sách (Event Log) và sau đó về nhà (Pod Completed).
3. **History Server (Quản lý nhà hàng):** Ngồi ở quầy 24/7, liên tục nhìn vào quyển sổ sách (Event Log trên GCS) để báo cáo lại xem đầu bếp vừa làm món gì, tốn bao nhiêu thời gian. Bạn ở ngoài (máy tính cá nhân) gọi `kubectl port-forward` là có thể gặp anh quản lý này.

## Phần 4: Tools & Methods

- **GCS Connector Shaded JAR:** Thay vì tải connector thường, tôi bắt buộc phải dùng bản `-shaded`. Lý do là Spark image gốc không có sẵn các thư viện lõi của Google (như guava, google-http-client). Bản shaded là một "cục bột nhào sẵn" chứa mọi thứ bên trong.
- **envsubst trong CI/CD:** Để không phải viết hardcode tag image vào file YAML, tôi dùng một công cụ bash cổ điển là `envsubst` để tiêm biến `$IMAGE_TAG` vào K8s manifest một cách mượt mà trước khi apply.

## Phần 5: Tradeoffs

Mọi thứ đều có giá của nó:
- **Dùng `local[*]` trên K8s:** 
  - *Được:* Tận dụng tối đa tài nguyên Node nhỏ, không lằng nhằng phân quyền RBAC cho Spark.
  - *Mất:* Mất đi khả năng scale-out phân tán của Spark. Nếu data phình to lên hàng trăm GB, 1 Pod này sẽ OOM (Out Of Memory). Lúc đó ta phải mua Node to hơn hoặc chuyển về Native K8s Mode.
- **Sử dụng file JSON Key thay vì Workload Identity:**
  - *Được:* Đơn giản, chạy được trên mọi cụm (không bị trói buộc vào GKE).
  - *Mất:* Rủi ro bảo mật cao hơn. Ai có quyền đọc K8s Secret hoặc vô tình `git push` file JSON thì coi như giao nguyên cái kho GCS cho hacker.

## Phần 6: Mistakes & Dead Ends

Đây là phần thú vị nhất, những chỗ tôi đã "đâm đầu vào tường":

1. **Cái chết lặng lẽ của GCS Connector:**
   - Tôi dùng lệnh `ADD` trong Dockerfile để tải `gcs-connector.jar`. Mọi thứ build thành công, nhưng chạy lên Spark lại kêu: *"Ơ tôi không hiểu gs:// là cái gì!"*.
   - Hóa ra, lệnh `ADD` sinh ra file thuộc về user `root` với quyền 600. Mà Spark thì chạy bằng user `185`. User 185 đi ngang qua file jar, thấy ghi "Cấm sờ" nên nó lơ luôn.
   - *Fix:* Phải chèn thêm lệnh `chmod 644` vào Dockerfile.

2. **Lỗi ClassNotFoundException của Google API:**
   - Xong lỗi phân quyền, Spark nhận ra file jar, nhưng lại la làng thiếu class `HttpRequestInitializer`.
   - *Fix:* Như đã nói ở trên, tôi phải vứt file jar cũ và đổi sang link tải bản `-shaded.jar`.

3. **History Server CrashLoopBackOff:**
   - History Server cứ chạy lên vài giây là chết nghẻo. Tôi check log thì chả thấy lỗi gì. Tại sao?
   - Vì cái script `start-history-server.sh` của Spark được thiết kế để chạy ngầm (background daemon) rồi trả về exit code 0. K8s thấy tiến trình chính kết thúc (exit 0) thì đinh ninh là "Ah, thằng này làm xong việc rồi", thế là nó dập Pod. Rồi nó lại sinh Pod mới. Vòng lặp vô tận.
   - *Fix:* Gọi thẳng class Java của History Server (`/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer`) để nó chạy ở foreground, chặn K8s lại.

## Phần 7: Future Pitfalls

*Này, nếu lần sau bạn làm cái này cho dự án khác, hãy nhớ:*
- K8s Job có thuộc tính `ttlSecondsAfterFinished`. Nếu bạn set nó là 30 giây, Pod sẽ bị dọn sạch nhanh đến mức CI/CD GitHub Action của bạn chưa kịp chạy lệnh `kubectl logs` để xem kết quả. Hãy để nó sống tầm 5-10 phút để tiện debug nhé.
- File config K8s của bạn không có cấu hình `resources.requests/limits` cho History Server. Nếu nó ăn RAM quá nhiều, K8s sẽ chém nó (OOMKilled). Nhớ đặt giới hạn cho nó sau này.

## Phần 8: Expert vs Beginner

- **Beginner:** Gặp lỗi "No FileSystem for scheme gs", Beginner sẽ lùng sục Google tìm đủ mọi thư viện maven add vào `pom.xml`, đổi code Scala loạn xạ, rồi rebuild lại code.
- **Expert:** Sẽ đặt câu hỏi: "Thư viện đã có trong Docker image, tại sao runtime không thấy?". Họ sẽ kiểm tra Docker layer, soi quyền file (permissions), và sau đó thêm cấu hình `--conf spark.hadoop.fs.gs.impl` lúc submit thay vì đụng vào code. Họ tách biệt tầng Infrastructure (K8s/Docker config) khỏi tầng Application (Scala code).

## Phần 9: Transferable Lessons

Hai bài học cốt lõi có thể mang đi mọi nơi:
1. **The Foreground Rule:** Bất kỳ ứng dụng nào chạy trên Docker/K8s (dù là Spark, Nginx, hay cronjob), tiến trình lõi phải LUÔN LUÔN chạy ở foreground. Đừng bao giờ dùng lệnh daemonizing (như `&` hay `nohup`), hệ thống quản lý container sẽ hiểu lầm là app đã dừng.
2. **Fat JARs vs Thin JARs:** Khi kết nối với cloud APIs (AWS/GCP/Azure) từ một hệ sinh thái cũ (như Hadoop/Spark), hãy luôn tìm bản **Shaded (Fat) JAR**. Conflict phiên bản thư viện HTTP Client là thứ giết chết thời gian khủng khiếp nhất. Shaded JAR gói ghém tất cả lại thành một vũ trụ riêng biệt, cứu bạn khỏi mớ bòng bong dependency hell. 

Uống cạn ly cà phê thôi, chúng ta đã làm tốt lắm rồi! 🚀
