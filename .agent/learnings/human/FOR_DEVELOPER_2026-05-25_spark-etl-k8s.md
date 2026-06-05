# Hành trình chuyển đổi Node.js Web App sang Config-Driven Spark ETL trên Kubernetes

*Như một buổi trò chuyện bên ly cà phê, hãy cùng nhìn lại cách chúng ta đã "đập đi xây lại" luồng xử lý dữ liệu của dự án này.*

---

### Phần 1: Approach & Reasoning
> Tôi đã chọn cách tiếp cận nào và tại sao? Điểm xuất phát là gì?

Ban đầu, chúng ta có một ứng dụng Node.js chạy như một Web Service (Deployment trên Kubernetes). Vấn đề là xử lý dữ liệu (ETL) không phải là một process chạy liên tục kiểu "lắng nghe request". Nó là một dạng công việc "chạy một đống dữ liệu, xong việc thì tắt". 

Vì thế, tôi quyết định loại bỏ hoàn toàn Node.js và chuyển sang dùng Apache Spark thuần túy, đóng gói bằng Scala & Maven. Quan trọng hơn, thay vì mỗi job ETL (ví dụ có 5 chỉ tiêu khác nhau) lại phải build một Docker image riêng, tôi chọn kiến trúc **Config-Driven**. Tưởng tượng bạn chỉ sản xuất một con robot duy nhất (Docker Image), nhưng cho nó đọc các bản hướng dẫn khác nhau (ConfigMap JSON) để nó làm các việc khác nhau. Điều này giúp chúng ta scale số lượng job lên vô hạn mà không cần động vào code gốc, trừ phi có logic quá phức tạp.

### Phần 2: Roads Not Taken
> Những con đường nào đã bị gạch bỏ?

1. **Giữ lại Node.js và dùng nó để gọi Spark submit**: Cách này giống như bạn thuê một phiên dịch viên chỉ để nói "Alo, bắt đầu làm việc đi". Nó dư thừa, làm phức tạp hệ thống, và tốn tài nguyên cho một cái server Node.js chạy ngầm không làm gì ngoài việc chờ.
2. **Build nhiều Docker image cho từng job**: Nếu có 5 job, ta có 5 file `Job1.scala`, `Job2.scala` và 5 cái Dockerfile. Cách này tốn cực kỳ nhiều ổ cứng, thời gian build CI/CD sẽ lâu phát điên, và mỗi lần cập nhật version thư viện là phải sửa ở 5 nơi.
3. **Chỉ dùng thuần Spark SQL**: Mặc dù SQL rất dễ đọc qua file JSON, nhưng Spark DataFrame API có những khả năng tối ưu (như xử lý dữ liệu phức tạp, gọi ML model, thao tác rdd) mà SQL thuần không làm được. Nếu bó hẹp vào SQL, ta sẽ bị kẹt khi gặp requirement khó. Do đó, tôi đã kết hợp cả 2: `sql` mode và `custom` mode.

### Phần 3: How Things Connect
> Các mảnh ghép khớp với nhau như thế nào?

Bạn có thể hình dung hệ thống này giống như một **nhà máy lắp ráp**:
- **Nguyên vật liệu (Input)**: File CSV, Parquet nằm trên GCS.
- **Cỗ máy chính (Docker Image/Spark)**: Được build sẵn với bộ não là `Transformer.scala`. Nó biết cách đọc JSON, biết cách nối các ống nước lại với nhau.
- **Bản vẽ kỹ thuật (ConfigMap JSON)**: Quy định nguồn vào ở đâu, nguồn ra ở đâu, dùng lệnh SQL gì, hoặc dùng hàm Custom nào.
- **Quản đốc (Kubernetes Job)**: Đọc bản vẽ, nạp vào cỗ máy, bật cầu dao điện. Xong việc thì tự động dọn dẹp (nhờ `ttlSecondsAfterFinished`).
- **Băng chuyền (CI/CD)**: Tự động hoá việc gắn thẻ (Tag), xoá ông quản đốc cũ (vì Job K8s là bất biến) và gọi ông quản đốc mới.

### Phần 4: Tools & Methods
> Tại sao lại là bộ tool này?

- **Scala & Maven**: Scala là ngôn ngữ "mẹ đẻ" của Spark. Tránh được chi phí overhead (serialize/deserialize) nếu dùng PySpark với các logic UDF phức tạp.
- **K8s Job thay vì Deployment**: K8s Job thiết kế riêng cho tác vụ chạy-rồi-chết. Deployment sẽ cố gắng restart Spark ngay khi nó chạy xong (hiểu lầm là app bị crash), dẫn đến loop vô tận.
- **`envsubst` trong CI/CD**: Rất nhẹ, có sẵn trên hầu hết các môi trường Linux/Ubuntu runner. Tuy nhiên, phải dùng đúng cú pháp `envsubst '$IMAGE_TAG'` để nó không "vô tình" thay thế nhầm những chuỗi `${...}` khác trong ConfigMap.

### Phần 5: Tradeoffs
> Ta đã được gì và mất gì?

**Sự đánh đổi lớn nhất**: Complexity của file cấu hình.
Bằng cách gom mọi thứ vào ConfigMap, chúng ta được sự linh hoạt tuyệt đối (không cần rebuild code khi đổi SQL). Nhưng bù lại, nếu viết sai cú pháp JSON hoặc SQL trong ConfigMap, code sẽ không báo lỗi lúc compile (build image), mà chỉ "nổ" lúc chạy (runtime). Chúng ta đánh đổi **Compile-time safety** lấy **Operational agility (Tốc độ vận hành)**. Để bù đắp, tôi đã thiết kế thêm class `ETLConfig` có validate đầu vào ngay từ những giây đầu tiên job chạy.

### Phần 6: Mistakes & Dead Ends
> Sai lầm nào đã xảy ra và fix ra sao?

Một cái "ngõ cụt" thú vị là lỗi **Zombie Pods**. Lúc đầu cấu hình Job chạy xong thì nó báo `Completed`. Nghe thì hay, nhưng K8s không tự xoá nó đi. Cứ mỗi lần CI/CD đẩy lên là sinh thêm 1 pod, chạy chục lần là cái cluster rác đầy. Giải pháp cực kỳ thanh lịch nhưng dễ quên là thêm đúng 1 dòng `ttlSecondsAfterFinished: 300` vào YAML. 

Tiếp đến là **cú lừa Lazy Evaluation**. Ở file `Extractor`, ban đầu có dòng `logger.info(s"Extracted ${df.count()} records")`. Nghe rất vô hại đúng không? "Cho tôi biết anh đọc được bao nhiêu dòng". Nhưng trong Spark, `.count()` là một **Action**. Nó ép Spark phải đọc toàn bộ file từ GCS, đếm số dòng, rồi... vứt đó, sau đó qua bước Transform nó lại đọc lại từ đầu. Gấp đôi thời gian, gấp đôi tiền mạng egress. Sửa lại: chỉ dùng `df.schema.simpleString` để log.

### Phần 7: Future Pitfalls
> Cạm bẫy nào nên tránh về sau?

- **Cập nhật Kubernetes Job**: Nhớ kỹ câu thần chú: K8s Job là "bất biến" (Immutable). Bạn không thể `kubectl apply` đè một cái Job đã chạy để đổi image. Luôn luôn phải xoá cái cũ (`kubectl delete job ...`) rồi mới tạo cái mới.
- **System.exit(1)**: Đừng bao giờ gọi hàm này trong môi trường cluster như K8s hay Hadoop. Nó "rút phích cắm" cái rụp. Thay vì thế, hãy `throw Exception`. K8s/Spark có cơ chế bắt exception để dọn dẹp rác (Graceful shutdown) trước khi chết.

### Phần 8: Expert vs Beginner
> Chuyên gia sẽ nhìn thấy gì mà lính mới bỏ qua?

Lính mới sẽ chỉ quan tâm việc "Code chạy ra đúng data là được", và hay xài `df.printSchema()` hay `System.exit(1)`. 
Một expert (như những gì ta vừa tối ưu) sẽ quan tâm đến **Observability & Resource Management**:
- `printSchema()` in thẳng ra Standard Output thành hàng tá dòng, làm rác hệ thống log tập trung (như ELK, Datadog) và không lọc theo ERROR/INFO được. Expert dùng `logger.info(schema)` bằng SLF4J.
- Expert nhận thức sâu sắc về lazy evaluation: Họ biết chính xác hàm nào là Transformation (như `.filter`, `.withColumn`) và hàm nào là Action (`.count`, `.collect`). Họ không bao giờ gọi Action chỉ để "in log cho vui".

### Phần 9: Transferable Lessons
> Bài học nào có thể xách đi dùng cho dự án khác?

1. **Tách biệt "Cơ chế" (Mechanism) và "Chính sách" (Policy)**: Docker image là cơ chế (cách chạy), JSON config là chính sách (chạy cái gì). Tách được hai cái này ra, dự án của bạn sẽ sống rất thọ.
2. **Infrastructure is Immutable**: Đừng đối xử với container hay K8s Job như thú cưng (chăm bẵm, chữa bệnh). Hãy đối xử với chúng như gia súc — lỗi là bỏ, xoá đi tạo cái mới (cattle, not pets). CI/CD xoá job cũ tạo job mới chính là tư duy này.
3. **Mọi tài nguyên mạng đều có giá**: Đừng bắt máy tính tải hàng GB dữ liệu về chỉ để in ra số dòng. Phải luôn tôn trọng chi phí I/O trong dữ liệu lớn.
