# Project 37: Xây dựng nền tảng ODS thời gian thực phục vụ phân tích dữ liệu lớn
---
Mentor: Nguyễn Ngọc Long - VTS\
Email: longnn23@viettel.com.vn

Mentee: Lê Đình Đức - VDT\
Email: ledinhduc1879@gmail.com

---
## A. Yêu cầu của mini-project (phía Mentor)
### 1. Bài toán của đơn vị

Nhiều lĩnh vực như tài chính, viễn thông, thương mại điện tử, IoT hay monitoring hệ thống đều có nhu cầu xử lý và phân tích dữ liệu gần thời gian thực. Các hệ thống batch truyền thống khó đáp ứng yêu cầu realtime analytics với dữ liệu lớn. Đề tài hướng tới xây dựng nền tảng ODS realtime hỗ trợ ingest, xử lý và truy vấn dữ liệu phục vụ dashboard, monitoring và phân tích vận hành.


### 2. Mô tả

Trong các hệ thống dữ liệu hiện đại, ODS (Operational Data Store) đóng vai trò là tầng dữ liệu trung gian phục vụ lưu trữ, đồng bộ và phân tích dữ liệu gần thời gian thực từ nhiều nguồn khác nhau như transactional database, log system, API hoặc message queue.

Các hệ thống ODS truyền thống thường gặp hạn chế về khả năng xử lý dữ liệu lớn, realtime analytics và mở rộng hệ thống khi số lượng dữ liệu tăng nhanh. Với sự phát triển của các nền tảng xử lý dữ liệu phân tán và realtime OLAP, việc xây dựng một hệ thống ODS hiện đại phục vụ phân tích dữ liệu lớn đang trở thành nhu cầu phổ biến trong doanh nghiệp.

Đề tài hướng tới việc xây dựng một nền tảng ODS hỗ trợ ingest, xử lý và truy vấn dữ liệu gần thời gian thực phục vụ analytics và dashboarding. Học viên cần thiết kế pipeline dữ liệu, xây dựng hệ thống ingest từ nhiều nguồn dữ liệu khác nhau, tối ưu lưu trữ và đánh giá hiệu năng hệ thống trong các kịch bản thực tế.

Ngoài việc triển khai hệ thống dữ liệu, học viên cần nghiên cứu các vấn đề trong Data Engineering như CDC, stream processing, partitioning, schema design, realtime query processing và monitoring hệ thống. Đề tài không yêu cầu xây dựng production system hoàn chỉnh, tuy nhiên cần thể hiện khả năng thiết kế kiến trúc dữ liệu hiện đại, triển khai pipeline xử lý dữ liệu và đánh giá hiệu năng hệ thống.

Yêu cầu:
- Khái niệm ODS là gì ? So sánh ODS với Warehouse truyền thống, Data Lake (cả Lakehouse), sự chuyển dịch Data Warehouse kết hợp với ODS làm realtime ODS với kiến trúc phân tán.
- Tìm hiểu và so sánh Starrock, RisingWave, Databend, Doris, ClickHouse
- Nội dung tham khảo phải tham khảo từ nguồn uy tín (paper/article/tool documents). Tuyệt đối không giả tạo, bịa đặt nguồn tham khảo.

### 3. Các bước thực hiện

1. Nghiên cứu kiến trúc ODS và realtime analytics:
- Tìm hiểu mô hình ODS trong hệ sinh thái Big Data.
- Tìm hiểu các phương pháp ingest dữ liệu realtime và batch.
- Nghiên cứu kiến trúc distributed storage và distributed query processing.
2. Xây dựng pipeline ingest và xử lý dữ liệu:
- Sinh hoặc thu thập dữ liệu từ database, log hoặc API.
- Xây dựng pipeline ingest dữ liệu realtime.
- Thực hiện transform và xử lý dữ liệu phục vụ analytics.
3. Thiết kế và triển khai hệ thống ODS:
- Thiết kế schema dữ liệu và data model phục vụ phân tích.
- Tối ưu partitioning, indexing và storage layout.
- Triển khai hệ thống query phục vụ dashboard và analytics realtime.
4. Query, visualization và monitoring:
- Xây dựng dashboard trực quan hóa dữ liệu.
- Thực hiện các truy vấn analytics realtime.
- Theo dõi và giám sát hiệu năng hệ thống.
5. Benchmark và đánh giá hệ thống:
- Đo throughput ingest dữ liệu.
- Đánh giá query latency và khả năng scale hệ thống.
- So sánh hiệu năng giữa các kiến trúc hoặc công nghệ khác nhau.

### 4. Yêu cầu đầu ra

- Một hệ thống ODS hoạt động ổn định trên Docker hoặc Kubernetes.
- Pipeline ingest và xử lý dữ liệu realtime hoàn chỉnh.
- Dashboard analytics và monitoring cơ bản.
- Báo cáo benchmark về throughput, latency và khả năng mở rộng hệ thống.
- Demo ingest dữ liệu realtime và query analytics trực tiếp trên hệ thống ODS.
    + 1 slide trình bày kiến trúc hệ thống và pipeline dữ liệu.
    + 1 báo cáo kỹ thuật mô tả kiến trúc ODS, pipeline xử lý dữ liệu và kết quả đánh giá hệ thống.


### 5. Tài liệu tham khảo

- ODS: StarRocks, RisingWave, Databend, Apache Doris ...
- Streaming / Processing: Apache Flink, Apache Spark, Apache NiFi, ...
- Visualization / Monitoring: Grafana, Apache Superset,...

### 6. Các câu hỏi bổ sung:
- Chứng minh cũng như tìm hiểu cho anh là tại sao lại dùng ODS mà không dùng warehouse, hay lakehouse
- DB dùng để CDC là con nào?
- Tại sao click stream event vào nifi mà ko bắn luôn vào con kafka
- Hiện tại công ty cx đang dùng con Starrocks, nên em có thể kiếm một con khác để tăng tính mới, sáng tạo nhé. Dùng con đó cx oke, k sao.
- Cân nhắc đoạn monitor và so sánh với các tool khác vì anh nghĩ em chỉ kịp làm 1 trong 2 cái thôi (So sánh lý thuyết cũng được, hiện thực thì càng tốt)
- Bổ sung cho anh thêm một ít bảng nữa và dữ liệu nhiều nhiều chút nhé (cả source và ODS)



---
---
## B. Ý tưởng Hiện thực (phía Mentee)

Phần này em xin trình bày ý tưởng thiết kế kiến trúc, giải pháp công nghệ, mô hình lưu trữ và kế hoạch đánh giá hiệu năng hệ thống ODS thời gian thực phục vụ phân tích dữ liệu Ecommerce Clickstream. **Toàn bộ hệ thống pipeline và các dịch vụ lưu trữ trong ý tưởng này sẽ được đóng gói, cấu hình và triển khai hoàn toàn trên môi trường Docker (sử dụng Docker Compose) để phục vụ cho việc kiểm thử và đánh giá hiệu năng.**

### 1. Ý tưởng Luồng Dữ liệu
- **Clickstream Events:** Ý tưởng là giả lập hành vi người dùng trên trang thương mại điện tử (xem sản phẩm, thêm vào giỏ hàng, tìm kiếm, thanh toán) gửi về hệ thống dưới dạng các sự kiện thời gian thực để phân tích xu hướng mua sắm ngay trong phiên truy cập. Ứng dụng sinh dữ liệu này sẽ dựa trên việc **fork và cải tiến** script [data_generator.py](https://github.com/kuldeep27396/clickstream-datagenerator/blob/main/data_generator.py) từ dự án của tác giả khác để phù hợp hơn với nhu cầu kiểm thử hiệu năng và cấu trúc dữ liệu của hệ thống.
- **CDC (Change Data Capture):** Để phân tích chuyên sâu, clickstream cần được đối chiếu, làm giàu thông tin với dữ liệu sản phẩm (Products) và người dùng (Users). Ý tưởng là sử dụng giải pháp CDC để bắt tức thời các sự kiện thay đổi (Insert, Update, Delete) trên cơ sở dữ liệu giao dịch (OLTP DB) và đồng bộ chúng xuống ODS, phục vụ các câu truy vấn JOIN thời gian thực.


---

### 2. Ý tưởng thiết kế các thành phần Pipeline

#### 2.1. Tầng Thu thập & Đệm (Ingestion & Buffering)
- **Apache NiFi:** Đóng vai trò là cổng tiếp nhận (Gateway) cho luồng clickstream giả lập qua giao thức HTTP, kiểm tra định dạng sơ bộ và chuyển tiếp bất đồng bộ vào Kafka.
- **Debezium:** Đóng vai trò giám sát và CDC dữ liệu nghiệp vụ. Debezium đọc log ghi thay đổi (Binlog/WAL) của Transactional Database để tự động bắt các hành động thay đổi dữ liệu của bảng `products` và `users` rồi đẩy thành các bản tin sự kiện CDC vào Kafka. Cơ chế này giúp thu thập dữ liệu gần như tức thời mà không gây ảnh hưởng đến hiệu năng hoạt động của DB nghiệp vụ.
- **Apache Kafka:** Làm hàng đợi đệm chịu tải. Luồng clickstream được phân vùng theo `session_id` để đảm bảo thứ tự tuyến tính; luồng CDC của database được phân vùng theo khóa chính của bảng để bảo toàn thứ tự các sự kiện cập nhật của từng bản ghi.

#### 2.2. Tầng Xử lý Dòng (Stream Processing)
- **Spark Structured Streaming:** Nhận đồng thời luồng clickstream và các luồng CDC từ Kafka theo các micro-batches.
- Ý tưởng xử lý bao gồm:
  - Parse JSON sự kiện clickstream (lọc bỏ dữ liệu lỗi, cấu hình watermark và loại bỏ trùng lặp sự kiện).
  - Parse các bản tin CDC để nhận diện hành động (Insert/Update/Delete) và trích xuất thông tin mới nhất.
  - Sử dụng các connector tương ứng để đẩy dữ liệu đã chuẩn hóa xuống ODS một cách hiệu quả nhất.

---

### 3. Ý tưởng thiết kế lưu trữ ODS (StarRocks)

StarRocks được định hướng làm ODS nhờ khả năng xử lý truy vấn OLAP phân tán mạnh mẽ và hỗ trợ đa dạng mô hình bảng dữ liệu.

#### 3.1. Thiết kế Mô hình Dữ liệu (Data Modeling)
Dự kiến xây dựng các nhóm bảng lưu trữ gồm:
- **Bảng Chi tiết Sự kiện (Detail Table):** Lưu trữ clickstream thô dùng mô hình `Duplicate Key` tối ưu cho ghi append-only thần tốc.
- **Bảng Danh mục & Thông tin (Dimension Tables):** Lưu trữ danh mục người dùng (`dim_users`) và sản phẩm (`dim_products`) dùng mô hình **Primary Key Table** nhằm hỗ trợ cơ chế ghi đè, cập nhật và xóa (Upsert/Delete) trực tiếp từ luồng sự kiện CDC của Debezium.
- **Bảng Tổng hợp Thời gian thực (Aggregate Table):** Sử dụng mô hình `Aggregate Key` để tự động tích lũy số liệu tổng hợp theo khung thời gian cụ thể.

#### 3.2. Phương án Tối ưu hóa Hiệu năng (Performance Optimization)
- **Phân vùng (Partitioning & Bucketing):** Chia nhỏ bảng chi tiết theo ngày (`event_date`) để tối ưu hóa việc quản lý vòng đời dữ liệu; băm dữ liệu theo `session_id` để phân tán tải đều trên các cluster nodes.
- **Chỉ mục phụ (Indexing):** Sử dụng **Bitmap Index** cho các trường phân loại (thiết bị, loại sự kiện) và **Bloom Filter Index** cho các trường có độ phân tán cao để rút ngắn thời gian lọc.
- **Real-time Query Join:** Tận dụng tối đa bộ tối ưu hóa truy vấn của StarRocks để JOIN trực tiếp bảng sự kiện lớn (Duplicate Key) với các bảng Dimension thời gian thực (Primary Key) tại thời điểm query. Giải pháp này giúp đơn giản hóa pipeline ở tầng xử lý dòng (Spark không cần làm static-join phức tạp) và đảm bảo thông tin Dimension trên dashboard luôn là mới nhất.

---

### 4. Ý tưởng trực quan hóa và giám sát hệ thống

#### 4.1. Dashboard Phân tích (Apache Superset)
Kết nối trực tiếp tới StarRocks để lấy dữ liệu phân tích. Dashboard sẽ tập trung vào các biểu đồ tự động cập nhật định kỳ:
- Biểu đồ phễu chuyển đổi (Conversion Funnel) từ lúc xem hàng đến khi mua hàng thành công.
- Xu hướng lượng truy cập và hành vi người dùng theo thời gian thực.
- Top danh mục và sản phẩm đang nhận được nhiều tương tác nhất.

#### 4.2. Giám sát Vận hành (Grafana & Prometheus)
Xây dựng một góc nhìn tập trung về trạng thái hệ thống:
- Theo dõi độ trễ tiêu thụ (Consumer Lag) của Kafka để đảm bảo hệ thống xử lý kịp lượng dữ liệu đổ về.
- Giám sát thời gian hoàn thành micro-batch của Spark nhằm phát hiện sớm nghẽn cổ chai.
- Đo tài nguyên CPU/RAM tiêu thụ và hiệu năng truy vấn của StarRocks.

---

### 5. Ý tưởng Benchmark và Đánh giá hệ thống

#### 5.1. Kịch bản Đánh giá Thực nghiệm (Docker-based Benchmark)
Hệ thống sẽ chạy thử nghiệm trên môi trường ảo hóa Docker (cấu hình các container thông qua Docker Compose, đồng thời giới hạn cứng tài nguyên CPU/RAM của từng service như Spark, Kafka, StarRocks) nhằm đánh giá khách quan hiệu năng trong điều kiện tài nguyên giới hạn. Các mục tiêu kiểm thử bao gồm:
- **Benchmark Throughput:** Tăng dần tốc độ sinh dữ liệu giả lập để xác định ngưỡng chịu tải tối đa của pipeline ghi nhận dữ liệu.
- **Benchmark Query Latency:** Chạy các câu lệnh SQL phân tích phức tạp đồng thời trong khi luồng ingestion đang nạp dữ liệu liên tục nhằm đánh giá độ trễ truy vấn dưới tải cao.
- **Độ trễ dòng dữ liệu (End-to-End Latency):** Đo thời gian trung bình từ khi sự kiện click phát sinh ở client giả lập cho đến khi nó xuất hiện trên dashboard.

#### 5.2. Đánh giá và So sánh Lý thuyết (Theoretical Analysis)
Báo cáo sẽ phân tích và so sánh các kiến trúc để làm rõ tính đúng đắn của việc lựa chọn công nghệ:
- So sánh StarRocks với Apache Doris, RisingWave và Databend.
- So sánh Spark Structured Streaming (xử lý micro-batch) với Apache Flink (xử lý event-by-event thực thụ).

