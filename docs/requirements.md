# Project 37: Xây dựng nền tảng ODS thời gian thực phục vụ phân tích dữ liệu lớn

## 1. Bài toán của đơn vị (nêu sự cần thiết phải nghiên cứu) 

Nhiều lĩnh vực như tài chính, viễn thông, thương mại điện tử, IoT hay monitoring hệ thống đều có nhu cầu xử lý và phân tích dữ liệu gần thời gian thực. Các hệ thống batch truyền thống khó đáp ứng yêu cầu realtime analytics với dữ liệu lớn. Đề tài hướng tới xây dựng nền tảng ODS realtime hỗ trợ ingest, xử lý và truy vấn dữ liệu phục vụ dashboard, monitoring và phân tích vận hành.


## 2. Mô tả

Trong các hệ thống dữ liệu hiện đại, ODS (Operational Data Store) đóng vai trò là tầng dữ liệu trung gian phục vụ lưu trữ, đồng bộ và phân tích dữ liệu gần thời gian thực từ nhiều nguồn khác nhau như transactional database, log system, API hoặc message queue.

Các hệ thống ODS truyền thống thường gặp hạn chế về khả năng xử lý dữ liệu lớn, realtime analytics và mở rộng hệ thống khi số lượng dữ liệu tăng nhanh. Với sự phát triển của các nền tảng xử lý dữ liệu phân tán và realtime OLAP, việc xây dựng một hệ thống ODS hiện đại phục vụ phân tích dữ liệu lớn đang trở thành nhu cầu phổ biến trong doanh nghiệp.

Đề tài hướng tới việc xây dựng một nền tảng ODS hỗ trợ ingest, xử lý và truy vấn dữ liệu gần thời gian thực phục vụ analytics và dashboarding. Học viên cần thiết kế pipeline dữ liệu, xây dựng hệ thống ingest từ nhiều nguồn dữ liệu khác nhau, tối ưu lưu trữ và đánh giá hiệu năng hệ thống trong các kịch bản thực tế.

Ngoài việc triển khai hệ thống dữ liệu, học viên cần nghiên cứu các vấn đề trong Data Engineering như CDC, stream processing, partitioning, schema design, realtime query processing và monitoring hệ thống. Đề tài không yêu cầu xây dựng production system hoàn chỉnh, tuy nhiên cần thể hiện khả năng thiết kế kiến trúc dữ liệu hiện đại, triển khai pipeline xử lý dữ liệu và đánh giá hiệu năng hệ thống.


## 3. Các bước thực hiện


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
- So sánh hiệu năng giữa các kiến trúc hoặc công nghệ khác nhau."

## 4. Yêu cầu đầu ra

- Một hệ thống ODS hoạt động ổn định trên Docker hoặc Kubernetes.
- Pipeline ingest và xử lý dữ liệu realtime hoàn chỉnh.
- Dashboard analytics và monitoring cơ bản.
- Báo cáo benchmark về throughput, latency và khả năng mở rộng hệ thống.
- Demo ingest dữ liệu realtime và query analytics trực tiếp trên hệ thống ODS.
    + 1 slide trình bày kiến trúc hệ thống và pipeline dữ liệu.
    + 1 báo cáo kỹ thuật mô tả kiến trúc ODS, pipeline xử lý dữ liệu và kết quả đánh giá hệ thống.


## 5. Tài liệu tham khảo

- ODS: StarRocks, RisingWave, Databend, Apache Doris ...
- Streaming / Processing: Apache Flink, Apache Spark, Apache NiFi, ...
- Visualization / Monitoring: Grafana, Apache Superset,...
