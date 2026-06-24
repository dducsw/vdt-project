# BÁO CÁO ĐÁNH GIÁ PROJECT — Nền tảng ODS Thời gian thực

> **Kết luận tổng quan:** Project đạt chất lượng **TỐT** và về cơ bản đáp ứng đầy đủ các yêu cầu đề ra. Kiến trúc thực tế khác so với ý tưởng ban đầu (không dùng NiFi, chuyển từ StarRocks sang Doris) nhưng theo hướng tốt hơn — phản ánh tư duy engineering chín chắn. Các điểm yếu chủ yếu nằm ở độ sâu benchmark và vài thiếu sót kỹ thuật nhỏ.

---

## I. TỔNG QUAN KIẾN TRÚC THỰC HIỆN

```
PostgreSQL (OLTP) → Debezium CDC → Kafka → Doris Routine Load → dim/fact tables
Python Datagen (Clickstream) → Kafka → Spark Structured Streaming → dwd_clickstream_events
                                                                           ↓
                                                           Apache Doris (ODS) — DWS Views
                                                                           ↓
                                                      Apache Superset (Dashboard Analytics)
                                                      Grafana + Prometheus + Kafka Exporter (Monitoring)
```

**Stack thực tế triển khai:**
| Thành phần | Công nghệ | Ghi chú |
|---|---|---|
| OLTP Source | PostgreSQL 16 | WAL logical replication |
| CDC | Debezium | PostgreSQL connector |
| Message Queue | Kafka (KRaft, 2 brokers) | Confluent CP 7.7.0 |
| Stream Processing | PySpark Structured Streaming 3.5.6 | Micro-batch 5s |
| ODS | Apache Doris 4.1.1 | 1 FE + 2 BE |
| Visualization | Apache Superset | MySQL protocol connector |
| Monitoring | Grafana + Prometheus + Kafka Exporter | |
| Data Generator | Python (Faker + confluent-kafka) | 10 events/s |
| Deployment | Docker Compose | Tất cả service containerized |

---

## II. ĐÁNH GIÁ THEO TỪNG YÊU CẦU

---

### 2.1 Xây dựng Pipeline Ingest & Xử lý Dữ liệu

#### ✅ Sinh/Thu thập dữ liệu từ database, log hoặc API
- **Nguồn 1 – OLTP Database (CDC):** PostgreSQL với dataset TheLook eCommerce (~930k rows: users, products, orders, order_items, inventory_items, distribution_centers). Debezium đọc WAL binlog, capture đúng Insert/Update/Delete, đẩy vào 6 Kafka topic riêng. **Chuẩn production.**
- **Nguồn 2 – Clickstream Simulated Events:** Python datagen dùng Faker sinh sự kiện tương tác user (view, cart, purchase, cancel, return) publish 10 events/s vào Kafka. Có đủ các loại event để phục vụ funnel analysis.
- **Đánh giá:** Đáp ứng đủ yêu cầu. Có **2 nguồn dữ liệu khác nhau** (OLTP CDC + behavioral event stream). Điểm trừ nhỏ: chỉ 18,048 clickstream events (hơi ít để demo scale thật sự), và không có nguồn API thứ ba như yêu cầu mô tả.

#### ✅ Xây dựng pipeline ingest dữ liệu realtime
- **Luồng CDC:** Debezium → Kafka → **Doris Routine Load** (không qua Spark). Đây là quyết định kiến trúc đúng đắn — Routine Load có built-in consumer, tự xử lý offset, không cần JVM overhead của Spark cho CDC đơn giản. Load rate thực đo: 202 rows/s (inventory), 75 rows/s (order_items), 0% error.
- **Luồng Clickstream:** Datagen → Kafka → **PySpark Structured Streaming** → Doris Stream Load. Trigger 5s micro-batch. Đủ để gọi là "near-realtime".
- **Đánh giá:** **Rất tốt.** Thiết kế "dual-path" khéo léo — Spark chỉ xử lý clickstream cần transformation phức tạp (parse URI, dedup, watermark), còn CDC đơn giản thì delegate cho Routine Load. Đây là pattern enterprise thực tế.

#### ✅ Transform & xử lý dữ liệu phục vụ analytics
- Parse URI → trích xuất `page_type` và `product_id` (product/123 → page_type="product", product_id=123).
- Watermark 10 phút + `dropDuplicates(["id", "event_timestamp"])` để đảm bảo xử lý sự kiện late-arrival.
- Dead-letter queue (`ods_clickstream_deadletter`) cho invalid events.
- CDC: timestamp microsecond → `from_unixtime(ts / 1000000)` conversion đúng PostgreSQL WAL format.
- **Đánh giá:** Tốt. Tuy nhiên, transform còn **khá đơn giản** — chủ yếu là schema mapping và URI parsing. Không có enrichment phức tạp hơn (e.g., user segmentation, session stitching) ở tầng Spark. Nhưng điều này hợp lý vì project chọn triết lý **Transform-on-Query** (đẩy tính toán sang Doris Views), đây là valid architectural choice.

---

### 2.2 Thiết kế và Triển khai Hệ thống ODS

#### ✅ Thiết kế schema dữ liệu và data model
**8 bảng vật lý** trong `thelook_dw`:

| Bảng | Model | Mục đích |
|---|---|---|
| `dwd_clickstream_events` | Duplicate Key | Append-only clickstream (DWD layer) |
| `ods_clickstream_deadletter` | Duplicate Key | Dead-letter queue |
| `dim_users` | Unique Key (MoW) | Dimension từ CDC, support Upsert/Delete |
| `dim_products` | Unique Key (MoW) | Dimension từ CDC |
| `dim_distribution_centers` | Unique Key (MoW) | Dimension từ CDC |
| `fact_orders` | Unique Key (MoW) | Fact từ CDC |
| `fact_order_items` | Unique Key (MoW) | Fact từ CDC |
| `fact_inventory_items` | Unique Key (MoW) | Fact từ CDC |

**4 DWS Views (không phải materialized tables):**
- `dws_clickstream_window_agg`: Aggregate theo window 5 phút.
- `dws_clickstream_sessions`: Session-level aggregation với top category.
- `dws_sales_performance_flat`: Multi-dim JOIN flat view.
- `dws_sales_overview_hourly`: Hourly sales KPI.
- *(Bonus: `dws_product_performance`, `dws_inventory_details`)*

**Đánh giá:** Data model **tốt và đúng chuẩn**. Phân tầng ODS rõ ràng: dim/fact (từ CDC) + dwd (từ clickstream) + dws (analytic views). Dùng đúng Table Model của Doris: Duplicate Key cho append-only, Unique Key + MoW cho upsertable CDC.

> [!WARNING]
> **Điểm yếu:** DWS Layer dùng **VIEW thường** thay vì **Materialized View** hoặc **Aggregate Table**. Mỗi lần query sẽ tính toán lại từ đầu trên raw data. Với dataset lớn thực tế, `dws_clickstream_sessions` có CTE lồng + ROW_NUMBER window function sẽ rất chậm. Trong benchmark, CS-5 (Session view) đã cho thấy điều này: avg 35.6ms, P99 153.8ms — chênh lệch rất lớn so với các query khác (1-5ms).

#### ✅ Tối ưu partitioning, indexing và storage layout
- **Dynamic Partitioning** theo `event_date` (DAY): tự tạo partition mới, thu hồi partition cũ (7 ngày), đúng chuẩn time-series data.
- **Hash Bucketing** theo `session_id` (clickstream) và `user_id`, `product_id`, `order_id` (CDC tables): phân tán đều, tối ưu co-location join.
- **Bloom Filter Index** trên `session_id`, `user_id`: bỏ qua block I/O không cần scan.
- **Inverted Index** trên `event_type`: full-text/keyword lookup nhanh.
- **Prefix Index**: built-in Doris theo sort key (Duplicate Key columns).
- **Colocate Join** (đề cập trong docs): bảng lớn cùng bucket strategy để local join.
- **Đánh giá:** Đầy đủ và đúng. Tuy nhiên, **Bitmap Index** (đề cập trong idea.md) không xuất hiện trong DDL thực tế — có thể đã bị bỏ vì Doris 4.x dùng Inverted Index thay thế tốt hơn. Cần giải thích rõ quyết định này trong báo cáo.

#### ✅ Triển khai hệ thống query phục vụ dashboard và analytics realtime
- 4 DWS Views làm abstraction layer cho Superset queries.
- 4 analytics queries trong `query.sql` (Conversion Funnel, Ingestion Latency, Trending Products, Hourly Sales).
- Doris kết nối Superset qua MySQL protocol port 9030.
- **Đánh giá:** Đủ để demo dashboard analytics. Các query có logic analytics thực tế (funnel, trending, profit margin).

---

### 2.3 Query, Visualization và Monitoring

#### ✅ Dashboard trực quan hóa dữ liệu
- Apache Superset: 3 dashboard có ảnh demo trong `dashboard/assets/`.
  - `superset1.png`: eCommerce Overview.
  - `superset2.png`: Clickstream Funnel.
  - `superset3.png`: Product & Sales Performance.
- **Đánh giá:** Có đủ dashboard, có ảnh minh chứng. Thiếu thông tin về cấu hình refresh interval, auto-refresh hay manual.

#### ✅ Truy vấn analytics realtime
- Benchmark thực đo: CS-1 đến CS-5 (Clickstream), CDC-1 đến CDC-5 (OLAP JOIN).
- Query latency: **1.5ms – 4.7ms** cho hầu hết queries (rất tốt).
- Multi-table JOIN (e.g., fact_order_items + dim_products + dim_users + fact_orders): avg 3.6ms.
- **Đánh giá:** Kết quả query rất ấn tượng với hardware giới hạn trong Docker.

#### ⚠️ Theo dõi và giám sát hiệu năng hệ thống
- **Grafana + Prometheus + Kafka Exporter**: Đã triển khai, có dashboard với các metrics:
  - Kafka broker count, consumer lag by group/topic.
  - Kafka message ingestion rate.
  - Doris FE JVM memory, BE allocated memory.
  - Doris query count và query errors.
- **Vấn đề đã ghi nhận trong result.txt:** "Total Queries (24) & Query Errors (24)" — tất cả 24 queries đều lỗi trong giai đoạn khởi tạo. Điều này cho thấy Prometheus scraping Doris metrics chưa được cấu hình đúng hoặc Doris chưa expose metrics endpoint đúng format khi mới boot.
- **Consumer Lag (5,244 messages)**: Lag còn cao, Spark micro-batch chưa kịp process trong thời điểm đo.
- **Đánh giá:** Monitoring infrastructure đúng hướng, nhưng **còn vài chỉ số chưa chính xác** (24/24 query errors). Cần fix và giải thích rõ trong báo cáo.

---

### 2.4 Benchmark & Đánh giá hệ thống

#### ⚠️ Đo throughput ingest dữ liệu
- CDC Routine Load: đo được tốc độ từng job (12-202 rows/s), lag = 0, error = 0%. **Tốt.**
- **Vấn đề:** Clickstream throughput = **0.00 events/sec** trong cả 3 samples! Script benchmark đo `count(event_id)` trong window 5s nhưng datagen đã kết thúc trước khi benchmark chạy, hoặc Spark không ingesting trong thời điểm đó. E2E lag (13.12s avg, P99 139.83s) được tính trên 5,000 historical events nhưng **không đo real-time throughput thực sự**.
- **Đây là điểm yếu lớn nhất:** Benchmark throughput clickstream không có kết quả có ý nghĩa vì datagen không chạy song song với benchmark script.

#### ✅ Đánh giá query latency
- Single query: 1.5ms – 35.6ms (rất tốt cho containerized environment).
- Concurrency test: Clickstream đạt 683.8 QPS (concurrency 3), CDC/OLAP đạt 1,313.4 QPS (concurrency 5). **Rất ấn tượng.**
- P99 tăng mạnh ở concurrency cao (cs-query P99 tăng 1827% từ conc 1→5) — đây là warning hợp lý, chứng tỏ script benchmark phát hiện resource contention đúng.

#### ❌ Khả năng scale hệ thống
- **Không có benchmark scale-out.** Hệ thống hiện tại cố định ở 1FE+2BE. Không có kịch bản thêm BE node để đo horizontal scaling effect.
- **Không so sánh với kiến trúc khác.** idea.md đề xuất so sánh Spark Streaming vs Flink, hoặc Doris vs StarRocks — nhưng chưa thực hiện (chỉ nêu lý thuyết).

---

## III. ĐỐI CHIẾU Ý TƯỞNG vs THỰC HIỆN

| Ý tưởng (idea.md) | Thực tế | Đánh giá |
|---|---|---|
| ODS dùng StarRocks | **Chuyển sang Apache Doris** | ✅ Tốt hơn — Doris native CDC tốt hơn, cộng đồng lớn hơn |
| NiFi làm HTTP gateway clickstream | **Bỏ NiFi, datagen push thẳng vào Kafka** | ✅ Đúng — NiFi thừa, tốn tài nguyên |
| Spark xử lý cả CDC lẫn clickstream | **Spark chỉ xử lý clickstream, CDC → Doris Routine Load** | ✅ Kiến trúc tối ưu hơn |
| StarRocks Aggregate Key Table | **Doris Unique Key + Views thường** | ⚠️ Views thay Materialized View là compromise |
| Colocate Join | Đề cập trong docs nhưng không thấy DDL PROPERTIES | ❓ Chưa rõ đã implement chưa |
| Bitmap Index | **Dùng Inverted Index thay thế** | ✅ Đúng — Doris 4.x recommend Inverted Index |
| Benchmark throughput tăng dần | **Chỉ đo static, không có load test scenarios** | ❌ Thiếu |
| So sánh Spark vs Flink | **Chỉ so sánh lý thuyết trong docs** | ⚠️ Thiếu thực nghiệm |

---

## IV. ĐIỂM MẠNH NỔI BẬT

1. **Thiết kế dual-path ingestion thông minh:** Phân biệt rõ luồng CDC (→ Routine Load) vs clickstream (→ Spark → Stream Load). Không over-engineer với Spark cho mọi thứ.
2. **Doris Table Model đúng:** Dùng đúng Duplicate Key cho append-only, Unique Key + MoW cho CDC upsertable data. Đây là điểm quan trọng thường bị làm sai.
3. **Dead-letter Queue pattern:** Tách luồng invalid events ra `ods_clickstream_deadletter` — pattern production standard.
4. **Watermark + Deduplication:** Xử lý late-arrival events và exactly-once semantics đúng cách.
5. **Observability stack đầy đủ:** Prometheus + Grafana + Kafka Exporter + Superset — monitoring end-to-end.
6. **Benchmark script tự động hóa hoàn toàn:** `demo/benchmark.py` kết nối Doris, đo multi-round, tính P50/P95/P99, measure concurrency — professional.
7. **Query performance thực sự ấn tượng:** Sub-5ms cho OLAP JOIN queries trên hardware giới hạn.

---

## V. ĐIỂM CẦN CẢI THIỆN (THEO MỨC ĐỘ ƯU TIÊN)

### 🔴 Cao — ảnh hưởng đến kết quả đánh giá

1. **Benchmark throughput clickstream = 0**: Cần chạy datagen *đồng thời* với benchmark script, hoặc thiết kế kịch bản load test riêng (e.g., tăng `PUBLISH_RATE_HZ` từ 10 → 100 → 1000 và đo Kafka consumer lag, Spark batch time).

2. **24/24 Doris query errors trong Grafana**: Cần debug và fix Prometheus scraping endpoint của Doris, hoặc giải thích rõ đây là lỗi trong giai đoạn cold-start và không ảnh hưởng production metrics.

3. **Consumer Lag cao (5,244 messages)**: Cần giải thích rõ context — lag này xảy ra ở thời điểm nào? Khi system cold-start? Khi datagen burst? Cần thêm kịch bản "lag recovery" trong benchmark.

### 🟡 Trung bình — nâng cao chất lượng

4. **DWS Views → nên có ít nhất 1 Materialized View**: Để demo khả năng pre-aggregation của Doris, có thể materialized view `mv_hourly_clickstream` với `REFRESH ASYNC` để thấy rõ lợi thế OLAP.

5. **Benchmark scale-out thiếu**: Thêm kịch bản "add BE node" và đo lại query latency, hoặc ít nhất thực hiện load test ở các mức `PUBLISH_RATE_HZ` khác nhau.

6. **Colocate Join chưa rõ**: Nếu đã implement, cần thêm `PROPERTIES ("colocate_with" = "group_name")` vào DDL. Nếu chưa, nên remove khỏi docs.

### 🟢 Nhỏ — polish

7. **Clickstream data volume**: 18,048 events khá ít. Nên có kịch bản chạy 30 phút để tích lũy ~180,000+ events trước khi chạy benchmark.

8. **Superset dashboard refresh**: Thêm cấu hình auto-refresh (e.g., 30s) và ghi rõ trong docs.

9. **CDC pipeline không xử lý `payload.before` cho UPDATE**: Routine Load chỉ lấy `payload.after.*`. Đây là đúng cho scenario upsert, nhưng nếu cần audit trail thì cần bổ sung.

---

## VI. KẾT LUẬN VÀ ĐIỂM SỐ TỔNG HỢP

| Tiêu chí | Điểm | Nhận xét |
|---|---|---|
| Pipeline ingest & transform | **8.5/10** | Dual-path design tốt, CDC đúng, Spark watermark/dedup đúng |
| Thiết kế ODS schema & partitioning | **8/10** | Table model đúng, indexing đầy đủ; DWS Views thay vì Materialized View là trade-off |
| Dashboard & visualization | **7.5/10** | Có đủ charts, thiếu auto-refresh và chi tiết cấu hình |
| Monitoring & observability | **7/10** | Stack đúng hướng, nhưng còn query errors chưa resolve |
| Benchmark & đánh giá | **6/10** | Query latency tốt, throughput clickstream = 0 là điểm trừ lớn; thiếu scale-out test |
| **Tổng thể** | **7.4/10** | Hệ thống hoạt động, kiến trúc đúng, cần cải thiện benchmark |

> [!NOTE]
> Project đã hoàn thiện phần lớn yêu cầu và thể hiện hiểu biết kỹ thuật tốt về Data Engineering. Các điểm yếu chủ yếu là benchmark chưa đầy đủ và một số chỉ số monitoring chưa chính xác — hoàn toàn có thể khắc phục trong thời gian ngắn.

