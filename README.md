# Báo cáo thực hành Hadoop – Phân tích doanh thu đơn hàng TMĐT theo nhóm sản phẩm

- **Sinh viên:** Nguyễn Đức Huy  
- **MSSV:** A52164  
- **Môi trường:** Google Colab · Apache Hadoop 3.5.0 · Java 17 · Hadoop Streaming (Python 3)  
- **Dữ liệu:** `orders_2026_09.csv` – đơn hàng tháng 09/2026

## 1. Bài toán

Một sàn thương mại điện tử cần biết **nhóm sản phẩm (category) nào có tổng doanh thu cao nhất, chỉ tính các giao dịch `SUCCESS`**, để tối ưu tồn kho, khuyến mại và nguồn lực giao hàng.

Dữ liệu gồm **1.500 giao dịch**, 11 trường: `transaction_id, order_date, order_time, province, category, channel, quantity, unit_price, amount, status, shipping_days`.
Trạng thái: **1.240 SUCCESS**, 142 FAILED, 118 RETURNED. Chỉ SUCCESS được tính doanh thu.

Luồng xử lý: `Input CSV → Mapper → Shuffle/Sort → Reducer → Output`.

## 2. Cấu trúc thư mục nộp bài

| Đường dẫn | Nội dung |
|---|---|
| `src/mapper_revenue.py`, `src/reducer_revenue.py` | Mã Mapper và Reducer |
| `results/part-00000_hdfs.txt` | Kết quả MapReduce đọc từ HDFS (sản phẩm 03) |
| `results/part-00000_local.txt` | Kết quả chạy chế độ local (giống hệt bản HDFS) |
| `results/revenue_sorted.csv`, `results/top3.md` | Doanh thu sắp xếp giảm dần, bảng Top 3 (sản phẩm 04) |
| `logs/00_hdfs_evidence.txt` | Log minh chứng tạo thư mục HDFS, upload, `-ls`, `-head`, `-cat` (sản phẩm 01, 02) |
| `logs/01…05_*.txt` | Log đầy đủ: phiên bản Hadoop, job local, format NameNode, job trên HDFS, job trên YARN |
| `notebook/Hadoop_Ecommerce_Lab.ipynb` | Notebook Colab đã chạy, kèm kết quả |

## 3. Minh chứng 01 – Tạo thư mục HDFS và upload dữ liệu

```bash
hadoop fs -mkdir -p /user/student/ecommerce/input
hadoop fs -mkdir -p /user/student/ecommerce/output
hadoop fs -put -f orders_2026_09.csv /user/student/ecommerce/input/
```

HDFS chạy chế độ pseudo-distributed (1 NameNode + 1 DataNode); `jps` hiển thị `NameNode`, `DataNode`, và `hdfs dfsadmin -report` báo `Live datanodes (1)`.
Log đầy đủ: [`logs/00_hdfs_evidence.txt`](logs/00_hdfs_evidence.txt), [`logs/03_format.txt`](logs/03_format.txt).

## 4. Minh chứng 02 – Xem dữ liệu trên HDFS

```
$ hadoop fs -ls /user/student/ecommerce/input
Found 1 items
-rw-r--r--   1 root supergroup     121497 2026-10-10 09:43 /user/student/ecommerce/input/orders_2026_09.csv

$ hadoop fs -head /user/student/ecommerce/input/orders_2026_09.csv   (5 dòng đầu)
transaction_id,order_date,order_time,province,category,channel,quantity,unit_price,amount,status,shipping_days
TX000001,2026-09-30,02:25:00,Ha Noi,Beauty,web,1,294947,294947,SUCCESS,2
TX000002,2026-09-23,23:58:00,Da Nang,Grocery,mobile,1,265514,265514,SUCCESS,3
TX000003,2026-09-05,15:38:00,Ha Noi,Electronics,partner,1,12741238,12741238,SUCCESS,3
TX000004,2026-09-09,18:01:00,Hai Phong,Electronics,mobile,1,8846997,8846997,SUCCESS,4
```

## 5. Chạy job MapReduce (Hadoop Streaming) trên HDFS

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar \
  -D mapreduce.job.reduces=1 \
  -files mapper_revenue.py,reducer_revenue.py \
  -mapper "python3 mapper_revenue.py" \
  -reducer "python3 reducer_revenue.py" \
  -input  /user/student/ecommerce/input/orders_2026_09.csv \
  -output /user/student/ecommerce/output/revenue_by_category
```

Job kết thúc thành công (`completed successfully`). Các counter chính: `Map input records = 1501` (1.500 dòng dữ liệu + 1 dòng tiêu đề), `Map output records = 1240` (đúng số giao dịch SUCCESS), `Reduce input groups = 8` (8 category), `Reduce output records = 8`. Xem [`logs/04_streaming_hdfs.txt`](logs/04_streaming_hdfs.txt).

## 6. Minh chứng 03 – Kết quả `part-00000`

Thư mục output trên HDFS có `_SUCCESS` và `part-00000` (133 byte):

```
Beauty       111032003
Book          39306022
Electronics 3703769311
Fashion      218597763
Grocery       80983147
Home         410315065
Sport        166194354
Toy           83881779
```

Kết quả được kiểm chứng bằng 3 cách, cho kết quả trùng khớp: (1) chạy local `mapper | sort | reducer`, (2) Hadoop Streaming chế độ local, (3) đối chiếu với Pandas (`groupby`). Kết quả chạy trên HDFS và local giống hệt nhau. Job chạy trên YARN cũng hoàn thành thành công ([`logs/05_streaming_yarn.txt`](logs/05_streaming_yarn.txt)).

## 7. Minh chứng 04 – Top 3 nhóm sản phẩm có doanh thu cao nhất

Tổng doanh thu giao dịch SUCCESS: **4.814.079.444 VND**.

| Hạng | Category | Doanh thu (VND) | Tỷ trọng |
|:---:|---|---:|---:|
| 1 | Electronics | 3.703.769.311 | 76,9% |
| 2 | Home | 410.315.065 | 8,5% |
| 3 | Fashion | 218.597.763 | 4,5% |

**Nhận xét:**

1. Electronics dẫn đầu với hơn 3,7 tỷ VND, chiếm 76,9% tổng doanh thu, gấp khoảng 9 lần nhóm đứng thứ hai là Home.
2. Home (8,5%) và Fashion (4,5%) lần lượt đứng thứ hai và thứ ba, nhưng khoảng cách với Electronics là rất lớn.
3. Ba nhóm đầu cộng lại chiếm 90,0% doanh thu; năm nhóm còn lại (Sport, Beauty, Toy, Grocery, Book) chỉ chiếm khoảng 10%.
4. Doanh thu tập trung quá cao vào một nhóm nên có rủi ro phụ thuộc: nếu Electronics giảm cầu hoặc thiếu hàng, tổng doanh thu sẽ giảm mạnh.
5. Đề xuất: ưu tiên bảo đảm tồn kho và năng lực giao hàng cho Electronics; dùng khuyến mại để thúc đẩy Home và Fashion nhằm đa dạng hóa nguồn thu.
6. Lưu ý: kết quả chỉ tính giao dịch SUCCESS; 142 đơn FAILED và 118 đơn RETURNED đã bị loại, nên các nhóm có tỷ lệ trả hàng cao có thể cần được xem xét riêng.

## 8. Minh chứng 05 – Mô tả Mapper và Reducer

**Mapper** (`src/mapper_revenue.py`) đọc từng dòng CSV từ standard input. Nếu `status = SUCCESS`, Mapper phát sinh một cặp **key–value** dạng `category <TAB> amount`, ví dụ `Electronics	12741238`. Giao dịch FAILED/RETURNED bị bỏ qua nên không sinh cặp nào.

**Shuffle/Sort:** Hadoop gom và sắp xếp các cặp theo key, để mọi cặp cùng `category` nằm liền nhau và đến cùng một Reducer.

**Reducer** (`src/reducer_revenue.py`) duyệt các dòng đã sắp xếp, **cộng dồn `amount` theo từng category**; khi gặp key mới thì in ra `category <TAB> tổng doanh thu`. Kết quả cuối ghi vào `part-00000`.

## 9. Liên hệ HDFS – MapReduce – YARN

- **HDFS** lưu dữ liệu phân tán theo cây thư mục (`/user/student/ecommerce/...`), chia file thành block và nhân bản.
- **MapReduce** xử lý theo mô hình key–value: Map → Shuffle/Sort → Reduce.
- **YARN** quản lý tài nguyên (ResourceManager, NodeManager) và lập lịch các task của job.

## 10. Hạn chế của xử lý batch

Kết quả chỉ có sau khi toàn bộ job chạy xong, nên không phù hợp nhu cầu báo cáo thời gian thực; mỗi job cũng có chi phí khởi động đáng kể, kể cả với dữ liệu nhỏ như bài này (~120 KB).

## 11. Cách chạy lại

Mở `notebook/Hadoop_Ecommerce_Lab.ipynb` trên Google Colab, chạy lần lượt các ô từ trên xuống (cần upload `orders_2026_09.csv`, `mapper_revenue.py`, `reducer_revenue.py`).
Kiểm tra nhanh không cần Hadoop:

```bash
cat orders_2026_09.csv | python3 src/mapper_revenue.py | sort | python3 src/reducer_revenue.py
```
