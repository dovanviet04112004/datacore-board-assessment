# DataCore Board Assessment

## Tổng quan
Dự án thu thập và hợp nhất dữ liệu Ban lãnh đạo từ CafeF và Vietstock cho các công ty niêm yết Việt Nam.

**Kết quả cuối cùng:** 1503 golden records từ 33 mã cổ phiếu (HOSE + HNX)

---

## Hướng dẫn cài đặt 

### Yêu cầu hệ thống
- **Python:** 3.10 trở lên
- **Hệ điều hành:** Windows, Linux, hoặc macOS
- **Không cần browser/Selenium** - Cả 2 scraper đều dùng requests

### Cài đặt từng bước

**Bước 1: Clone repository**
```bash
git clone <repository-url>
cd datacore-board-assessment
```

**Bước 2: Tạo virtual environment**
```bash
# Windows:
py -3.11 -m venv venv

# Linux/Mac:
python3.11 -m venv venv
```

**Bước 3: Kích hoạt virtual environment**
```bash
# Windows (PowerShell):
.\venv\Scripts\Activate.ps1

# Windows (CMD):
venv\Scripts\activate.bat

# Linux/Mac:
source venv/bin/activate
```

**Bước 4: Cài đặt dependencies**
```bash
pip install -r requirements.txt
```

### Cấu hình (Tùy chọn)
Chỉnh sửa `config.yaml` để thay đổi:
- Danh sách mã cổ phiếu cần scrape
- Timeout và delay giữa các request
- Ngưỡng khớp mờ (fuzzy matching threshold)

---

## Cách chạy

**Chạy theo thứ tự Task 1 → Task 2 → Task 3**

### Task 1: Thu thập dữ liệu từ CafeF
```bash
# Windows:
py -3.11 src/scrape_cafef.py

# Linux/Mac:
python src/scrape_cafef.py
```
**Output:** `data/processed/cafef_processed.parquet` (~1630 records)

### Task 2: Thu thập dữ liệu từ Vietstock
```bash
# Windows:
py -3.11 src/scrape_vietstock.py

# Linux/Mac:
python src/scrape_vietstock.py
```
**Output:** `data/processed/vietstock_processed.parquet` (~478 records)

### Task 3: Merge dữ liệu
```bash
# Windows:
py -3.11 src/merge.py

# Linux/Mac:
python src/merge.py
```
**Output:** 
- `data/final/board_members_golden.parquet`
- `data/final/board_members_golden.csv`

### Chạy tất cả (One-liner)
```bash
# Windows:
py -3.11 src/scrape_cafef.py; py -3.11 src/scrape_vietstock.py; py -3.11 src/merge.py

# Linux/Mac:
python src/scrape_cafef.py && python src/scrape_vietstock.py && python src/merge.py
```

### Chạy Tests
```bash
# Windows:
py -3.11 -m pytest tests/ -v

# Linux/Mac:
python -m pytest tests/ -v
```

## Project Structure
```
datacore-board-assessment/
├── README.md                    # Setup, how to run, your approach
├── requirements.txt             # Python dependencies (pinned versions)
├── config.yaml                  # All configurable parameters
├── src/
│   ├── scrape_cafef.py          # Task 1 scraper
│   ├── scrape_vietstock.py      # Task 2 scraper
│   ├── merge.py                 # Task 3 merge logic
│   └── utils.py                 # Shared utilities
├── data/
│   ├── raw/                     # Raw scraped outputs
│   ├── processed/               # Cleaned individual source data
│   └── final/                   # Merged golden dataset
├── docs/
│   ├── data_dictionary.md       # Field definitions and metadata
│   └── data_quality_report.md   # Quality analysis
├── notebooks/                   # EDA or analysis notebooks
└── tests/                       # Unit tests
```

## Cách tiếp cận kỹ thuật

### Quyết định thiết kế chính

#### 1. Requests thay vì Selenium
**Trade-off:** Tốc độ và độ tin cậy vs. Khả năng xử lý JavaScript phức tạp

- **CafeF:** Trang dùng JavaScript để load data, nhưng phát hiện được AJAX API endpoint ẩn (`s.cafef.vn/Ajax/...`) → Dùng requests trực tiếp, nhanh hơn 10x so với Selenium
- **Vietstock:** Trang server-rendered, chỉ cần xử lý CSRF token qua `requests.Session()`
- **Kết quả:** Scrape 33 mã trong ~2 phút 

#### 2. CSRF Token Handling (Vietstock)
- **Vấn đề:** Vietstock yêu cầu `__RequestVerificationToken` và `ASP.NET_SessionId`
- **Giải pháp:** Dùng `requests.Session()` để tự động lấy và gửi cookies
- **Không cần:** Parse token thủ công từ HTML hay sử dụng Selenium

#### 3. Chiến lược Merge 3 giai đoạn
```
Giai đoạn 1: Khớp chính xác (ticker + name_normalized) → confidence = 1.0
Giai đoạn 2: Khớp mờ (similarity ≥ 0.85)            → confidence = score
Giai đoạn 3: Bản ghi không khớp                      → confidence = 0.5
```
**Lý do:** Tên người có thể khác nhau giữa 2 nguồn (dấu, viết tắt) → cần fuzzy matching

#### 4. Name Normalization
- Chuyển thành chữ thường
- Bỏ dấu tiếng Việt (unidecode)
- Xóa khoảng trắng thừa
- **Ví dụ:** "Nguyễn Văn A" → "nguyen van a"

#### 5. Board Type Classification
- **CafeF:** Phân loại theo header bảng (HĐQT, Ban GĐ, BKS)
- **Vietstock:** Phân loại theo chức vụ (CTHĐQT → HĐQT, TGĐ → Ban GĐ)
- **Ưu tiên:** Lấy board_type từ Vietstock vì chính xác hơn

### Cải tiến với thêm thời gian

1. **Parallel scraping:** Dùng `asyncio` + `aiohttp` để scrape nhiều ticker đồng thời
2. **Fuzzy matching nâng cao:** Sử dụng `rapidfuzz` hoặc `jellyfish` thay vì simple character matching
3. **Data validation:** Thêm schema validation với `pydantic` hoặc `pandera`
4. **Incremental scraping:** Chỉ scrape mã mới hoặc đã thay đổi
5. **Database storage:** Lưu vào SQLite/PostgreSQL thay vì file Parquet
6. **Monitoring:** Thêm metrics về số lượng records, thời gian scrape, tỷ lệ thành công

---

## Hạn chế 

### Ticker thất bại

| Ticker | Nguồn | Lý do | Ghi chú |
|--------|-------|-------|---------|
| DVD | Vietstock | Dữ liệu bị che `***` (Premium content) | Có dữ liệu từ CafeF (23 records) |

### Edge cases chưa xử lý
1. **Chức vụ mới:** Các chức vụ không có trong mapping → phân loại là "Khác"
2. **Dữ liệu lịch sử:** Chỉ lấy dữ liệu hiện tại, không theo dõi thay đổi qua thời gian
3. **Unicode edge cases:** Một số ký tự đặc biệt có thể không được normalize đúng

### Nguồn không truy cập được

1. **UPCOM:** Không có trong danh sách scrape (chỉ HOSE + HNX)
2. **API bị block:** Nếu scrape quá nhanh, có thể bị rate-limited (đã xử lý bằng delay 1.5s)
3. **Captcha:** Không xử lý captcha (hiện tại chưa gặp)

### Giới hạn dữ liệu

| Metric | Giá trị |
|--------|---------|
| Tổng ticker | 33 |
| CafeF records | 1630 |
| Vietstock records | 478 |
| Golden records | 1503 |
| Match rate (cả 2 nguồn) | 30.2% (454/1503) |
| Chỉ CafeF | 68.2% (1025/1503) |
| Chỉ Vietstock | 1.6% (24/1503) |

---

## Output Files

| File | Mô tả |
|------|-------|
| `data/raw/cafef_raw.parquet` | Dữ liệu thô từ CafeF |
| `data/raw/vietstock_raw.parquet` | Dữ liệu thô từ Vietstock |
| `data/processed/cafef_processed.parquet` | Dữ liệu CafeF đã xử lý |
| `data/processed/vietstock_processed.parquet` | Dữ liệu Vietstock đã xử lý |
| `data/final/board_members_golden.parquet` | **Golden dataset (Parquet)** |
| `data/final/board_members_golden.csv` | **Golden dataset (CSV)** |

---

## Dependencies

Xem chi tiết trong `requirements.txt`. Các thư viện chính:
- `requests` - HTTP client
- `beautifulsoup4` + `lxml` - HTML parsing
- `pandas` + `pyarrow` - Data processing và Parquet I/O
- `unidecode` - Vietnamese character normalization
- `pyyaml` - Config file parsing
- `pytest` - Testing

---

## Author

**TODO:** Do Van Viet
