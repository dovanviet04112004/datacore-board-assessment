# DataCore Board Assessment

## Overview
Project for scraping and merging board member data from CafeF and Vietstock.

## Setup

### Prerequisites
- Python 3.11+

**Note:** No browser required - both scrapers use API/requests.

### Installation
```bash
# Create virtual environment
# Windows:
py -3.11 -m venv venv
# Linux/Mac:
python3.11 -m venv venv

# Activate virtual environment
# Windows (PowerShell):
.\venv\Scripts\Activate.ps1
# Windows (CMD):
venv\Scripts\activate.bat
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## How to Run

**Chạy theo thứ tự:** Task 1 → Task 2 → Task 3

### Task 1: Scrape CafeF
```bash
# Windows:
py -3.11 src/scrape_cafef.py
# Linux/Mac:
python src/scrape_cafef.py
```

### Task 2: Scrape Vietstock
```bash
# Windows:
py -3.11 src/scrape_vietstock.py
# Linux/Mac:
python src/scrape_vietstock.py
```

### Task 3: Merge Data
```bash
# Windows:
py -3.11 src/merge.py
# Linux/Mac:
python src/merge.py
```

### Run Tests
```bash
# Windows:
py -3.11 -m pytest tests/
# Linux/Mac:
python -m pytest tests/
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

## Approach

### Task 1: CafeF Scraper
- **URL Pattern:** `https://cafef.vn/du-lieu/{exchange}/{TICKER}-ban-lanh-dao-so-huu.chn`
- **Technology:** `requests` với AJAX API endpoint
- **API Endpoint:** `https://s.cafef.vn/Ajax/CongTy/BanLanhDao.aspx?sym={ticker}`
- **Why API:** Trang dùng JavaScript load data, nhưng phát hiện API endpoint ẩn → không cần Selenium
- **Data Structure:** HTML table với các section (HĐQT, Ban GĐ, BKS, Khác)
- **Output:** 1630 records từ 33 tickers

### Task 2: Vietstock Scraper
- **URL Pattern:** `https://finance.vietstock.vn/{TICKER}/ban-lanh-dao.htm`
- **Technology:** `requests.Session()` với auto CSRF token handling
- **Why requests:** Trang server-rendered, không cần JavaScript
- **CSRF Solution:** Session cookies (`ASP.NET_SessionId`, `__RequestVerificationToken`) tự động lấy khi truy cập homepage
- **Data Structure:** Một bảng chứa tất cả thành viên, role xác định loại ban
- **Additional Fields:** year_of_birth, education, shares, tenure_since
- **Output:** 478 records từ 32 tickers

### Task 3: Merge
- **Strategy:** Khớp 3 giai đoạn (chính xác → mờ → không khớp)
- **Exact Match Key:** (ticker, name_normalized)
- **Fuzzy Threshold:** 0.85 similarity
- **Output:** 1503 golden records từ 33 tickers
- **Match Rate:** 30.2% bản ghi xác nhận từ cả 2 nguồn (454/1503)
- **Enrichment:** Kết hợp trường từ cả 2 nguồn (tuổi, trình độ, cổ phần, v.v.)

## Author
TODO: Add author information
