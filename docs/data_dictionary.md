# Data Dictionary

## Overview
This document defines the fields and metadata for the board member dataset.

## Field Definitions

### Raw Data Fields

| Field Name | Data Type | Source | Description |
|------------|-----------|--------|-------------|
| `name` | string | CafeF/Vietstock | Full name of board member |
| `position` | string | CafeF/Vietstock | Position/role in the company |
| `company_name` | string | CafeF/Vietstock | Company name |
| `stock_code` | string | CafeF/Vietstock | Stock ticker symbol |
| `start_date` | date | CafeF/Vietstock | Date when position started |
| `end_date` | date | CafeF/Vietstock | Date when position ended (if applicable) |
| `birth_year` | integer | CafeF/Vietstock | Year of birth |
| `education` | string | CafeF/Vietstock | Educational background |
| `source` | string | System | Data source identifier |
| `scraped_at` | datetime | System | Timestamp of data collection |

### Processed Data Fields

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `name_normalized` | string | Normalized name for matching |
| `company_normalized` | string | Normalized company name |
| `is_current` | boolean | Whether position is current |

### Final Dataset Fields (Golden Dataset)

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `member_id` | string | Unique identifier (MD5 hash of ticker:name) |
| `ticker` | string | Stock ticker symbol |
| `exchange` | string | Stock exchange (HOSE/HNX) |
| `person_name` | string | Canonical name |
| `name_normalized` | string | Normalized name for matching |
| `role` | string | Position/role in company |
| `board_type` | string | Board type (HĐQT/Ban GĐ/BKS/Khác) |
| `age` | integer | Age (calculated from year_of_birth) |
| `year_of_birth` | string | Birth year (from Vietstock) |
| `education` | string | Educational background (from Vietstock) |
| `shares` | string | Number of shares owned (from Vietstock) |
| `tenure_since` | string | Year joined company (from Vietstock) |
| `data_sources` | string | Sources: "cafef", "vietstock", or "cafef,vietstock" |
| `confidence_score` | float | Match confidence (1.0=exact, 0.85-0.99=fuzzy, 0.5=single source) |
| `is_current` | boolean | Whether position is current |
| `scraped_at` | datetime | Latest scraping timestamp |

## Data Quality Rules

1. **Name**: Must not be empty, minimum 2 characters
2. **Company**: Must not be empty
3. **Stock Code**: Must be valid VN stock ticker format
4. **Dates**: Must be valid date format, start_date <= end_date

## Metadata

- **Last Updated**: 2026-03-03
- **Total Records**: 1503 (golden dataset)
- **Data Coverage**: 33 tickers (HOSE: 20, HNX: 13)
- **Sources**: CafeF (1630 records), Vietstock (478 records)
