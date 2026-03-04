# Từ điển Dữ liệu (Data Dictionary)

## Tổng quan
Tài liệu này định nghĩa các trường và metadata cho bộ dữ liệu ban lãnh đạo.

## Định nghĩa các trường

### Trường dữ liệu thô (Raw Data)

| Tên trường | Kiểu dữ liệu | Nguồn | Mô tả |
|------------|--------------|-------|-------|
| `ticker` | string | CafeF/Vietstock | Mã cổ phiếu |
| `exchange` | string | CafeF/Vietstock | Sàn giao dịch (HOSE/HNX) |
| `person_name` | string | CafeF/Vietstock | Họ và tên đầy đủ |
| `role` | string | CafeF/Vietstock | Chức vụ trong công ty |
| `board_type` | string | CafeF/Vietstock | Loại ban (HĐQT/Ban GĐ/BKS/Khác) |
| `age` | integer | CafeF | Tuổi |
| `year_of_birth` | string | Vietstock | Năm sinh |
| `education` | string | Vietstock | Trình độ học vấn |
| `shares` | string | Vietstock | Số lượng cổ phần sở hữu |
| `tenure_since` | string | Vietstock | Năm bắt đầu gắn bó |
| `source` | string | Hệ thống | Nguồn dữ liệu (cafef/vietstock) |
| `scraped_at` | datetime | Hệ thống | Thời gian thu thập |
| `name_normalized` | string | Hệ thống | Tên chuẩn hóa để so khớp |
| `is_current` | boolean | Hệ thống | Có đang giữ chức vụ không |

### Trường dữ liệu Golden Dataset

| Tên trường | Kiểu dữ liệu | Mô tả | Ví dụ |
|------------|--------------|-------|-------|
| `member_id` | string | ID duy nhất (MD5 hash của ticker:name) | `a1b2c3d4` |
| `ticker` | string | Mã cổ phiếu | `FPT` |
| `exchange` | string | Sàn giao dịch | `HOSE` hoặc `HNX` |
| `person_name` | string | Họ tên đầy đủ | `Trương Gia Bình` |
| `name_normalized` | string | Tên chuẩn hóa (bỏ dấu, viết thường) | `truong gia binh` |
| `role` | string | Chức vụ | `Chủ tịch HĐQT` |
| `board_type` | string | Loại ban | `HĐQT`, `Ban GĐ`, `BKS`, `Khác` |
| `age` | integer | Tuổi | `68` |
| `year_of_birth` | string | Năm sinh (từ Vietstock) | `1956` |
| `education` | string | Trình độ học vấn (từ Vietstock) | `Phó giáo sư` |
| `shares` | string | Số cổ phần sở hữu (từ Vietstock) | `117,347,966` |
| `tenure_since` | string | Năm bắt đầu gắn bó (từ Vietstock) | `1988` |
| `data_sources` | string | Nguồn dữ liệu | `cafef`, `vietstock`, `cafef,vietstock` |
| `confidence_score` | float | Điểm tin cậy | `1.0` (khớp chính xác), `0.85-0.99` (khớp mờ), `0.5` (1 nguồn) |
| `is_current` | boolean | Đang giữ chức vụ | `true` |
| `scraped_at` | datetime | Thời gian thu thập mới nhất | `2026-03-05T00:40:43` |

## Giá trị Board Type

| Giá trị | Mô tả | Chức vụ liên quan |
|---------|-------|-------------------|
| `HĐQT` | Hội đồng Quản trị | Chủ tịch HĐQT, Phó CT HĐQT, Thành viên HĐQT |
| `Ban GĐ` | Ban Giám đốc | Tổng Giám đốc, Phó TGĐ, Kế toán trưởng |
| `BKS` | Ban Kiểm soát | Trưởng BKS, Thành viên BKS |
| `Khác` | Vị trí khác | Thư ký, Người phụ trách quản trị, v.v. |

## Giá trị Confidence Score

| Giá trị | Loại khớp | Mô tả |
|---------|-----------|-------|
| `1.0` | Khớp chính xác | Tìm thấy cùng người ở cả 2 nguồn (ticker + name_normalized giống nhau) |
| `0.85 - 0.99` | Khớp mờ | Tên tương tự (similarity ≥ 0.85) |
| `0.5` | Một nguồn | Chỉ tìm thấy ở CafeF hoặc Vietstock |

## Quy tắc chất lượng dữ liệu

1. **person_name**: Không được rỗng, tối thiểu 2 ký tự
2. **ticker**: Phải là mã cổ phiếu VN hợp lệ (1-5 chữ in hoa)
3. **exchange**: Phải là `HOSE` hoặc `HNX`
4. **board_type**: Phải là một trong 4 giá trị: HĐQT, Ban GĐ, BKS, Khác
5. **confidence_score**: Giá trị từ 0.0 đến 1.0

## Metadata

- **Cập nhật lần cuối**: 2026-03-05
- **Tổng số bản ghi Golden**: 1503
- **Phạm vi dữ liệu**: 33 mã (HOSE: 20, HNX: 13)
- **Nguồn**: CafeF (1630 bản ghi), Vietstock (478 bản ghi)
