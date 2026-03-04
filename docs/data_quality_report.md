# Báo cáo Chất lượng Dữ liệu (Data Quality Report)

## Tổng quan
Phân tích chất lượng dữ liệu ban lãnh đạo từ CafeF và Vietstock sau khi thu thập và hợp nhất.

**Ngày báo cáo:** 2026-03-05

---

## Tóm tắt thu thập dữ liệu

| Metric | CafeF | Vietstock | Golden Dataset |
|--------|-------|-----------|----------------|
| Tổng bản ghi | 1630 | 478 | 1503 |
| Số ticker | 33 | 32 | 33 |
| Tỷ lệ thành công | 100% | 97% | - |
| Sàn giao dịch | HOSE, HNX | HOSE, HNX | HOSE, HNX |

---

## Thống kê theo nguồn

### Task 1: CafeF
- **API Endpoint:** `s.cafef.vn/Ajax/CongTy/BanLanhDao.aspx?sym={ticker}`
- **Tỷ lệ thành công:** 33/33 mã (100%)
- **Ticker thất bại:** Không có (SAB retry thành công)
- **Thời gian trung bình/ticker:** ~0.5 giây
- **Tổng bản ghi:** 1630

**Phân bố theo sàn:**
| Sàn | Số bản ghi |
|-----|------------|
| HOSE | 1126 |
| HNX | 504 |

### Task 2: Vietstock
- **URL Pattern:** `finance.vietstock.vn/{TICKER}/ban-lanh-dao.htm`
- **Tỷ lệ thành công:** 32/33 mã (97%)
- **Ticker thất bại:** DVD (dữ liệu bị che `***` - nội dung trả phí)
- **Thời gian trung bình/ticker:** ~1.5 giây
- **Tổng bản ghi:** 478

**Phân bố theo loại ban:**
| Loại ban | Số bản ghi |
|----------|------------|
| HĐQT | 234 |
| Ban GĐ | 155 |
| BKS | 80 |
| Khác | 9 |

---

## Phân tích độ đầy đủ (Completeness)

### Độ đầy đủ theo trường

| Trường | CafeF | Vietstock | Golden Dataset |
|--------|-------|-----------|----------------|
| ticker | 100% | 100% | 100% |
| exchange | 100% | 100% | 100% |
| person_name | 100% | 100% | 100% |
| role | 100% | 100% | 100% |
| board_type | 100% | 100% | 100% |
| age | ~90% | ~95% | ~75% |
| year_of_birth | N/A | ~95% | ~30% |
| education | N/A | ~80% | ~30% |
| shares | N/A | ~85% | ~30% |
| tenure_since | N/A | ~80% | ~30% |

**Ghi chú:** Các trường year_of_birth, education, shares, tenure_since chỉ có từ Vietstock, nên tỷ lệ thấp hơn trong Golden Dataset do đa số là bản ghi chỉ từ CafeF.

---

## Chất lượng Merge (Task 3)

### Phân bố loại khớp

| Loại khớp | Số lượng | Tỷ lệ | Confidence |
|-----------|----------|-------|------------|
| Khớp chính xác | 452 | 30.1% | 1.0 |
| Khớp mờ (fuzzy) | 2 | 0.1% | 0.85-0.99 |
| Chỉ CafeF | 1025 | 68.2% | 0.5 |
| Chỉ Vietstock | 24 | 1.6% | 0.5 |
| **Tổng** | **1503** | **100%** | - |

### Phân bố nguồn dữ liệu

| Nguồn | Số bản ghi | Tỷ lệ |
|-------|------------|-------|
| Cả 2 nguồn (`cafef,vietstock`) | 454 | 30.2% |
| Chỉ CafeF (`cafef`) | 1025 | 68.2% |
| Chỉ Vietstock (`vietstock`) | 24 | 1.6% |

### Phân bố điểm tin cậy

| Confidence Score | Số bản ghi | Ý nghĩa |
|------------------|------------|---------|
| 1.0 | 452 | Xác nhận từ cả 2 nguồn |
| 0.95 - 0.99 | 2 | Khớp mờ với độ tương đồng cao |
| 0.5 | 1049 | Chỉ tìm thấy ở 1 nguồn |

### Xử lý trùng lặp
- **Số bản ghi trùng bị loại:** 151
- **Lý do:** Cùng người có nhiều chức vụ hoặc xuất hiện nhiều lần

---

## Các vấn đề dữ liệu đã biết

### 1. SAB - CafeF (Đã khắc phục)
- **Vấn đề:** Lần đầu trả về trang trống
- **Giải pháp:** Retry logic thành công lần 2
- **Tác động:** Không mất dữ liệu

### 2. DVD - Vietstock
- **Vấn đề:** Dữ liệu ban lãnh đạo bị che bởi `*** ***` (nội dung trả phí)
- **Nguyên nhân:** Vietstock yêu cầu tài khoản Premium để xem thông tin một số công ty
- **Dữ liệu thực tế:** `Họ và tên: *** ***`, `Chức vụ: ***`
- **Tác động:** Mất 1 ticker từ Vietstock
- **Khắc phục:** Dữ liệu có sẵn từ CafeF (23 bản ghi)

### 3. Khác biệt định dạng Board Type
| CafeF | Vietstock | Chuẩn hóa |
|-------|-----------|-----------|
| Hội đồng quản trị | HĐQT | HĐQT |
| Ban giám đốc | Ban GĐ | Ban GĐ |
| Ban kiểm toán | BKS | BKS |

**Giải pháp:** Ưu tiên giá trị từ Vietstock vì ngắn gọn và chuẩn hóa hơn.

### 4. Khác biệt định dạng Role
- **CafeF:** "Chủ tịch HĐQT", "Tổng Giám đốc"
- **Vietstock:** "CTHĐQT", "TGĐ"
- **Giải pháp:** Giữ nguyên role từ nguồn gốc (cả 2 đều đúng)

---

## Kiểm tra chất lượng đã thực hiện

| Kiểm tra | Mô tả | Kết quả |
|----------|-------|---------|
| Name normalization | Chuẩn hóa tên (viết thường, bỏ dấu) | Thành công |
| Board type standardization | Chuẩn hóa loại ban | Thành công |
| Duplicate detection | Phát hiện và loại bỏ trùng lặp | 151 bản ghi loại |
| Confidence scoring | Đánh điểm tin cậy | 3 mức: 1.0, 0.85-0.99, 0.5 |
| Source tracking | Theo dõi nguồn dữ liệu | data_sources field |
| Retry logic | Thử lại ticker thất bại | Tối đa 3 vòng |

---

## Đề xuất cải thiện

### Ngắn hạn
1. **Giảm ngưỡng fuzzy matching** xuống 0.80 để bắt thêm edge cases
2. **Thêm validation** cho năm sinh (1920-2010) và tuổi (18-100)
3. **Log chi tiết hơn** để debug các ticker thất bại

### Dài hạn
1. **Theo dõi lịch sử** - Phát hiện thay đổi nhân sự qua thời gian
2. **Cross-validate** với nguồn chính thức (báo cáo thường niên, HOSE/HNX)
3. **Parallel scraping** với asyncio để tăng tốc
4. **Database storage** thay vì file để query hiệu quả hơn

---

## Phụ lục

### Định nghĩa các metric chất lượng

| Metric | Định nghĩa |
|--------|------------|
| Completeness | % giá trị không null |
| Accuracy | % giá trị chính xác (kiểm tra thủ công) |
| Consistency | Mức độ đồng nhất trong biểu diễn dữ liệu |
| Confidence | Điểm tin cậy khớp (0-1) |

### Ánh xạ Board Type

| Chuẩn | CafeF | Vietstock | Chức vụ liên quan |
|-------|-------|-----------|-------------------|
| HĐQT | Hội đồng quản trị | HĐQT | CT, Phó CT, TV HĐQT |
| Ban GĐ | Ban giám đốc | Ban GĐ | TGĐ, Phó TGĐ, KTT |
| BKS | Ban kiểm toán | BKS | Trưởng BKS, TV BKS |
| Khác | Vị trí khác | Khác | Thư ký, Quản trị viên |
