import os
import sys
import time
import logging
import random
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Thêm thư mục cha vào path để import
sys.path.append(str(Path(__file__).parent.parent))

from src.utils import load_config, setup_logging, normalize_name

logger = logging.getLogger(__name__)

# Danh sách mã cổ phiếu mặc định với thông tin sàn (ticker, exchange)
DEFAULT_TICKERS = [
    # HOSE (20 mã)
    ('HPG', 'hose'), ('VNM', 'hose'), ('VIC', 'hose'), ('VHM', 'hose'), ('VCB', 'hose'),
    ('BID', 'hose'), ('CTG', 'hose'), ('TCB', 'hose'), ('MBB', 'hose'), ('VPB', 'hose'),
    ('FPT', 'hose'), ('MSN', 'hose'), ('VRE', 'hose'), ('GAS', 'hose'), ('SAB', 'hose'),
    ('PLX', 'hose'), ('MWG', 'hose'), ('PNJ', 'hose'), ('REE', 'hose'), ('SSI', 'hose'),
    # HNX (13 mã)
    ('SHB', 'hnx'), ('PVS', 'hnx'), ('VCS', 'hnx'), ('MBS', 'hnx'), ('NVB', 'hnx'),
    ('HUT', 'hnx'), ('IDC', 'hnx'), ('CEO', 'hnx'), ('PVI', 'hnx'), ('SHS', 'hnx'),
    ('DVD', 'hnx'), ('TAR', 'hnx'), ('PVB', 'hnx'),
]

# Ánh xạ loại ban dựa trên tiêu đề bảng
BOARD_TYPE_MAPPING = {
    'HỘI ĐỒNG SÁNG LẬP': 'HĐQT',
    'HỘI ĐỒNG QUẢN TRỊ': 'HĐQT',
    'BAN GIÁM ĐỐC': 'Ban GĐ',
    'BAN TỔNG GIÁM ĐỐC': 'Ban GĐ',
    'BAN KIỂM SOÁT': 'BKS',
}


def get_board_type(header_text: str) -> str:
    """Ánh xạ tiêu đề bảng sang loại ban chuẩn hóa."""
    header_upper = header_text.upper().strip()
    for key, value in BOARD_TYPE_MAPPING.items():
        if key in header_upper:
            return value
    return 'Khác'


class CaFeFScraper:
    """
    Scraper thu thập dữ liệu ban lãnh đạo từ CafeF sử dụng AJAX API.
    
    Sử dụng API endpoint nội bộ của trang:
    https://s.cafef.vn/Ajax/CongTy/BanLanhDao.aspx?sym={TICKER}
    """
    
    # AJAX API endpoint (phát hiện bằng cách inspect trang)
    API_URL = "https://s.cafef.vn/Ajax/CongTy/BanLanhDao.aspx?sym={ticker}"
    
    # URL tham chiếu từ đề bài (để tài liệu hóa)
    TARGET_URL = "https://cafef.vn/du-lieu/{exchange}/{ticker}-ban-lanh-dao-so-huu.chn"
    
    def __init__(self, config: dict):
        """Khởi tạo scraper với cấu hình."""
        self.config = config
        self.scraping_config = config.get('scraping', {})
        self.timeout = self.scraping_config.get('timeout', 30)
        self.delay = self.scraping_config.get('delay', 1.5)
        self.max_retries = self.scraping_config.get('max_retries', 3)
        
        # Khởi tạo session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.scraping_config.get(
                'user_agent',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Referer': 'https://cafef.vn/',
        })
        
        # Thống kê
        self.stats = {
            'total_tickers': 0,
            'successful': 0,
            'failed': 0,
            'total_records': 0,
        }
        
        logger.info("Scraper CafeF API đã khởi tạo")
    
    def _fetch_page(self, ticker: str) -> Optional[str]:
        """
        Lấy dữ liệu ban lãnh đạo từ AJAX API.
        
        Args:
            ticker: Mã cổ phiếu (ví dụ: 'FPT', 'VNM')
            
        Returns:
            Nội dung HTML hoặc None nếu thất bại
        """
        url = self.API_URL.format(ticker=ticker)
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Fetching {url} (attempt {attempt + 1}/{self.max_retries})")
                
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                
                # Kiểm tra có nội dung thực sự không
                if len(resp.text) < 500:
                    logger.warning(f"Phản hồi quá ngắn cho {ticker}")
                    continue
                
                # Kiểm tra có chỉ báo dữ liệu ban lãnh đạo
                if 'Ông' not in resp.text and 'Bà' not in resp.text:
                    logger.warning(f"Không tìm thấy tên người cho {ticker}")
                    return None
                    
                return resp.text
                
            except requests.Timeout:
                logger.warning(f"Timeout fetching {ticker}, attempt {attempt + 1}")
            except requests.HTTPError as e:
                logger.warning(f"HTTP error for {ticker}: {e}")
                if e.response.status_code == 404:
                    return None
            except requests.RequestException as e:
                logger.warning(f"Request error for {ticker}: {e}")
            
            # Lùi theo cấp số nhân
            if attempt < self.max_retries - 1:
                sleep_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(sleep_time)
        
        return None
    
    def _parse_board_table(self, table, board_type: str, ticker: str, 
                           exchange: str, scraped_at: str) -> List[Dict]:
        """
        Phân tích bảng thành viên ban lãnh đạo.
        
        Cấu trúc bảng (từ API):
        - Header: Chức vụ | Họ tên | (trống) | Tuổi | Quá trình công tác
        - Dữ liệu: Role | Name | (trống) | Age | Experience
        
        Args:
            table: Element bảng BeautifulSoup
            board_type: Loại ban chuẩn hóa (HĐQT, Ban GĐ, BKS, Khác)
            ticker: Mã cổ phiếu
            exchange: Sàn giao dịch
            scraped_at: Thời điểm thu thập
            
        Returns:
            Danh sách bản ghi cá nhân
        """
        records = []
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return records
        
        # Bỏ qua dòng header
        for row in rows[1:]:
            cells = row.find_all('td')
            
            # Cần ít nhất 4 ô: Role, Name, (trống), Age
            if len(cells) < 4:
                continue
            
            try:
                role_raw = cells[0].get_text(strip=True)
                name_raw = cells[1].get_text(strip=True)
                age_raw = cells[3].get_text(strip=True) if len(cells) > 3 else ''
                
                # Bỏ qua nếu không có tên
                if not name_raw or name_raw == '-':
                    continue
                
                # Bỏ qua các dòng giống header
                if name_raw == 'Họ tên' or role_raw == 'Chức vụ':
                    continue
                
                # Làm sạch tên - xóa tiền tố danh xưng
                person_name = name_raw
                for prefix in ['Ông ', 'Bà ', 'ông ', 'bà ']:
                    if person_name.startswith(prefix):
                        person_name = person_name[len(prefix):]
                        break
                
                # Phân tích tuổi
                age = None
                if age_raw and age_raw.isdigit():
                    age = int(age_raw)
                
                # Tạo bản ghi
                record = {
                    'ticker': ticker,
                    'exchange': exchange.upper(),
                    'person_name': person_name.strip(),
                    'role': role_raw.strip(),
                    'board_type': board_type,
                    'age': age,
                    'source': 'cafef',
                    'scraped_at': scraped_at,
                    'name_normalized': normalize_name(person_name),
                    'is_current': True,
                }
                
                records.append(record)
                
            except (IndexError, AttributeError) as e:
                logger.debug(f"Error parsing row: {e}")
                continue
        
        return records
    
    def _parse_html(self, html: str, ticker: str, exchange: str) -> List[Dict]:
        """
        Phân tích dữ liệu ban lãnh đạo từ phản hồi HTML của API.
        
        API trả về HTML với nhiều bảng:
        - Mỗi phần ban có dòng header (HỘI ĐỒNG QUẢN TRỊ, v.v.)
        - Theo sau là bảng dữ liệu với các cột: Chức vụ | Họ tên | Tuổi | ...
        
        Args:
            html: Nội dung HTML từ API
            ticker: Mã cổ phiếu
            exchange: Sàn giao dịch
            
        Returns:
            Danh sách tất cả bản ghi thành viên
        """
        scraped_at = datetime.now().isoformat()
        soup = BeautifulSoup(html, 'lxml')
        
        all_records = []
        seen_names = set()  # Để loại bỏ trùng lặp
        
        # Tìm tất cả bảng
        tables = soup.find_all('table')
        
        current_board_type = 'Khác'
        
        for table in tables:
            # Kiểm tra header loại ban
            first_row = table.find('tr')
            if first_row:
                first_cell = first_row.find('td')
                if first_cell:
                    header_text = first_cell.get_text(strip=True)
                    
                    # Kiểm tra nếu đây là bảng header
                    if any(key in header_text.upper() for key in BOARD_TYPE_MAPPING.keys()):
                        current_board_type = get_board_type(header_text)
                        logger.debug(f"Tìm thấy phần ban: {header_text} -> {current_board_type}")
                        continue  # Bỏ qua bảng header
            
            # Phân tích bảng dữ liệu
            records = self._parse_board_table(
                table, current_board_type, ticker, exchange, scraped_at
            )
            
            # Loại bỏ trùng lặp theo (name, role)
            for record in records:
                key = (record['person_name'], record['role'])
                if key not in seen_names:
                    seen_names.add(key)
                    all_records.append(record)
        
        return all_records
    
    def scrape_ticker(self, ticker: str, exchange: str) -> List[Dict]:
        """
        Thu thập dữ liệu ban lãnh đạo cho một mã cổ phiếu.
        
        Args:
            ticker: Mã cổ phiếu
            exchange: Sàn giao dịch (hose, hnx, upcom)
            
        Returns:
            Danh sách bản ghi thành viên
        """
        logger.info(f"Scraping CafeF for {ticker}")
        
        html = self._fetch_page(ticker)
        if not html:
            logger.warning(f"No data returned for {ticker}")
            return []
        
        records = self._parse_html(html, ticker, exchange)
        
        logger.info(f"Found {len(records)} board members for {ticker}")
        return records
    
    def scrape_all(self, tickers: List[Tuple[str, str]] = None) -> pd.DataFrame:
        """
        Thu thập dữ liệu ban lãnh đạo cho tất cả các mã.
        
        Args:
            tickers: Danh sách tuple (ticker, exchange).
                    Nếu None, sử dụng config hoặc danh sách mặc định.
            
        Returns:
            DataFrame chứa tất cả dữ liệu thành viên
        """
        if tickers is None:
            # Lấy ticker từ config
            tickers_config = self.config.get('tickers', [])
            if tickers_config:
                tickers = [(t['ticker'], t['exchange']) for t in tickers_config]
            else:
                tickers = DEFAULT_TICKERS
        
        self.stats['total_tickers'] = len(tickers)
        all_records = []
        failed_tickers = []  # Theo dõi ticker thất bại để retry
        successful_tickers = set()  # Theo dõi ticker thành công
        
        logger.info(f"Starting CafeF scrape for {len(tickers)} tickers")
        
        for i, (ticker, exchange) in enumerate(tickers, 1):
            try:
                records = self.scrape_ticker(ticker, exchange)
                
                if records:
                    all_records.extend(records)
                    successful_tickers.add(ticker)
                else:
                    failed_tickers.append((ticker, exchange))
                    
            except Exception as e:
                logger.error(f"Error scraping {ticker}: {e}")
                failed_tickers.append((ticker, exchange))
            
            # Giới hạn tốc độ với jitter
            if i < len(tickers):
                sleep_time = self.delay + random.uniform(0, 0.5)
                logger.debug(f"Nghỉ {sleep_time:.2f}s trước request tiếp")
                time.sleep(sleep_time)
            
            # Ghi log tiến độ
            if i % 5 == 0:
                logger.info(f"Progress: {i}/{len(tickers)} tickers processed")
        
        # Retry các ticker thất bại với delay dài hơn (tối đa 3 vòng retry)
        max_retry_rounds = 3
        retry_round = 0
        
        while failed_tickers and retry_round < max_retry_rounds:
            retry_round += 1
            retry_delay = self.delay * (retry_round + 1)  # Tăng delay mỗi vòng
            
            logger.info(f"Retry round {retry_round}/{max_retry_rounds} for {len(failed_tickers)} failed tickers (delay: {retry_delay}s)")
            time.sleep(retry_delay * 2)  # Chờ thêm trước vòng retry
            
            still_failed = []
            
            for ticker, exchange in failed_tickers:
                try:
                    logger.info(f"Retrying {ticker} (round {retry_round})")
                    time.sleep(retry_delay + random.uniform(0.5, 1.5))
                    
                    records = self.scrape_ticker(ticker, exchange)
                    
                    if records:
                        all_records.extend(records)
                        successful_tickers.add(ticker)
                        logger.info(f"Successfully scraped {ticker} on retry")
                    else:
                        still_failed.append((ticker, exchange))
                        
                except Exception as e:
                    logger.error(f"Error retrying {ticker}: {e}")
                    still_failed.append((ticker, exchange))
            
            failed_tickers = still_failed
        
        # Cập nhật thống kê cuối
        self.stats['successful'] = len(successful_tickers)
        self.stats['failed'] = len(failed_tickers)
        self.stats['total_records'] = len(all_records)
        
        if failed_tickers:
            logger.warning(f"Final failed tickers after all retries: {[t[0] for t in failed_tickers]}")
        
        # Chuyển sang DataFrame
        df = pd.DataFrame(all_records)
        
        logger.info(f"Scraping complete. Stats: {self.stats}")
        
        return df
    
    def close(self):
        """Đóng session."""
        if self.session:
            self.session.close()
            self.session = None


def save_to_parquet(df: pd.DataFrame, output_path: str):
    """Lưu DataFrame sang định dạng Parquet."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine='pyarrow', index=False)
    logger.info(f"Saved {len(df)} records to {output_path}")


def main():
    """Điểm vào chính của CafeF scraper."""
    # Tải cấu hình
    config = load_config()
    
    # Thiết lập logging
    setup_logging(config)
    
    # Khởi tạo scraper
    scraper = CaFeFScraper(config)
    
    try:
        # Thu thập tất cả ticker
        df = scraper.scrape_all()
        
        if not df.empty:
            # Lưu dữ liệu thô trước
            raw_path = Path(__file__).parent.parent / 'data' / 'raw' / 'cafef_raw.parquet'
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            save_to_parquet(df, str(raw_path))
            
            # Lưu dữ liệu đã xử lý
            output_path = Path(__file__).parent.parent / 'data' / 'processed' / 'cafef_processed.parquet'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            save_to_parquet(df, str(output_path))
            
            # In tóm tắt
            print("\n" + "="*60)
            print("CAFEF SCRAPING SUMMARY")
            print("="*60)
            print(f"Total tickers processed: {scraper.stats['total_tickers']}")
            print(f"Successful: {scraper.stats['successful']}")
            print(f"Failed: {scraper.stats['failed']}")
            print(f"Total records: {scraper.stats['total_records']}")
            
            # Hiển thị phân bố loại ban
            print("\nBoard type distribution:")
            print(df['board_type'].value_counts().to_string())
            
            # Hiển thị phân bố sàn
            print("\nExchange distribution:")
            print(df['exchange'].value_counts().to_string())
            
            # Hiển thị dữ liệu mẫu
            print("\nSample records:")
            print(df[['ticker', 'exchange', 'person_name', 'role', 'source', 'scraped_at']].head(10).to_string())
        else:
            print("No data scraped.")
            
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise
    finally:
        scraper.close()


if __name__ == '__main__':
    main()
