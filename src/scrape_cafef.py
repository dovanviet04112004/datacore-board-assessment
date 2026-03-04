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


sys.path.append(str(Path(__file__).parent.parent))

from src.utils import load_config, setup_logging, normalize_name

logger = logging.getLogger(__name__)

# danh sách mã cổ phiếu mặc định với thông tin sàn (ticker, exchange)
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
    """ AJAX API """
    
    API_URL = "https://s.cafef.vn/Ajax/CongTy/BanLanhDao.aspx?sym={ticker}"
    TARGET_URL = "https://cafef.vn/du-lieu/{exchange}/{ticker}-ban-lanh-dao-so-huu.chn"
    
    def __init__(self, config: dict):
        """khởi tạo scraper"""
        self.config = config
        self.scraping_config = config.get('scraping', {})
        self.timeout = self.scraping_config.get('timeout', 30)
        self.delay = self.scraping_config.get('delay', 1.5)
        self.max_retries = self.scraping_config.get('max_retries', 3)
        
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
        
        self.stats = {
            'total_tickers': 0,
            'successful': 0,
            'failed': 0,
            'total_records': 0,
        }
        
        logger.info("Scraper CafeF API đã khởi tạo")
    
    def _fetch_page(self, ticker: str) -> Optional[str]:
        """Lấy dữ liệu từ AJAX API."""
        url = self.API_URL.format(ticker=ticker)
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Fetching {url} (attempt {attempt + 1}/{self.max_retries})")
                
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                
                if len(resp.text) < 500:
                    logger.warning(f"Phản hồi quá ngắn cho {ticker}")
                    continue
                
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
            
            if attempt < self.max_retries - 1:
                sleep_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(sleep_time)
        
        return None
    
    def _parse_board_table(self, table, board_type: str, ticker: str, 
                           exchange: str, scraped_at: str) -> List[Dict]:
        """phân tích bảng thành viên ban lãnh đạo"""
        records = []
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return records
        
        for row in rows[1:]:
            cells = row.find_all('td')
            
            if len(cells) < 4:
                continue
            
            try:
                role_raw = cells[0].get_text(strip=True)
                name_raw = cells[1].get_text(strip=True)
                age_raw = cells[3].get_text(strip=True) if len(cells) > 3 else ''
                
                if not name_raw or name_raw == '-':
                    continue
                
                if name_raw == 'Họ tên' or role_raw == 'Chức vụ':
                    continue
                
                # xóa tiền tố danh xưng
                person_name = name_raw
                for prefix in ['Ông ', 'Bà ', 'ông ', 'bà ']:
                    if person_name.startswith(prefix):
                        person_name = person_name[len(prefix):]
                        break
                
                age = None
                if age_raw and age_raw.isdigit():
                    age = int(age_raw)
                
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
        """phân tích dữ liệu từ HTML """
        scraped_at = datetime.now().isoformat()
        soup = BeautifulSoup(html, 'lxml')
        all_records = []
        seen_names = set()
        tables = soup.find_all('table')
        current_board_type = 'Khác'
        
        for table in tables:
            first_row = table.find('tr')
            if first_row:
                first_cell = first_row.find('td')
                if first_cell:
                    header_text = first_cell.get_text(strip=True)
                    if any(key in header_text.upper() for key in BOARD_TYPE_MAPPING.keys()):
                        current_board_type = get_board_type(header_text)
                        logger.debug(f"Tìm thấy phần ban: {header_text} -> {current_board_type}")
                        continue
            
            records = self._parse_board_table(
                table, current_board_type, ticker, exchange, scraped_at
            )
            
            for record in records:
                key = (record['person_name'], record['role'])
                if key not in seen_names:
                    seen_names.add(key)
                    all_records.append(record)
        
        return all_records
    
    def scrape_ticker(self, ticker: str, exchange: str) -> List[Dict]:
        """thu thập dữ liệu cho một mã"""
        logger.info(f"Scraping CafeF for {ticker}")
        
        html = self._fetch_page(ticker)
        if not html:
            logger.warning(f"No data returned for {ticker}")
            return []
        
        records = self._parse_html(html, ticker, exchange)
        
        logger.info(f"Found {len(records)} board members for {ticker}")
        return records
    
    def scrape_all(self, tickers: List[Tuple[str, str]] = None) -> pd.DataFrame:
        """thu thập dữ liệu cho tất cả các mã"""
        if tickers is None:
            tickers_config = self.config.get('tickers', [])
            if tickers_config:
                tickers = [(t['ticker'], t['exchange']) for t in tickers_config]
            else:
                tickers = DEFAULT_TICKERS
        
        self.stats['total_tickers'] = len(tickers)
        all_records = []
        failed_tickers = []
        successful_tickers = set()
        
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
            
            # Giới hạn tốc độ
            if i < len(tickers):
                sleep_time = self.delay + random.uniform(0, 0.5)
                logger.debug(f"Nghỉ {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            if i % 5 == 0:
                logger.info(f"Progress: {i}/{len(tickers)} tickers processed")
        
        # retry các ticker thất bại (tối đa 3 vòng)
        max_retry_rounds = 3
        retry_round = 0
        
        while failed_tickers and retry_round < max_retry_rounds:
            retry_round += 1
            retry_delay = self.delay * (retry_round + 1)
            
            logger.info(f"Retry round {retry_round}/{max_retry_rounds} for {len(failed_tickers)} failed tickers")
            time.sleep(retry_delay * 2)
            
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
        
        self.stats['successful'] = len(successful_tickers)
        self.stats['failed'] = len(failed_tickers)
        self.stats['total_records'] = len(all_records)
        
        if failed_tickers:
            logger.warning(f"Final failed tickers: {[t[0] for t in failed_tickers]}")
        
        df = pd.DataFrame(all_records)
        logger.info(f"Scraping complete. Stats: {self.stats}")
        return df
    
    def close(self):
        """đóng session"""
        if self.session:
            self.session.close()
            self.session = None


def save_to_parquet(df: pd.DataFrame, output_path: str):
    """lưu DataFrame sang định dạng Parquet"""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine='pyarrow', index=False)
    logger.info(f"Saved {len(df)} records to {output_path}")


def main():
    config = load_config()
    setup_logging(config)
    scraper = CaFeFScraper(config)
    
    try:
        df = scraper.scrape_all()
        
        if not df.empty:
            
            raw_path = Path(__file__).parent.parent / 'data' / 'raw' / 'cafef_raw.parquet'
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            save_to_parquet(df, str(raw_path))
            
            output_path = Path(__file__).parent.parent / 'data' / 'processed' / 'cafef_processed.parquet'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            save_to_parquet(df, str(output_path))
            
          
            print("\n" + "="*60)
            print("CAFEF SCRAPING SUMMARY")
            print("="*60)
            print(f"Total tickers processed: {scraper.stats['total_tickers']}")
            print(f"Successful: {scraper.stats['successful']}")
            print(f"Failed: {scraper.stats['failed']}")
            print(f"Total records: {scraper.stats['total_records']}")
            
            print("\nBoard type distribution:")
            print(df['board_type'].value_counts().to_string())
            
            print("\nExchange distribution:")
            print(df['exchange'].value_counts().to_string())
            
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
