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

# danh sách mã cổ phiếu
DEFAULT_TICKERS = [
    'HPG', 'VNM', 'VIC', 'VHM', 'VCB', 'BID', 'CTG', 'TCB', 'MBB', 'VPB',
    'FPT', 'MSN', 'VRE', 'GAS', 'SAB', 'PLX', 'MWG', 'PNJ', 'REE', 'SSI',
    'SHB', 'PVS', 'VCS', 'MBS', 'NVB', 'HUT', 'IDC', 'CEO', 'PVI', 'SHS',
    'DVD', 'TAR', 'PVB',
]

ROLE_TO_BOARD_TYPE = {
    'CTHĐQT': 'HĐQT',
    'Phó CTHĐQT': 'HĐQT',
    'TVHĐQT': 'HĐQT',
    'Chủ tịch HĐQT': 'HĐQT',
    'Phó Chủ tịch HĐQT': 'HĐQT',
    'Thành viên HĐQT': 'HĐQT',
    'TV HĐQT': 'HĐQT',
    'TGĐ': 'Ban GĐ',
    'Phó TGĐ': 'Ban GĐ',
    'Tổng Giám đốc': 'Ban GĐ',
    'Phó Tổng Giám đốc': 'Ban GĐ',
    'PTGĐ': 'Ban GĐ',
    'Giám đốc': 'Ban GĐ',
    'Phó Giám đốc': 'Ban GĐ',
    'KTT': 'Ban GĐ',
    'Kế toán trưởng': 'Ban GĐ',
    'Trưởng BKS': 'BKS',
    'Thành viên BKS': 'BKS',
    'TV BKS': 'BKS',
    'TVBKS': 'BKS',
    'Người phụ trách quản trị': 'Khác',
    'Thư ký': 'Khác',
}


def get_board_type(role: str) -> str:
    """Ánh xạ chức vụ sang loại ban"""
    role_normalized = role.strip()
    
    # Khớp trực tiếp trước
    if role_normalized in ROLE_TO_BOARD_TYPE:
        return ROLE_TO_BOARD_TYPE[role_normalized]
    
    # Khớp theo từ khóa
    role_upper = role_normalized.upper()
    if 'HĐQT' in role_upper or 'HỘI ĐỒNG QUẢN TRỊ' in role_upper:
        return 'HĐQT'
    elif 'BKS' in role_upper or 'KIỂM SOÁT' in role_upper:
        return 'BKS'
    elif 'GIÁM ĐỐC' in role_upper or 'TGĐ' in role_upper or 'GĐ' in role_upper or 'KTT' in role_upper:
        return 'Ban GĐ'
    else:
        return 'Khác'


class VietstockScraper:
    """Scraper thu thập dữ liệu ban lãnh đạo từ Vietstock"""
    
    BASE_URL = "https://finance.vietstock.vn"
    LEADER_URL = "{base}/{ticker}/ban-lanh-dao.htm"
    
    def __init__(self, config: dict):
        """khởi tạo scraper"""
        self.config = config
        self.scraping_config = config.get('scraping', {})
        self.timeout = self.scraping_config.get('timeout', 30)
        self.delay = self.scraping_config.get('delay', 1.5)
        self.max_retries = self.scraping_config.get('max_retries', 3)
        
        self.session = None
        self._init_session()
        
        self.stats = {
            'total_tickers': 0,
            'successful': 0,
            'failed': 0,
            'total_records': 0,
        }
    
    def _init_session(self):
        """khởi tạo session với CSRF token"""
        self.session = requests.Session()
        
        # headers giả lập trình duyệt
        self.session.headers.update({
            'User-Agent': self.scraping_config.get(
                'user_agent',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # lấy cookies từ trang chủ
        try:
            logger.info("Đang thiết lập session Vietstock")
            resp = self.session.get(self.BASE_URL, timeout=self.timeout)
            resp.raise_for_status()
            
            cookies = self.session.cookies.get_dict()
            if '__RequestVerificationToken' in cookies:
                logger.info("Đã lấy CSRF token")
            if 'ASP.NET_SessionId' in cookies:
                logger.info(f"Session ID: {cookies['ASP.NET_SessionId'][:8]}")
                
            logger.info(f"Session đã thiết lập")
            
        except requests.RequestException as e:
            logger.error(f"Không thể thiết lập session: {e}")
            raise
    
    def _fetch_page(self, ticker: str) -> Optional[str]:
        """Lấy trang ban lãnh đạo."""
        url = self.LEADER_URL.format(base=self.BASE_URL, ticker=ticker)
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Fetching {url} (attempt {attempt + 1}/{self.max_retries})")
                
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                
                if len(resp.text) < 1000:
                    logger.warning(f"Phản hồi quá ngắn cho {ticker}")
                    continue
                    
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
    
    def _parse_board_table(self, table, ticker: str, scraped_at: str) -> List[Dict]:
        """phân tích bảng thành viên"""
        records = []
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return records
        
        current_period = None
        
        for row in rows[1:]:
            cells = row.find_all('td')
            if not cells:
                continue
            
            try:
                cell_index = 0
                
                first_cell = cells[0]
                if first_cell.has_attr('rowspan'):
                    current_period = first_cell.get_text(strip=True)
                    cell_index = 1
                elif current_period is None:
                    current_period = first_cell.get_text(strip=True)
                    cell_index = 1
                
                remaining_cells = cells[cell_index:]
                if len(remaining_cells) < 6:
                    continue
                
                name_raw = remaining_cells[0].get_text(strip=True)
                role = remaining_cells[1].get_text(strip=True)
                year_of_birth = remaining_cells[2].get_text(strip=True)
                education = remaining_cells[3].get_text(strip=True)
                shares = remaining_cells[4].get_text(strip=True)
                tenure = remaining_cells[5].get_text(strip=True)
                
                # bỏ qua dữ liệu bị che (nội dung trả phí)
                if '***' in name_raw or name_raw == '-':
                    continue
                
                # xóa tiền tố danh xưng
                person_name = name_raw
                for prefix in ['Ông ', 'Bà ', 'ông ', 'bà ']:
                    if person_name.startswith(prefix):
                        person_name = person_name[len(prefix):]
                        break
                
                if not person_name or person_name == '-':
                    continue
                
                board_type = get_board_type(role)
                
                age = None
                if year_of_birth and year_of_birth.isdigit():
                    current_year = datetime.now().year
                    age = current_year - int(year_of_birth)
                
                # tạo bản ghi
                record = {
                    'ticker': ticker,
                    'exchange': self._get_exchange(ticker),
                    'person_name': person_name.strip(),
                    'role': role.strip(),
                    'board_type': board_type,
                    'age': age,
                    'source': 'vietstock',
                    'scraped_at': scraped_at,
                    'name_normalized': normalize_name(person_name),
                    'is_current': True,
                    'year_of_birth': year_of_birth if year_of_birth and year_of_birth != '-' else None,
                    'education': education if education and education not in ['-', 'N/a', 'N/A'] else None,
                    'shares': shares if shares and shares != '-' else None,
                    'tenure_since': tenure if tenure and tenure not in ['-', '***'] else None,
                    'report_period': current_period,
                }
                
                records.append(record)
                
            except (IndexError, AttributeError) as e:
                logger.debug(f"Error parsing row for {ticker}: {e}")
                continue
        
        return records
    
    def _get_exchange(self, ticker: str) -> str:
        """lấy sàn giao dịch"""
        tickers_config = self.config.get('tickers', [])
        for t in tickers_config:
            if t.get('ticker') == ticker:
                return t.get('exchange', 'unknown').upper()
        
        hnx_tickers = {'SHB', 'PVS', 'VCS', 'MBS', 'NVB', 'HUT', 'IDC', 'CEO', 'PVI', 'SHS', 'DVD', 'TAR', 'PVB'}
        return 'HNX' if ticker in hnx_tickers else 'HOSE'
    
    def scrape_ticker(self, ticker: str) -> List[Dict]:
        """thu thập dữ liệu cho một mã"""
        logger.info(f"Scraping Vietstock for {ticker}")
        scraped_at = datetime.now().isoformat()
        
        html = self._fetch_page(ticker)
        if not html:
            logger.warning(f"No data returned for {ticker}")
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table', class_='table')
        
        if not tables:
            logger.warning(f"No data tables found for {ticker}")
            return []
        
        # Chỉ lấy bảng đầu (dữ liệu mới nhất)
        records = []
        if tables:
            first_table_records = self._parse_board_table(tables[0], ticker, scraped_at)
            for r in first_table_records:
                r['is_current'] = True
            records.extend(first_table_records)
        
        logger.info(f"Found {len(records)} board members for {ticker}")
        return records
    
    def scrape_all(self, tickers: List[str] = None) -> pd.DataFrame:
        """Thu thập dữ liệu cho tất cả các mã"""
        if tickers is None:
            tickers_config = self.config.get('tickers', [])
            if tickers_config:
                tickers = [t['ticker'] for t in tickers_config]
            else:
                tickers = DEFAULT_TICKERS
        
        self.stats['total_tickers'] = len(tickers)
        all_records = []
        failed_tickers = []
        successful_tickers = set()
        
        logger.info(f"Starting Vietstock scrape for {len(tickers)} tickers")
        
        for i, ticker in enumerate(tickers, 1):
            try:
                records = self.scrape_ticker(ticker)
                
                if records:
                    all_records.extend(records)
                    successful_tickers.add(ticker)
                else:
                    failed_tickers.append(ticker)
                    
            except Exception as e:
                logger.error(f"Error scraping {ticker}: {e}")
                failed_tickers.append(ticker)
            
            # giới hạn tốc độ
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
            
            # khởi tạo lại session (cookies mới)
            self._init_session()
            
            still_failed = []
            
            for ticker in failed_tickers:
                try:
                    logger.info(f"Retrying {ticker} (round {retry_round})")
                    time.sleep(retry_delay + random.uniform(0.5, 1.5))
                    
                    records = self.scrape_ticker(ticker)
                    
                    if records:
                        all_records.extend(records)
                        successful_tickers.add(ticker)
                        logger.info(f"Successfully scraped {ticker} on retry")
                    else:
                        still_failed.append(ticker)
                        
                except Exception as e:
                    logger.error(f"Error retrying {ticker}: {e}")
                    still_failed.append(ticker)
            
            failed_tickers = still_failed
        
        self.stats['successful'] = len(successful_tickers)
        self.stats['failed'] = len(failed_tickers)
        self.stats['total_records'] = len(all_records)
        
        if failed_tickers:
            logger.warning(f"Final failed tickers: {failed_tickers}")
        
        df = pd.DataFrame(all_records)
        logger.info(f"Scraping complete. Stats: {self.stats}")
        return df
    
    def close(self):
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
    scraper = VietstockScraper(config)
    
    try:
        df = scraper.scrape_all()
        
        if not df.empty:
          
            raw_path = Path(__file__).parent.parent / 'data' / 'raw' / 'vietstock_raw.parquet'
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            save_to_parquet(df, str(raw_path))
            
            output_path = Path(__file__).parent.parent / 'data' / 'processed' / 'vietstock_processed.parquet'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            save_to_parquet(df, str(output_path))
            
            print("\n" + "="*60)
            print("VIETSTOCK SCRAPING SUMMARY")
            print("="*60)
            print(f"Total tickers processed: {scraper.stats['total_tickers']}")
            print(f"Successful: {scraper.stats['successful']}")
            print(f"Failed: {scraper.stats['failed']}")
            print(f"Total records: {scraper.stats['total_records']}")
            
            print("\nBoard type distribution:")
            print(df['board_type'].value_counts().to_string())
            
            print("\nSample records:")
            print(df[['ticker', 'person_name', 'role', 'board_type', 'year_of_birth', 'education']].head(10).to_string())
        else:
            print("No data scraped.")
            
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise
    finally:
        scraper.close()


if __name__ == '__main__':
    main()
