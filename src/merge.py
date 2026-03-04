import os
import sys
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import pandas as pd
import numpy as np

# Thêm thư mục cha vào path để import
sys.path.append(str(Path(__file__).parent.parent))

from src.utils import load_config, setup_logging, normalize_name, calculate_similarity

logger = logging.getLogger(__name__)


def load_processed_data(config: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Tải dữ liệu đã xử lý từ cả hai nguồn.
    
    Returns:
        Tuple (cafef_df, vietstock_df)
    """
    paths = config['paths']
    
    # Tải file parquet (không phải CSV)
    cafef_path = Path(paths['processed_data']) / 'cafef_processed.parquet'
    vietstock_path = Path(paths['processed_data']) / 'vietstock_processed.parquet'
    
    cafef_df = pd.DataFrame()
    vietstock_df = pd.DataFrame()
    
    if cafef_path.exists():
        cafef_df = pd.read_parquet(cafef_path)
        logger.info(f"Loaded {len(cafef_df)} records from CafeF")
    else:
        logger.warning(f"CafeF data not found at {cafef_path}")
    
    if vietstock_path.exists():
        vietstock_df = pd.read_parquet(vietstock_path)
        logger.info(f"Loaded {len(vietstock_df)} records from Vietstock")
    else:
        logger.warning(f"Vietstock data not found at {vietstock_path}")
    
    return cafef_df, vietstock_df


def generate_member_id(ticker: str, name_normalized: str) -> str:
    """
    Tạo ID thành viên duy nhất dựa trên ticker và tên chuẩn hóa.
    
    Args:
        ticker: Mã cổ phiếu
        name_normalized: Tên đã chuẩn hóa
        
    Returns:
        ID hash 8 ký tự
    """
    key = f"{ticker.upper()}:{name_normalized.lower()}"
    return hashlib.md5(key.encode()).hexdigest()[:8]


def standardize_board_type(cafef_type: str, vietstock_type: str) -> str:
    """
    Chuẩn hóa loại ban từ cả hai nguồn.
    
    Ánh xạ:
    - CafeF: "Hội đồng quản trị", "Ban giám đốc / Kế toán trưởng", "Ban kiểm toán", "Vị trí khác"
    - Vietstock: "HĐQT", "Ban GĐ", "BKS", "Khác"
    
    Đầu ra: HĐQT, Ban GĐ, BKS, Khác
    """
    # Ưu tiên định dạng Vietstock vì ngắn hơn
    if vietstock_type and vietstock_type not in ['', 'nan', None]:
        return vietstock_type
    
    # Ánh xạ định dạng CafeF sang chuẩn
    if cafef_type:
        cafef_type_str = str(cafef_type)
        if 'quản trị' in cafef_type_str.lower():
            return 'HĐQT'
        elif 'giám đốc' in cafef_type_str.lower():
            return 'Ban GĐ'
        elif 'kiểm' in cafef_type_str.lower():
            return 'BKS'
    
    return 'Khác'


def merge_records(cafef_row: pd.Series, vietstock_row: pd.Series, 
                  confidence: float) -> Dict:
    """
    Merge hai bản ghi khớp từ CafeF và Vietstock.
    
    Ưu tiên:
    - name: Vietstock (đầy đủ hơn)
    - role: CafeF (cụ thể hơn)
    - age: Giá trị không null
    - Các trường bổ sung: Vietstock
    
    Args:
        cafef_row: Bản ghi CafeF
        vietstock_row: Bản ghi Vietstock
        confidence: Điểm độ tin cậy khớp
        
    Returns:
        Dict bản ghi đã merge
    """
    ticker = cafef_row.get('ticker', vietstock_row.get('ticker', ''))
    name_normalized = cafef_row.get('name_normalized', vietstock_row.get('name_normalized', ''))
    
    # Ưu tiên tên Vietstock vì có thể đầy đủ hơn
    person_name = vietstock_row.get('person_name', cafef_row.get('person_name', ''))
    if not person_name or person_name == '':
        person_name = cafef_row.get('person_name', '')
    
    # Ưu tiên role CafeF (thường cụ thể hơn)
    role = cafef_row.get('role', vietstock_row.get('role', ''))
    
    # Chuẩn hóa loại ban
    board_type = standardize_board_type(
        str(cafef_row.get('board_type', '')),
        str(vietstock_row.get('board_type', ''))
    )
    
    # Tuổi: ưu tiên giá trị không null
    age = cafef_row.get('age') if pd.notna(cafef_row.get('age')) else vietstock_row.get('age')
    
    # Thời điểm scrape mới nhất
    cafef_time = cafef_row.get('scraped_at', '')
    vietstock_time = vietstock_row.get('scraped_at', '')
    scraped_at = max(str(cafef_time), str(vietstock_time)) if cafef_time and vietstock_time else (str(cafef_time) or str(vietstock_time))
    
    return {
        'member_id': generate_member_id(ticker, name_normalized),
        'ticker': ticker,
        'exchange': cafef_row.get('exchange', vietstock_row.get('exchange', '')),
        'person_name': person_name,
        'name_normalized': name_normalized,
        'role': role,
        'board_type': board_type,
        'age': age if pd.notna(age) else None,
        'year_of_birth': vietstock_row.get('year_of_birth'),
        'education': vietstock_row.get('education'),
        'shares': vietstock_row.get('shares'),
        'tenure_since': vietstock_row.get('tenure_since'),
        'data_sources': 'cafef,vietstock',
        'confidence_score': confidence,
        'is_current': True,
        'scraped_at': scraped_at,
    }


def create_single_source_record(row: pd.Series, source: str) -> Dict:
    """
    Tạo bản ghi từ một nguồn duy nhất (không khớp).
    
    Args:
        row: Bản ghi nguồn
        source: 'cafef' hoặc 'vietstock'
        
    Returns:
        Dict bản ghi
    """
    ticker = row.get('ticker', '')
    name_normalized = row.get('name_normalized', '')
    
    # Chuẩn hóa loại ban
    board_type = str(row.get('board_type', ''))
    if source == 'cafef':
        board_type = standardize_board_type(board_type, '')
    
    record = {
        'member_id': generate_member_id(ticker, name_normalized),
        'ticker': ticker,
        'exchange': row.get('exchange', ''),
        'person_name': row.get('person_name', ''),
        'name_normalized': name_normalized,
        'role': row.get('role', ''),
        'board_type': board_type,
        'age': row.get('age') if pd.notna(row.get('age')) else None,
        'year_of_birth': row.get('year_of_birth') if source == 'vietstock' else None,
        'education': row.get('education') if source == 'vietstock' else None,
        'shares': row.get('shares') if source == 'vietstock' else None,
        'tenure_since': row.get('tenure_since') if source == 'vietstock' else None,
        'data_sources': source,
        'confidence_score': 0.5,  # Nguồn đơn = độ tin cậy thấp hơn
        'is_current': row.get('is_current', True),
        'scraped_at': row.get('scraped_at', ''),
    }
    
    return record


def merge_data(cafef_df: pd.DataFrame, vietstock_df: pd.DataFrame, 
               config: dict) -> pd.DataFrame:
    """
    Merge dữ liệu từ cả hai nguồn.
    
    Chiến lược:
    1. Khớp chính xác theo (ticker, name_normalized) - confidence 1.0
    2. Khớp mờ theo tên trong cùng ticker - confidence dựa trên độ tương đồng
    3. Bản ghi không khớp - confidence 0.5
    
    Args:
        cafef_df: DataFrame CafeF
        vietstock_df: DataFrame Vietstock
        config: Dict cấu hình
        
    Returns:
        DataFrame đã merge (golden dataset)
    """
    merge_config = config.get('merge', {})
    similarity_threshold = merge_config.get('similarity_threshold', 0.85)
    
    merged_records = []
    matched_cafef_indices = set()
    matched_vietstock_indices = set()
    
    # Thống kê
    stats = {
        'exact_matches': 0,
        'fuzzy_matches': 0,
        'cafef_only': 0,
        'vietstock_only': 0,
    }
    
    logger.info("Starting merge process...")
    
    # Xử lý DataFrame rỗng
    if cafef_df.empty and vietstock_df.empty:
        logger.warning("Cả hai DataFrame đều rỗng")
        return pd.DataFrame()
    
    if cafef_df.empty:
        logger.info("Dữ liệu CafeF rỗng, chỉ sử dụng Vietstock")
        for idx, row in vietstock_df.iterrows():
            record = create_single_source_record(row, 'vietstock')
            merged_records.append(record)
            stats['vietstock_only'] += 1
        return pd.DataFrame(merged_records)
    
    if vietstock_df.empty:
        logger.info("Dữ liệu Vietstock rỗng, chỉ sử dụng CafeF"))
        for idx, row in cafef_df.iterrows():
            record = create_single_source_record(row, 'cafef')
            merged_records.append(record)
            stats['cafef_only'] += 1
        return pd.DataFrame(merged_records)
    
    # --- Giai đoạn 1: Khớp chính xác theo (ticker, name_normalized) ---
    logger.info("Giai đoạn 1: Khớp chính xác...")
    
    for cafef_idx, cafef_row in cafef_df.iterrows():
        if cafef_idx in matched_cafef_indices:
            continue
            
        ticker = cafef_row['ticker']
        name_norm = cafef_row.get('name_normalized', '')
        
        if not name_norm:
            continue
        
        # Tìm khớp chính xác trong Vietstock
        vietstock_matches = vietstock_df[
            (vietstock_df['ticker'] == ticker) & 
            (vietstock_df['name_normalized'] == name_norm) &
            (~vietstock_df.index.isin(matched_vietstock_indices))
        ]
        
        if not vietstock_matches.empty:
            vietstock_idx = vietstock_matches.index[0]
            vietstock_row = vietstock_matches.iloc[0]
            
            # Merge các bản ghi
            merged_record = merge_records(cafef_row, vietstock_row, confidence=1.0)
            merged_records.append(merged_record)
            
            matched_cafef_indices.add(cafef_idx)
            matched_vietstock_indices.add(vietstock_idx)
            stats['exact_matches'] += 1
    
    logger.info(f"Giai đoạn 1 hoàn thành: {stats['exact_matches']} khớp chính xác")
    
    # --- Giai đoạn 2: Khớp mờ cho các bản ghi còn lại ---
    logger.info("Giai đoạn 2: Khớp mờ...")
    
    unmatched_cafef = cafef_df[~cafef_df.index.isin(matched_cafef_indices)]
    unmatched_vietstock = vietstock_df[~vietstock_df.index.isin(matched_vietstock_indices)]
    
    for cafef_idx, cafef_row in unmatched_cafef.iterrows():
        if cafef_idx in matched_cafef_indices:
            continue
            
        ticker = cafef_row['ticker']
        cafef_name = cafef_row.get('name_normalized', '')
        
        if not cafef_name:
            continue
        
        # Tìm các ứng viên trong cùng ticker
        candidates = unmatched_vietstock[
            (unmatched_vietstock['ticker'] == ticker) &
            (~unmatched_vietstock.index.isin(matched_vietstock_indices))
        ]
        
        best_match = None
        best_score = 0
        best_idx = None
        
        for vietstock_idx, vietstock_row in candidates.iterrows():
            vietstock_name = vietstock_row.get('name_normalized', '')
            if not vietstock_name:
                continue
            
            # Tính độ tương đồng
            score = calculate_similarity(cafef_name, vietstock_name)
            
            if score >= similarity_threshold and score > best_score:
                best_score = score
                best_match = vietstock_row
                best_idx = vietstock_idx
        
        if best_match is not None:
            # Tìm thấy khớp mờ
            merged_record = merge_records(cafef_row, best_match, confidence=best_score)
            merged_records.append(merged_record)
            
            matched_cafef_indices.add(cafef_idx)
            matched_vietstock_indices.add(best_idx)
            stats['fuzzy_matches'] += 1
    
    logger.info(f"Giai đoạn 2 hoàn thành: {stats['fuzzy_matches']} khớp mờ")
    
    # --- Giai đoạn 3: Thêm các bản ghi không khớp ---
    logger.info("Giai đoạn 3: Thêm các bản ghi không khớp...")
    
    # Bản ghi CafeF không khớp
    for cafef_idx, cafef_row in cafef_df.iterrows():
        if cafef_idx not in matched_cafef_indices:
            record = create_single_source_record(cafef_row, 'cafef')
            merged_records.append(record)
            stats['cafef_only'] += 1
    
    # Bản ghi Vietstock không khớp
    for vietstock_idx, vietstock_row in vietstock_df.iterrows():
        if vietstock_idx not in matched_vietstock_indices:
            record = create_single_source_record(vietstock_row, 'vietstock')
            merged_records.append(record)
            stats['vietstock_only'] += 1
    
    logger.info(f"Giai đoạn 3 hoàn thành: {stats['cafef_only']} chỉ CafeF, {stats['vietstock_only']} chỉ Vietstock")
    
    # Tạo DataFrame
    merged_df = pd.DataFrame(merged_records)
    
    # Loại bỏ trùng lặp theo member_id (giữ lại lần xuất hiện đầu)
    if not merged_df.empty:
        initial_count = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['member_id'], keep='first')
        duplicates_removed = initial_count - len(merged_df)
        if duplicates_removed > 0:
            logger.info(f"Đã loại bỏ {duplicates_removed} member_id trùng lặp")
    
    # Ghi log tóm tắt
    logger.info("=" * 60)
    logger.info("TÓM TẮT MERGE")
    logger.info("=" * 60)
    logger.info(f"Exact matches (confidence=1.0): {stats['exact_matches']}")
    logger.info(f"Fuzzy matches (confidence≥{similarity_threshold}): {stats['fuzzy_matches']}")
    logger.info(f"CafeF only (confidence=0.5): {stats['cafef_only']}")
    logger.info(f"Vietstock only (confidence=0.5): {stats['vietstock_only']}")
    logger.info(f"Total golden records: {len(merged_df)}")
    logger.info("=" * 60)
    
    return merged_df


def save_final_data(df: pd.DataFrame, config: dict):
    """
    Lưu dữ liệu đã merge vào thư mục final ở cả hai định dạng Parquet và CSV.
    """
    final_dir = Path(config['paths']['final_data'])
    final_dir.mkdir(parents=True, exist_ok=True)
    
    # Lưu dạng Parquet (định dạng chính)
    parquet_path = final_dir / 'board_members_golden.parquet'
    df.to_parquet(parquet_path, engine='pyarrow', index=False)
    logger.info(f"Saved Parquet to {parquet_path}")
    
    # Lưu dạng CSV (để kiểm tra dễ dàng)
    csv_path = final_dir / 'board_members_golden.csv'
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved CSV to {csv_path}")


def main():
    """Hàm chính thực hiện merge."""
    # Tải cấu hình
    config = load_config()
    
    # Thiết lập logging
    setup_logging(config)
    
    logger.info("Bắt đầu Task 3: Merge Dữ liệu")
    logger.info("=" * 60)
    
    # Tải dữ liệu
    cafef_df, vietstock_df = load_processed_data(config)
    
    # Merge
    merged_df = merge_data(cafef_df, vietstock_df, config)
    
    if not merged_df.empty:
        # Lưu
        save_final_data(merged_df, config)
        
        # In tóm tắt
        print("\n" + "=" * 60)
        print("TASK 3: MERGE HOÀN THÀNH")
        print("=" * 60)
        print(f"Tổng số bản ghi golden: {len(merged_df)}")
        print(f"Số ticker duy nhất: {merged_df['ticker'].nunique()}")
        
        # Phân bố nguồn
        print("\nPhân bố nguồn:")
        print(merged_df['data_sources'].value_counts().to_string())
        
        # Phân bố điểm tin cậy
        print("\nPhân bố điểm tin cậy:")
        print(merged_df['confidence_score'].value_counts().sort_index(ascending=False).to_string())
        
        # Phân bố loại ban
        print("\nPhân bố loại ban:")
        print(merged_df['board_type'].value_counts().to_string())
        
        # Bản ghi mẫu
        print("\nBản ghi mẫu:")
        sample_cols = ['ticker', 'person_name', 'role', 'board_type', 'data_sources', 'confidence_score']
        print(merged_df[sample_cols].head(10).to_string())
    else:
        print("Không có dữ liệu để merge.")
    
    logger.info("Task 3 hoàn thành.")


if __name__ == "__main__":
    main()
