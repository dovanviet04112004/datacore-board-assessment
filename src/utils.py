import re
import logging
from pathlib import Path
from typing import Optional

import yaml
from unidecode import unidecode


def load_config(config_path: str = "config.yaml") -> dict:
    """Tải cấu hình từ file YAML."""
    config_file = Path(config_path)
    if not config_file.exists():
        # Thử tìm trong thư mục cha
        config_file = Path(__file__).parent.parent / config_path
    
    with open(config_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def setup_logging(config: dict):
    """Thiết lập logging dựa trên cấu hình."""
    log_config = config.get('logging', {})
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )


def normalize_name(name: str) -> str:
    """
    Chuẩn hóa tên tiếng Việt để so khớp.
    
    Tham số:
        name: Chuỗi tên đầu vào
        
    Trả về:
        Chuỗi tên đã chuẩn hóa
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Chuyển thành chữ thường
    name = name.lower().strip()
    
    # Xóa khoảng trắng thừa
    name = re.sub(r'\s+', ' ', name)
    
    # Chuyển ký tự tiếng Việt sang ASCII để so khớp
    name_ascii = unidecode(name)
    
    return name_ascii


def normalize_company_name(company: str) -> str:
    """
    Chuẩn hóa tên công ty để so khớp.
    
    Tham số:
        company: Chuỗi tên công ty đầu vào
        
    Trả về:
        Chuỗi tên công ty đã chuẩn hóa
    """
    if not company or not isinstance(company, str):
        return ""
    
    # Chuyển thành chữ thường
    company = company.lower().strip()
    
    # Xóa các hậu tố phổ biến
    suffixes = [
        'công ty cổ phần',
        'công ty tnhh',
        'ctcp',
        'tnhh',
        'joint stock company',
        'jsc',
        'corporation',
        'corp',
        'company',
        'co.',
    ]
    
    for suffix in suffixes:
        company = company.replace(suffix, '')
    
    # Xóa khoảng trắng thừa
    company = re.sub(r'\s+', ' ', company).strip()
    
    # Chuyển ký tự tiếng Việt sang ASCII
    company_ascii = unidecode(company)
    
    return company_ascii


def clean_text(text: str) -> str:
    """Làm sạch và chuẩn hóa văn bản."""
    if not text or not isinstance(text, str):
        return ""
    
    # Xóa khoảng trắng thừa
    text = re.sub(r'\s+', ' ', text.strip())
    
    return text


def calculate_similarity(str1: str, str2: str) -> float:
    """
    Tính độ tương đồng giữa hai chuỗi dựa trên khoảng cách Levenshtein.
    
    Tham số:
        str1: Chuỗi thứ nhất
        str2: Chuỗi thứ hai
        
    Trả về:
        Điểm tương đồng từ 0 đến 1
    """
    if not str1 or not str2:
        return 0.0
    
    str1 = normalize_name(str1)
    str2 = normalize_name(str2)
    
    if str1 == str2:
        return 1.0
    
    # Tỷ lệ đơn giản dựa trên ký tự chung
    len1, len2 = len(str1), len(str2)
    max_len = max(len1, len2)
    
    if max_len == 0:
        return 1.0
    
    # Đếm số ký tự khớp
    matches = sum(1 for a, b in zip(str1, str2) if a == b)
    
    return matches / max_len


def ensure_directory(path: str) -> Path:
    """Đảm bảo thư mục tồn tại, tạo mới nếu chưa có."""
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path
