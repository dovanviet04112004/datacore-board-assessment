import re
import logging
from pathlib import Path
from typing import Optional

import yaml
from unidecode import unidecode


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    config_file = Path(config_path)
    if not config_file.exists():
        # Try looking in parent directory
        config_file = Path(__file__).parent.parent / config_path
    
    with open(config_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def setup_logging(config: dict):
    """Setup logging based on configuration."""
    log_config = config.get('logging', {})
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )


def normalize_name(name: str) -> str:
    """
    Normalize Vietnamese names for matching.
    
    Args:
        name: Input name string
        
    Returns:
        Normalized name string
    """
    if not name or not isinstance(name, str):
        return ""
    
    # Convert to lowercase
    name = name.lower().strip()
    
    # Remove extra whitespace
    name = re.sub(r'\s+', ' ', name)
    
    # Convert Vietnamese characters to ASCII for matching
    name_ascii = unidecode(name)
    
    return name_ascii


def normalize_company_name(company: str) -> str:
    """
    Normalize company names for matching.
    
    Args:
        company: Input company name string
        
    Returns:
        Normalized company name string
    """
    if not company or not isinstance(company, str):
        return ""
    
    # Convert to lowercase
    company = company.lower().strip()
    
    # Remove common suffixes
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
    
    # Remove extra whitespace
    company = re.sub(r'\s+', ' ', company).strip()
    
    # Convert Vietnamese characters to ASCII
    company_ascii = unidecode(company)
    
    return company_ascii


def clean_text(text: str) -> str:
    """Clean and normalize text."""
    if not text or not isinstance(text, str):
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    
    return text


def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculate similarity between two strings using Levenshtein distance.
    
    Args:
        str1: First string
        str2: Second string
        
    Returns:
        Similarity score between 0 and 1
    """
    if not str1 or not str2:
        return 0.0
    
    str1 = normalize_name(str1)
    str2 = normalize_name(str2)
    
    if str1 == str2:
        return 1.0
    
    # Simple ratio based on common characters
    len1, len2 = len(str1), len(str2)
    max_len = max(len1, len2)
    
    if max_len == 0:
        return 1.0
    
    # Count matching characters
    matches = sum(1 for a, b in zip(str1, str2) if a == b)
    
    return matches / max_len


def ensure_directory(path: str) -> Path:
    """Ensure directory exists, create if not."""
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path
