import pytest
import sys
from pathlib import Path

# Thêm src vào path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import normalize_name, normalize_company_name, clean_text, calculate_similarity


class TestNormalizeName:
    
    def test_basic_name(self):
        assert normalize_name("Nguyen Van A") == "nguyen van a"

    def test_vietnamese_characters(self):
        assert normalize_name("Nguyễn Văn Á") == "nguyen van a"
    
    def test_extra_whitespace(self):
        assert normalize_name("Nguyen   Van   A") == "nguyen van a"
    
    def test_empty_input(self):
        assert normalize_name("") == ""
        assert normalize_name(None) == ""
    
    def test_leading_trailing_whitespace(self):
        assert normalize_name("  Nguyen Van A  ") == "nguyen van a"


class TestNormalizeCompanyName:
    
    def test_remove_ctcp(self):
        result = normalize_company_name("CTCP ABC")
        assert "ctcp" not in result
    
    def test_vietnamese_company(self):
        result = normalize_company_name("Công ty Cổ phần ABC")
        assert "công ty cổ phần" not in result


class TestCleanText:
    
    def test_basic_cleaning(self):
        assert clean_text("  Hello   World  ") == "Hello World"
    
    def test_empty_input(self):
        assert clean_text("") == ""
        assert clean_text(None) == ""


class TestCalculateSimilarity:
    
    def test_identical_strings(self):
        assert calculate_similarity("test", "test") == 1.0
    
    def test_empty_strings(self):
        
        assert calculate_similarity("", "test") == 0.0
        assert calculate_similarity("test", "") == 0.0
    
    def test_similar_strings(self):
       
        score = calculate_similarity("Nguyen Van A", "Nguyen Van B")
        assert score > 0.7


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
