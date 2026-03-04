import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import normalize_name, normalize_company_name, clean_text, calculate_similarity


class TestNormalizeName:
    """Tests for normalize_name function."""
    
    def test_basic_name(self):
        """Test basic name normalization."""
        assert normalize_name("Nguyen Van A") == "nguyen van a"
    
    def test_vietnamese_characters(self):
        """Test Vietnamese character conversion."""
        assert normalize_name("Nguyễn Văn Á") == "nguyen van a"
    
    def test_extra_whitespace(self):
        """Test removal of extra whitespace."""
        assert normalize_name("Nguyen   Van   A") == "nguyen van a"
    
    def test_empty_input(self):
        """Test empty input handling."""
        assert normalize_name("") == ""
        assert normalize_name(None) == ""
    
    def test_leading_trailing_whitespace(self):
        """Test trimming of whitespace."""
        assert normalize_name("  Nguyen Van A  ") == "nguyen van a"


class TestNormalizeCompanyName:
    """Tests for normalize_company_name function."""
    
    def test_remove_ctcp(self):
        """Test removal of CTCP suffix."""
        result = normalize_company_name("CTCP ABC")
        assert "ctcp" not in result
    
    def test_vietnamese_company(self):
        """Test Vietnamese company name."""
        result = normalize_company_name("Công ty Cổ phần ABC")
        assert "công ty cổ phần" not in result


class TestCleanText:
    """Tests for clean_text function."""
    
    def test_basic_cleaning(self):
        """Test basic text cleaning."""
        assert clean_text("  Hello   World  ") == "Hello World"
    
    def test_empty_input(self):
        """Test empty input handling."""
        assert clean_text("") == ""
        assert clean_text(None) == ""


class TestCalculateSimilarity:
    """Tests for calculate_similarity function."""
    
    def test_identical_strings(self):
        """Test identical strings return 1.0."""
        assert calculate_similarity("test", "test") == 1.0
    
    def test_empty_strings(self):
        """Test empty strings return 0.0."""
        assert calculate_similarity("", "test") == 0.0
        assert calculate_similarity("test", "") == 0.0
    
    def test_similar_strings(self):
        """Test similar strings return high score."""
        score = calculate_similarity("Nguyen Van A", "Nguyen Van B")
        assert score > 0.7


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
