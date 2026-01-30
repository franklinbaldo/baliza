import os
import pytest
from pathlib import Path
from src.baliza.utils import validate_url
from src.baliza.extractor import PNCPExtractor

def test_validate_url_valid():
    """Test valid URLs are accepted."""
    assert validate_url("https://pncp.gov.br") == "https://pncp.gov.br"
    assert validate_url("http://example.com/api") == "http://example.com/api"

def test_validate_url_invalid_scheme():
    """Test invalid schemes are rejected."""
    with pytest.raises(ValueError, match="Invalid URL scheme"):
        validate_url("ftp://example.com")
    with pytest.raises(ValueError, match="Invalid URL scheme"):
        validate_url("file:///etc/passwd")

def test_validate_url_private_ip():
    """Test private IPs are rejected."""
    private_ips = [
        "http://localhost",
        "http://127.0.0.1",
        "http://[::1]",
        "http://192.168.1.1",
        "http://10.0.0.1",
        "http://172.16.0.1",
        "http://169.254.169.254", # Metadata service
    ]
    for url in private_ips:
        with pytest.raises(ValueError, match="restricted"):
            validate_url(url)

def test_validate_url_bypass():
    """Test bypass mechanism."""
    os.environ["BALIZA_ALLOW_PRIVATE_NETWORKS"] = "1"
    try:
        assert validate_url("http://localhost") == "http://localhost"
    finally:
        del os.environ["BALIZA_ALLOW_PRIVATE_NETWORKS"]

def test_extractor_initialization_ssrf():
    """Test PNCPExtractor rejects invalid base_url."""
    with pytest.raises(ValueError, match="restricted"):
        PNCPExtractor(Path(":memory:"), base_url="http://169.254.169.254")
