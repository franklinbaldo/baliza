import os
import socket
from pathlib import Path
from unittest.mock import patch
import pytest
from src.baliza.extractor import PNCPExtractor

def test_extractor_allows_public_url():
    """Test that PNCPExtractor allows public URLs."""
    # Mock socket.gethostbyname to return a public IP
    with patch("socket.gethostbyname", return_value="8.8.8.8"):
        extractor = PNCPExtractor(Path(":memory:"), base_url="https://public-api.com")
        assert extractor.base_url == "https://public-api.com"

def test_extractor_blocks_localhost():
    """Test that PNCPExtractor raises ValueError for localhost."""
    # Mock socket.gethostbyname to return a loopback IP
    with patch("socket.gethostbyname", return_value="127.0.0.1"):
        with pytest.raises(ValueError, match="URL resolves to private IP"):
            PNCPExtractor(Path(":memory:"), base_url="http://localhost:8000")

def test_extractor_blocks_private_ip():
    """Test that PNCPExtractor raises ValueError for private IPs."""
    # Mock socket.gethostbyname to return a private IP
    with patch("socket.gethostbyname", return_value="192.168.1.5"):
        with pytest.raises(ValueError, match="URL resolves to private IP"):
            PNCPExtractor(Path(":memory:"), base_url="http://internal-service")

def test_extractor_allows_private_ip_with_env_var():
    """Test that PNCPExtractor allows private IPs when env var is set."""
    with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):
        with patch("socket.gethostbyname", return_value="192.168.1.5"):
            extractor = PNCPExtractor(Path(":memory:"), base_url="http://internal-service")
            assert extractor.base_url == "http://internal-service"

def test_extractor_validates_scheme():
    """Test that PNCPExtractor rejects non-http/https schemes."""
    with pytest.raises(ValueError, match="Invalid scheme"):
        PNCPExtractor(Path(":memory:"), base_url="ftp://example.com")

def test_extractor_default_url_is_safe():
    """Test that the default URL is considered safe."""
    # Mock to ensure default URL (pncp.gov.br) resolves to public IP
    with patch("socket.gethostbyname", return_value="200.1.2.3"):
        extractor = PNCPExtractor(Path(":memory:"))
        assert extractor.base_url == "https://pncp.gov.br/api/consulta/v1"
