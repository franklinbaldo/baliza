import pytest
from unittest.mock import patch
from baliza.cli import _FallbackClient

@pytest.mark.parametrize("hostname, ip_address", [
    ("localhost", "127.0.0.1"),
    ("internal.service", "10.0.0.5"),
    ("aws.metadata", "169.254.169.254"),
    ("local.ipv6", "::1"),
])
def test_fallback_client_blocks_private_ips(hostname, ip_address):
    """Test that _FallbackClient blocks requests to private IPs (SSRF protection)."""
    client = _FallbackClient()
    url = f"http://{hostname}/data"

    # Mock getaddrinfo to return the specific IP
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # getaddrinfo returns list of (family, type, proto, canonname, sockaddr)
        mock_getaddrinfo.return_value = [
            (None, None, None, None, (ip_address, 80))
        ]

        # We expect a RuntimeError due to security check
        with pytest.raises(RuntimeError, match="Security: Host.*resolves to private/loopback IP"):
            client.get(url)

def test_fallback_client_allows_public_ips():
    """Test that _FallbackClient allows requests to public IPs."""
    client = _FallbackClient()
    url = "http://example.com/data"

    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # 93.184.216.34 is example.com (public IP)
        mock_getaddrinfo.return_value = [
            (None, None, None, None, ("93.184.216.34", 80))
        ]

        # Mock urlopen so we don't actually hit the network
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_response = mock_urlopen.return_value.__enter__.return_value
            mock_response.getcode.return_value = 200
            mock_response.read.return_value = b"{}"

            client.get(url)
