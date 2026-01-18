
import pytest
import threading
import http.server
import socketserver
import time
import os
from unittest.mock import patch, MagicMock
import httpx
from baliza.cli import _SecureClient, _FallbackClient, _is_safe_url

# Helper to run a local server
class LocalServer(threading.Thread):
    def __init__(self):
        super().__init__()
        self.port = 0
        self.daemon = True
        self.handler = http.server.SimpleHTTPRequestHandler
        self.httpd = None

    def run(self):
        with socketserver.TCPServer(("localhost", 0), self.handler) as httpd:
            self.port = httpd.server_address[1]
            self.httpd = httpd
            httpd.serve_forever()

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()

@pytest.fixture(scope="module")
def local_server():
    server = LocalServer()
    server.start()
    time.sleep(1)
    yield server
    server.stop()

class TestSSRFProtection:

    def test_is_safe_url_blocks_localhost(self):
        assert _is_safe_url("http://localhost:8000") is False
        assert _is_safe_url("http://127.0.0.1:8000") is False
        assert _is_safe_url("http://[::1]:8000") is False

    def test_is_safe_url_blocks_private_networks(self):
        assert _is_safe_url("http://192.168.1.1") is False
        assert _is_safe_url("http://10.0.0.1") is False
        assert _is_safe_url("http://172.16.0.1") is False

    def test_is_safe_url_blocks_cloud_metadata(self):
        assert _is_safe_url("http://169.254.169.254") is False

    def test_is_safe_url_allows_public_ips(self):
        # 8.8.8.8 is Google DNS, definitely public
        assert _is_safe_url("http://8.8.8.8") is True
        assert _is_safe_url("https://example.com") is True

    def test_is_safe_url_bypass_env_var(self):
        with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):
            assert _is_safe_url("http://localhost:8000") is True
            assert _is_safe_url("http://192.168.1.1") is True

    def test_secure_client_blocks_request(self, local_server):
        client = _SecureClient()
        url = f"http://localhost:{local_server.port}"

        with pytest.raises(ValueError, match="blocked by SSRF protection"):
            client.get(url)

    def test_secure_client_allows_request_with_bypass(self, local_server):
        with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):
            client = _SecureClient()
            url = f"http://localhost:{local_server.port}"
            # Should not raise ValueError
            # It might raise RuntimeError if the server response is not handled correctly by the client logic
            # (e.g. if it expects specific content), but we just want to ensure it DOES NOT raise the SSRF ValueError.
            try:
                client.get(url)
            except RuntimeError:
                # Connection successful but maybe response processing failed, which is fine for this test
                pass

    def test_fallback_client_blocks_request(self, local_server):
        client = _FallbackClient()
        url = f"http://localhost:{local_server.port}"

        with pytest.raises(ValueError, match="blocked by SSRF protection"):
            client.get(url)

    def test_fallback_client_allows_request_with_bypass(self, local_server):
        with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):
            client = _FallbackClient()
            url = f"http://localhost:{local_server.port}"
            try:
                client.get(url)
            except RuntimeError:
                pass
