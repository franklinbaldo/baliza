
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

import pytest

from baliza.cli import _FallbackClient, _SecureClient, httpx


class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass

@pytest.fixture(scope="module")
def local_server():
    server = HTTPServer(('localhost', 0), SimpleHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    yield server
    server.shutdown()
    server.server_close()

def test_ssrf_fallback_client_blocks_localhost(local_server):
    """Test that _FallbackClient blocks access to localhost."""
    port = local_server.server_port
    url = f"http://localhost:{port}/"

    client = _FallbackClient()

    # This should raise a security error
    with pytest.raises(RuntimeError, match="blocked"):
        client.get(url)

def test_ssrf_fallback_client_blocks_loopback_ip(local_server):
    """Test that _FallbackClient blocks access to 127.0.0.1."""
    port = local_server.server_port
    url = f"http://127.0.0.1:{port}/"

    client = _FallbackClient()

    with pytest.raises(RuntimeError, match="blocked"):
        client.get(url)

@pytest.mark.skipif(httpx is None, reason="httpx not installed")
def test_ssrf_secure_client_blocks_localhost(local_server):
    """Test that _SecureClient blocks access to localhost."""
    port = local_server.server_port
    url = f"http://localhost:{port}/"

    with _SecureClient() as client:
        with pytest.raises(RuntimeError, match="blocked"):
            client.get(url)

@pytest.mark.skipif(httpx is None, reason="httpx not installed")
def test_ssrf_secure_client_blocks_loopback_ip(local_server):
    """Test that _SecureClient blocks access to 127.0.0.1."""
    port = local_server.server_port
    url = f"http://127.0.0.1:{port}/"

    with _SecureClient() as client:
        with pytest.raises(RuntimeError, match="blocked"):
            client.get(url)

def test_ssrf_bypass_env_var(local_server):
    """Test that setting BALIZA_ALLOW_PRIVATE_NETWORKS=1 allows access."""
    port = local_server.server_port
    url = f"http://localhost:{port}/"

    with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):
        client = _FallbackClient()
        # Should NOT raise now
        response = client.get(url)
        assert response.status_code == 200
