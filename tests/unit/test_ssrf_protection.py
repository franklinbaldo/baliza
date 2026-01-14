import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from baliza.cli import _FallbackClient, _SecureClient, httpx


class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

@pytest.fixture
def local_server():
    # Use 127.0.0.1 explicitly to ensure we are testing loopback
    server = HTTPServer(('127.0.0.1', 0), SimpleHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    yield server
    server.shutdown()
    server.server_close()

def test_ssrf_protection_fallback(local_server):
    # Ensure bypass is disabled
    if "BALIZA_ALLOW_PRIVATE_NETWORKS" in os.environ:
        del os.environ["BALIZA_ALLOW_PRIVATE_NETWORKS"]

    url = f"http://127.0.0.1:{local_server.server_port}"
    client = _FallbackClient()

    # This should raise RuntimeError because we are accessing a private/loopback IP
    with pytest.raises(RuntimeError, match="Private network access is blocked"):
        client.get(url)

if httpx is not None:
    def test_ssrf_protection_secure(local_server):
        if "BALIZA_ALLOW_PRIVATE_NETWORKS" in os.environ:
            del os.environ["BALIZA_ALLOW_PRIVATE_NETWORKS"]

        url = f"http://127.0.0.1:{local_server.server_port}"
        # _SecureClient requires httpx.Client init args or none
        client = _SecureClient()

        with pytest.raises(RuntimeError, match="Private network access is blocked"):
            client.get(url)
