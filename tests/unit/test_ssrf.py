
import os
import pytest
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from unittest.mock import patch, MagicMock
from baliza.cli import _FallbackClient, _is_safe_url

def test_fallback_client_rejects_file_scheme(tmp_path):
    """Test that _FallbackClient refuses to load file:// URLs."""

    # Create a dummy file
    dummy_file = tmp_path / "secret.txt"
    dummy_file.write_text("SECRET_DATA")

    file_url = f"file://{dummy_file.absolute()}"

    client = _FallbackClient()

    # It should raise an error now (we expect ValueError or RuntimeError)
    with pytest.raises((ValueError, RuntimeError), match="URL scheme must be http or https"):
        client.get(file_url)

def test_fallback_client_allows_http():
    """Test that _FallbackClient still attempts http:// URLs (mocked)."""
    client = _FallbackClient()

    # It seems the sandbox environment allows network access, so this might succeed.
    # If it succeeds, it returns a _FallbackResponse.
    # If it fails (e.g. timeout), it raises RuntimeError.

    try:
        response = client.get("http://example.com")
        assert response.status_code == 200
    except RuntimeError:
        # If it failed due to network, that's fine too, as long as it wasn't the ValueError scheme check
        pass

def test_fallback_client_does_not_follow_redirects():
    """Test that _FallbackClient does NOT follow redirects."""

    # We must allow private networks for this test as it uses localhost
    with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/redirect':
                    self.send_response(302)
                    self.send_header('Location', '/target')
                    self.end_headers()
                elif self.path == '/target':
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(b"Target Reached")
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format, *args):
                return  # Silence logs

        # Setup server
        port = 0 # Let OS choose port
        server = HTTPServer(('localhost', port), RedirectHandler)
        actual_port = server.server_port

        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()

        base_url = f"http://localhost:{actual_port}"
        redirect_url = f"{base_url}/redirect"

        try:
            with _FallbackClient() as client:
                response = client.get(redirect_url)
                # Should be 302, not 200
                assert response.status_code == 302
                assert "Target Reached" not in response._text
        finally:
            server.shutdown()
            server.server_close()

def test_is_safe_url_logic():
    """Unit test for _is_safe_url logic."""
    # Test public IPs
    with patch("socket.gethostbyname") as mock_dns:
        mock_dns.return_value = "8.8.8.8"
        assert _is_safe_url("http://example.com") is True
        assert _is_safe_url("http://8.8.8.8") is True

    # Test localhost/private IPs
    assert _is_safe_url("http://localhost") is False
    assert _is_safe_url("http://127.0.0.1") is False

    with patch("socket.gethostbyname") as mock_dns:
        mock_dns.return_value = "192.168.1.1"
        assert _is_safe_url("http://internal.corp") is False

    with patch("socket.gethostbyname") as mock_dns:
        mock_dns.return_value = "10.0.0.5"
        assert _is_safe_url("http://10.0.0.5") is False

    with patch("socket.gethostbyname") as mock_dns:
        mock_dns.return_value = "169.254.1.1" # Link local
        assert _is_safe_url("http://meta-data") is False

def test_env_override_allows_private():
    """Test that BALIZA_ALLOW_PRIVATE_NETWORKS bypasses checks."""
    with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):
        assert _is_safe_url("http://localhost") is True
        assert _is_safe_url("http://127.0.0.1") is True

def test_clients_block_private_ips():
    """Test that clients raise ValueError for private IPs."""

    # Test FallbackClient
    with _FallbackClient() as client:
        with pytest.raises(ValueError, match="blocked"):
            client.get("http://localhost:1234")

    # Test SecureClient (if httpx available)
    try:
        from baliza.cli import _SecureClient
        with _SecureClient() as client:
             with pytest.raises(ValueError, match="blocked"):
                client.get("http://127.0.0.1:1234")
    except ImportError:
        pass
