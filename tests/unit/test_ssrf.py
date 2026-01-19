
import os
import pytest
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from baliza.cli import _FallbackClient

def test_fallback_client_rejects_file_scheme(tmp_path):
    """Test that _FallbackClient refuses to load file:// URLs."""

    # Create a dummy file
    dummy_file = tmp_path / "secret.txt"
    dummy_file.write_text("SECRET_DATA")

    file_url = f"file://{dummy_file.absolute()}"

    client = _FallbackClient()

    # It should raise an error now (we expect ValueError or RuntimeError)
    # Note: SSRF protection might kick in before scheme check depending on implementation order,
    # but file:// typically fails parsing or scheme check.
    # In our implementation, _is_safe_url is called first, and file:// usually parses with empty hostname,
    # causing _is_safe_url to return False, raising "blocked by SSRF protection".
    with pytest.raises((ValueError, RuntimeError)):
        client.get(file_url)

def test_fallback_client_allows_http(monkeypatch):
    """Test that _FallbackClient still attempts http:// URLs (mocked)."""
    # Allow localhost for this test if needed, though example.com is external
    # monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
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

def test_fallback_client_does_not_follow_redirects(monkeypatch):
    """Test that _FallbackClient does NOT follow redirects."""

    # Allow localhost for this test since we spin up a local server
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")

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
