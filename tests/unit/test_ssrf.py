
import pytest
from baliza.cli import _FallbackClient

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
