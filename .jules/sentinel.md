## 2024-05-24 - Prevent SSRF in Fallback Client
**Vulnerability:** Server-Side Request Forgery (SSRF) in `_FallbackClient`. The fallback HTTP client (using `urllib`) allowed requests to private/loopback IP addresses if `httpx` was not installed, potentially exposing internal services if a malicious configuration was used.
**Learning:** Checking URL schemes (http/https) is insufficient for SSRF protection; hostname resolution and IP validation are required to block access to internal networks.
**Prevention:** Implemented `_validate_url_host` in `_FallbackClient` to resolve hostnames and verify that the target IP address is not private or loopback before making the request.
