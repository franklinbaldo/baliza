#!/usr/bin/env python3
"""
Test a few specific parameter variations to verify they work with the actual API
"""

import requests
from src.baliza.extraction.config import create_pncp_rest_config


def test_api_calls():
    """Test a few specific parameter variations against the real API"""

    print("Testing specific parameter variations against PNCP API...")

    # Get the configuration
    config = create_pncp_rest_config("20210101", "20210131")
    resources = config["resources"]

    # Test a few specific variations
    test_resources = [
        # PCA with different classification codes
        next((r for r in resources if r["name"] == "pca_2024_class01"), None),
        next((r for r in resources if r["name"] == "pca_2024_class11"), None),
        # PCA usuario with different user IDs
        next((r for r in resources if r["name"] == "pca_usuario_2024_user3"), None),
        # Modalidade variations
        next((r for r in resources if r["name"] == "contratacoes_proposta_mod6"), None),
        next((r for r in resources if r["name"] == "contratacoes_proposta_mod8"), None),
    ]

    # Filter out None values
    test_resources = [r for r in test_resources if r is not None]

    base_url = config["client"]["base_url"]
    headers = config["client"]["headers"]

    print(f"\nTesting {len(test_resources)} resource variations:")

    for resource in test_resources:
        name = resource["name"]
        path = resource["endpoint"]["path"]
        params = resource["endpoint"]["params"]

        url = f"{base_url}{path}"

        print(f"\n🔄 Testing: {name}")
        print(f"   URL: {url}")
        print(f"   Params: {params}")

        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            print(f"   Status: {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and "data" in data:
                        item_count = len(data.get("data", []))
                        total_records = data.get("totalRegistros", 0)
                        print(
                            f"   ✅ SUCCESS: {item_count} items, {total_records} total records"
                        )
                    else:
                        print("   ✅ SUCCESS: Valid response")
                except:
                    print("   ✅ SUCCESS: Non-JSON response")

            elif response.status_code == 204:
                print("   ✅ SUCCESS: No content (valid)")

            else:
                print(f"   ❌ FAILED: {response.text[:100]}...")

        except Exception as e:
            print(f"   ❌ ERROR: {e}")

    print(f"\n✅ Tested {len(test_resources)} parameter variations!")


if __name__ == "__main__":
    test_api_calls()
