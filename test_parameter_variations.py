#!/usr/bin/env python3
"""
Test script to verify parameter variation generation works correctly
"""

from src.baliza.extraction.config import create_pncp_rest_config

def test_parameter_variations():
    """Test that parameter variations are generated correctly"""
    
    print("Testing parameter variation generation...")
    
    # Create config with parameter variations
    config = create_pncp_rest_config("20210101", "20210131", [6, 8])  # Test with some modalidades
    
    resources = config["resources"]
    
    print(f"\nTotal resources generated: {len(resources)}")
    
    # Group resources by endpoint type
    endpoint_groups = {}
    for resource in resources:
        endpoint_base = resource["name"].split("_")[0]
        if endpoint_base not in endpoint_groups:
            endpoint_groups[endpoint_base] = []
        endpoint_groups[endpoint_base].append(resource["name"])
    
    print("\nResource breakdown by endpoint:")
    for endpoint, resource_names in endpoint_groups.items():
        print(f"  {endpoint}: {len(resource_names)} variations")
        for name in resource_names[:5]:  # Show first 5
            print(f"    - {name}")
        if len(resource_names) > 5:
            print(f"    ... and {len(resource_names) - 5} more")
    
    # Test specific endpoint variations
    print("\nTesting specific variations:")
    
    # Find PCA variations
    pca_resources = [r for r in resources if r["name"].startswith("pca_")]
    print(f"\nPCA variations: {len(pca_resources)}")
    for resource in pca_resources[:3]:  # Show first 3
        print(f"  - {resource['name']}")
        params = resource["endpoint"]["params"]
        print(f"    Parameters: {params}")
    
    # Find modalidade variations
    modalidade_resources = [r for r in resources if "_mod" in r["name"]]
    print(f"\nModalidade variations: {len(modalidade_resources)}")
    for resource in modalidade_resources[:3]:  # Show first 3
        print(f"  - {resource['name']}")
        params = resource["endpoint"]["params"]
        print(f"    Parameters: {params}")
    
    return len(resources)

if __name__ == "__main__":
    total_resources = test_parameter_variations()
    print(f"\n✅ Successfully generated {total_resources} resource variations!")