#!/usr/bin/env python3
"""
Final PNCP Endpoint Testing Script
Tests each endpoint with optimized parameters and date ranges
"""

import asyncio
import httpx
import json
from datetime import date, timedelta
from src.baliza.pncp_extractor import PNCP_ENDPOINTS, PNCP_BASE_URL

async def test_endpoint_final(endpoint_config: dict) -> dict:
    """Test endpoint with optimized parameters."""
    
    print(f"\n🧪 Testing endpoint: {endpoint_config['name']}")
    print(f"   Path: {endpoint_config['path']}")
    
    # Set appropriate test dates based on endpoint
    today = date.today()
    
    if endpoint_config['name'] == 'contratacoes_proposta':
        # Future date for proposta endpoint
        test_date = today + timedelta(days=30)
        print(f"   Using future date: {test_date}")
    elif endpoint_config['name'] == 'instrumentoscobranca_inclusao':
        # Use March 2025 date that you confirmed has data
        test_date = date(2025, 3, 15)
        print(f"   Using March 2025 date: {test_date}")
    elif endpoint_config['name'] == 'pca_atualizacao':
        # Try a broader date range for PCA
        test_date = date(2025, 1, 15)
        print(f"   Using January 2025 date: {test_date}")
    else:
        # Recent past for other endpoints
        test_date = today - timedelta(days=7)
        print(f"   Using recent past: {test_date}")
    
    # Build parameters with proper page size handling
    page_size = endpoint_config.get("page_size", 20)
    min_page_size = endpoint_config.get("min_page_size", 1)
    actual_page_size = max(min(10, page_size), min_page_size)  # Test with small but valid page size
    
    params = {
        "tamanhoPagina": actual_page_size,
        "pagina": 1,
    }
    
    # Add date parameters
    if endpoint_config["supports_date_range"]:
        if endpoint_config['name'] == 'instrumentoscobranca_inclusao':
            # Use monthly range for instrumentos cobrança
            start_date = date(2025, 3, 1).strftime("%Y%m%d")
            end_date = date(2025, 3, 31).strftime("%Y%m%d")
        elif endpoint_config['name'] == 'pca_atualizacao':
            # Use broader range for PCA
            start_date = date(2025, 1, 1).strftime("%Y%m%d")
            end_date = date(2025, 1, 31).strftime("%Y%m%d")
        else:
            start_date = test_date.strftime("%Y%m%d")
            end_date = test_date.strftime("%Y%m%d")
        
        params[endpoint_config["date_params"][0]] = start_date
        params[endpoint_config["date_params"][1]] = end_date
        print(f"   Date range: {start_date} to {end_date}")
    elif endpoint_config.get("requires_single_date", False):
        single_date = test_date.strftime("%Y%m%d")
        params[endpoint_config["date_params"][0]] = single_date
        print(f"   Single date: {single_date}")
    
    # Add extra parameters
    if "extra_params" in endpoint_config:
        params.update(endpoint_config["extra_params"])
        print(f"   Extra params: {endpoint_config['extra_params']}")
    
    print(f"   Page size: {actual_page_size} (min: {min_page_size}, max: {page_size})")
    
    # Make the request
    async with httpx.AsyncClient(
        base_url=PNCP_BASE_URL,
        timeout=30.0,
        headers={
            "User-Agent": "BALIZA/3.0 (Final Testing)",
            "Accept": "application/json",
        }
    ) as client:
        try:
            response = await client.get(endpoint_config["path"], params=params)
            
            result = {
                "endpoint": endpoint_config["name"],
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "url": str(response.url),
                "params": params,
            }
            
            if response.status_code == 200:
                try:
                    if response.text.strip():
                        data = response.json()
                        result["total_records"] = data.get("totalRegistros", 0)
                        result["total_pages"] = data.get("totalPaginas", 0)
                        result["has_data"] = len(data.get("data", [])) > 0
                        result["sample_record"] = data.get("data", [{}])[0] if data.get("data") else {}
                        
                        if result["total_records"] > 0:
                            print(f"   ✅ SUCCESS: {result['total_records']} records, {result['total_pages']} pages")
                        else:
                            print(f"   ⚠️  SUCCESS but no data for this date range")
                    else:
                        result["empty_response"] = True
                        print(f"   ⚠️  SUCCESS but empty response")
                except Exception as e:
                    result["json_error"] = str(e)
                    result["response_text"] = response.text[:200]
                    print(f"   ⚠️  SUCCESS but JSON error: {e}")
            else:
                result["error"] = response.text
                print(f"   ❌ FAILED: HTTP {response.status_code}")
                if response.status_code in [400, 422]:
                    print(f"   Error: {response.text[:200]}")
                    
        except Exception as e:
            result = {
                "endpoint": endpoint_config["name"],
                "status_code": 0,
                "success": False,
                "error": str(e),
                "params": params,
            }
            print(f"   💥 EXCEPTION: {e}")
    
    return result

async def test_all_endpoints_final():
    """Final comprehensive test of all endpoints."""
    print("🚀 Final PNCP Endpoint Testing")
    print(f"📊 Testing {len(PNCP_ENDPOINTS)} endpoints with optimized configurations")
    
    results = []
    successful_with_data = 0
    successful_no_data = 0
    failed = 0
    
    for endpoint in PNCP_ENDPOINTS:
        result = await test_endpoint_final(endpoint)
        results.append(result)
        
        if result["success"]:
            if result.get("total_records", 0) > 0:
                successful_with_data += 1
            else:
                successful_no_data += 1
        else:
            failed += 1
        
        # Delay between endpoints to be respectful
        await asyncio.sleep(1)
    
    # Summary
    print(f"\n📋 FINAL TEST SUMMARY")
    print(f"✅ Successful with data: {successful_with_data}")
    print(f"⚠️  Successful but no data: {successful_no_data}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Total tested: {len(results)}")
    
    # Detailed results
    print(f"\n📄 ENDPOINT STATUS:")
    for result in results:
        endpoint_name = result["endpoint"]
        if result["success"]:
            if result.get("total_records", 0) > 0:
                total_records = result["total_records"]
                print(f"✅ {endpoint_name}: {total_records:,} records")
            else:
                print(f"⚠️  {endpoint_name}: Working but no data")
        else:
            status_code = result.get("status_code", 0)
            print(f"❌ {endpoint_name}: HTTP {status_code} - Failed")
    
    # Configuration validation
    print(f"\n🔧 CONFIGURATION ANALYSIS:")
    working_endpoints = [r for r in results if r["success"] and r.get("total_records", 0) > 0]
    print(f"✅ {len(working_endpoints)} endpoints are fully functional")
    
    if failed > 0:
        print(f"❌ {failed} endpoints need configuration fixes")
    
    # Save comprehensive results
    with open("endpoint_test_results_final.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Comprehensive results saved to: endpoint_test_results_final.json")
    
    return results

if __name__ == "__main__":
    asyncio.run(test_all_endpoints_final())