#!/usr/bin/env python3
"""
PNCP Endpoint Testing Script - Fixed Version
Tests each endpoint with appropriate date ranges and parameters
"""

import asyncio
import httpx
import json
from datetime import date, timedelta
from src.baliza.pncp_extractor import PNCP_ENDPOINTS, PNCP_BASE_URL

async def test_endpoint_with_fallbacks(endpoint_config: dict) -> dict:
    """Test a single endpoint with multiple date strategies."""
    
    print(f"\n🧪 Testing endpoint: {endpoint_config['name']}")
    print(f"   Path: {endpoint_config['path']}")
    print(f"   Description: {endpoint_config['description']}")
    
    # Define different date strategies
    today = date.today()
    test_dates = []
    
    if endpoint_config['name'] == 'contratacoes_proposta':
        # This endpoint requires future dates
        test_dates = [
            ("future_date", today + timedelta(days=30)),
            ("far_future", today + timedelta(days=90)),
        ]
    else:
        # Other endpoints work with past dates
        test_dates = [
            ("recent_past", today - timedelta(days=7)),
            ("month_ago", today - timedelta(days=30)),
            ("three_months_ago", today - timedelta(days=90)),
        ]
    
    best_result = None
    
    for date_label, test_date in test_dates:
        print(f"   🗓️  Trying {date_label}: {test_date}")
        
        # Build parameters
        params = {
            "tamanhoPagina": min(10, endpoint_config.get("page_size", 20)),
            "pagina": 1,
        }
        
        # Add date parameters
        if endpoint_config["supports_date_range"]:
            start_date = test_date.strftime("%Y%m%d")
            end_date = test_date.strftime("%Y%m%d")
            params[endpoint_config["date_params"][0]] = start_date
            params[endpoint_config["date_params"][1]] = end_date
        elif endpoint_config.get("requires_single_date", False):
            single_date = test_date.strftime("%Y%m%d")
            params[endpoint_config["date_params"][0]] = single_date
        
        # Add extra parameters
        if "extra_params" in endpoint_config:
            params.update(endpoint_config["extra_params"])
        
        # Make the request
        async with httpx.AsyncClient(
            base_url=PNCP_BASE_URL,
            timeout=30.0,
            headers={
                "User-Agent": "BALIZA/3.0 (Testing)",
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
                    "test_date": test_date.isoformat(),
                    "date_strategy": date_label,
                }
                
                if response.status_code == 200:
                    try:
                        if response.text.strip():  # Check if response has content
                            data = response.json()
                            result["total_records"] = data.get("totalRegistros", 0)
                            result["total_pages"] = data.get("totalPaginas", 0)
                            result["has_data"] = len(data.get("data", [])) > 0
                            result["data_sample"] = data.get("data", [])[:2] if data.get("data") else []
                            
                            if result["total_records"] > 0:
                                print(f"      ✅ SUCCESS: {result['total_records']} records, {result['total_pages']} pages")
                                best_result = result
                                break
                            else:
                                print(f"      ⚠️  No data for this date")
                        else:
                            result["empty_response"] = True
                            print(f"      ⚠️  Empty response (no content)")
                    except Exception as e:
                        result["json_error"] = str(e)
                        result["response_text"] = response.text[:500]
                        print(f"      ⚠️  JSON parse error: {e}")
                else:
                    result["error"] = response.text
                    print(f"      ❌ HTTP {response.status_code}: {response.text[:100]}...")
                    
                if best_result is None:
                    best_result = result
                    
            except Exception as e:
                result = {
                    "endpoint": endpoint_config["name"],
                    "status_code": 0,
                    "success": False,
                    "error": str(e),
                    "params": params,
                    "test_date": test_date.isoformat(),
                    "date_strategy": date_label,
                }
                print(f"      💥 EXCEPTION: {e}")
                
                if best_result is None:
                    best_result = result
        
        # Small delay between attempts
        await asyncio.sleep(0.5)
    
    return best_result

async def test_all_endpoints_fixed():
    """Test all endpoints with improved strategies."""
    print("🚀 Starting PNCP Endpoint Testing (Fixed Version)")
    print(f"📊 Testing {len(PNCP_ENDPOINTS)} endpoints with multiple date strategies")
    
    results = []
    successful = 0
    failed = 0
    
    for endpoint in PNCP_ENDPOINTS:
        result = await test_endpoint_with_fallbacks(endpoint)
        results.append(result)
        
        if result["success"] and result.get("total_records", 0) > 0:
            successful += 1
        else:
            failed += 1
        
        # Delay between endpoints
        await asyncio.sleep(1)
    
    # Summary
    print(f"\n📋 TESTING SUMMARY")
    print(f"✅ Successful with data: {successful}")
    print(f"❌ Failed or no data: {failed}")
    print(f"📊 Total: {len(results)}")
    
    # Detailed results
    print(f"\n📄 DETAILED RESULTS:")
    for result in results:
        if result["success"] and result.get("total_records", 0) > 0:
            status = "✅"
            total_records = result.get("total_records", "?")
            date_strategy = result.get("date_strategy", "unknown")
            print(f"{status} {result['endpoint']}: {total_records} records ({date_strategy})")
        elif result["success"]:
            status = "⚠️ "
            error_info = result.get("json_error", result.get("empty_response", "No data"))
            print(f"{status} {result['endpoint']}: Success but {error_info}")
        else:
            status = "❌"
            error = result.get("error", "Unknown error")[:100]
            print(f"{status} {result['endpoint']}: HTTP {result.get('status_code', 0)} - {error}")
    
    # Save results
    with open("endpoint_test_results_fixed.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: endpoint_test_results_fixed.json")
    
    return results

if __name__ == "__main__":
    asyncio.run(test_all_endpoints_fixed())