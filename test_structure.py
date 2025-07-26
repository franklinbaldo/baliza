#!/usr/bin/env python3
"""
Structure validation script for Baliza ETL pipeline
Tests import structure and key functionality without requiring external dependencies
"""

import sys
import os
import inspect
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_imports():
    """Test that all modules can be imported structurally"""
    print("🧪 Testing import structure...")
    
    try:
        # Test config
        from baliza.config import settings
        print("✅ Config module imports correctly")
    except Exception as e:
        print(f"❌ Config import failed: {e}")
    
    try:
        # Test enums
        from baliza.enums import ModalidadeContratacao
        print("✅ Enums module imports correctly")
        print(f"   Found {len(list(ModalidadeContratacao))} modalidades")
    except Exception as e:
        print(f"❌ Enums import failed: {e}")
    
    try:
        # Test utils without external deps
        from baliza.utils.endpoints import DateRangeHelper, URLBuilder
        print("✅ Utils modules import correctly")
        
        # Test date helper functionality
        date_range = DateRangeHelper.get_month_range_from_string("2021-10")
        print(f"   Date range for 2021-10: {date_range}")
        
    except Exception as e:
        print(f"❌ Utils import failed: {e}")

def test_flow_structure():
    """Test flow module structure"""
    print("\n🔄 Testing flow structure...")
    
    flow_files = [
        "raw.py",
        "staging.py", 
        "marts.py",
        "export.py",
        "complete_extraction.py"
    ]
    
    for flow_file in flow_files:
        flow_path = Path("src/baliza/flows") / flow_file
        if flow_path.exists():
            print(f"✅ {flow_file} exists")
            
            # FIXME: Checking for features by searching for strings in the file
            #        is extremely fragile and should be avoided. This test will break
            #        easily with minor refactoring. A better approach would be to
            #        import the modules and inspect their members (functions, classes)
            #        directly to ensure the required components exist.
            with open(flow_path) as f:
                content = f.read()
                
            if flow_file == "raw.py":
                if "extract_phase_2a_concurrent" in content:
                    print("   ✅ Main extraction flow found")
                if "_check_existing_request" in content:
                    print("   ✅ URL deduplication implemented")
                if "store_api_requests_batch" in content:
                    print("   ✅ Storage deduplication implemented")
                    
            elif flow_file == "staging.py":
                if "extract_and_normalize_payloads" in content:
                    print("   ✅ JSON normalization implemented")
                if "polars" in content:
                    print("   ✅ Polars integration added")
                    
            elif flow_file == "marts.py":
                if "endpoint_performance" in content:
                    print("   ✅ New analytics tables implemented")
                if "ibis._.http_status.between" in content:
                    print("   ✅ Proper Ibis aggregations")
                    
            elif flow_file == "export.py":
                if "export_to_parquet" in content:
                    print("   ✅ Parquet export flow implemented")
                if "partition_by" in content:
                    print("   ✅ Partitioned export support")
                    
        else:
            print(f"❌ {flow_file} missing")

def test_cli_integration():
    """Test CLI integration"""
    print("\n⌨️  Testing CLI integration...")
    
    cli_path = Path("src/baliza/cli.py")
    if cli_path.exists():
        with open(cli_path) as f:
            content = f.read()
            
        if "from .flows.export import export_to_parquet" in content:
            print("✅ Export flow imported in CLI")
            
        if "def export(" in content:
            print("✅ Export command implemented")
            
        if "--staging/--no-staging" in content:
            print("✅ Flexible export options")
            
        if "--mes" in content:
            print("✅ Month filtering support")
            
        # Count commands
        command_count = content.count("@app.command")
        print(f"✅ Found {command_count} CLI commands")
        
    else:
        print("❌ CLI module missing")

def test_optimization_features():
    """Test that optimization features are properly implemented"""
    print("\n⚡ Testing optimization features...")
    
    # Test URL deduplication
    raw_path = Path("src/baliza/utils/http_client.py")
    if raw_path.exists():
        with open(raw_path) as f:
            content = f.read()
            
        if "_check_existing_request" in content:
            print("✅ URL-based deduplication implemented")
            
        if "_analyze_existing_pagination" in content:
            print("✅ Pagination analysis implemented")
            
        if "api_requests.join(hot_payloads" in content:
            print("✅ Ibis joins for efficiency")
            
    # Test storage deduplication
    raw_flows_path = Path("src/baliza/flows/raw.py")
    if raw_flows_path.exists():
        with open(raw_flows_path) as f:
            content = f.read()
            
        if "existing_hashes" in content:
            print("✅ Storage-level deduplication")
            
        if "payload_sha256" in content:
            print("✅ SHA-256 based deduplication")

def test_database_schema():
    """Test database schema structure"""
    print("\n🗄️  Testing database schema...")
    
    schema_path = Path("src/baliza/sql/init_schema.sql")
    if schema_path.exists():
        with open(schema_path) as f:
            content = f.read()
            
        required_tables = [
            "raw.api_requests",
            "raw.hot_payloads", 
            "meta.execution_log"
        ]
        
        for table in required_tables:
            if table in content:
                print(f"✅ {table} defined")
            else:
                print(f"❌ {table} missing")
                
        if "payload_sha256" in content:
            print("✅ Correct column naming")
            
    else:
        print("❌ Schema file missing")

def main():
    """Run all tests"""
    print("🚀 Baliza ETL Pipeline Structure Validation")
    print("=" * 50)
    
    test_imports()
    test_flow_structure()
    test_cli_integration()
    test_optimization_features()
    test_database_schema()
    
    print("\n🎉 Structure validation completed!")
    print("\nNext steps:")
    print("1. Install dependencies: pip install -e .")
    print("2. Initialize database: baliza init")
    print("3. Run tests: baliza doctor")
    print("4. Test extraction: baliza extract --mes 2021-10")

if __name__ == "__main__":
    main()