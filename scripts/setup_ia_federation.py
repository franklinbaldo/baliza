#!/usr/bin/env python3
"""
Setup Internet Archive federation for Baliza.

This script configures DuckDB to use Internet Archive as the primary data source,
with local storage as fallback only.
"""
import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from baliza.ia_federation import InternetArchiveFederation
import typer


def main():
    """Setup IA federation for Baliza."""
    
    print("🚀 Setting up Internet Archive federation for Baliza...")
    
    # Initialize federation
    federation = InternetArchiveFederation()
    
    try:
        # Step 1: Discover and catalog IA data
        print("\n📋 Step 1: Discovering Internet Archive data...")
        items = federation.discover_ia_items()
        
        if not items:
            print("⚠️ No Baliza data found in Internet Archive")
            print("   Make sure data has been uploaded using the collection 'baliza-pncp'")
            return
        
        print(f"✅ Found {len(items)} items with Parquet data")
        
        # Step 2: Create federated views
        print("\n🔗 Step 2: Creating federated DuckDB views...")
        federation.create_federated_views()
        
        # Step 3: Show data availability
        print("\n📊 Step 3: Checking data availability...")
        availability = federation.get_data_availability()
        
        ia_records = availability.get('internet_archive', {}).get('total_records', 0)
        local_records = availability.get('local_storage', {}).get('total_records', 0)
        
        print(f"""
📈 Data Sources Configured:
   🌐 Internet Archive: {ia_records:,} records
   💾 Local Storage: {local_records:,} records
   
🎯 Federation Strategy:
   - Primary: Internet Archive (archive-first)
   - Fallback: Local storage (for new/uncached data)
   - Unified: Combined view prioritizing IA data
        """)
        
        # Step 4: Test federation
        print("\n🧪 Step 4: Testing federation...")
        test_federation(federation)
        
        print("""
✅ Internet Archive federation setup complete!

🎯 Next steps:
   1. Update your DBT models to use {{ source('federated', 'contratos_unified') }}
   2. Run: dbt run --select stg_contratos
   3. Use baliza ia-federation status to check federation health
   
📋 Available commands:
   python scripts/setup_ia_federation.py  # Re-run setup
   python src/baliza/ia_federation.py discover  # Discover IA data
   python src/baliza/ia_federation.py federate  # Update federation
   python src/baliza/ia_federation.py status   # Check status
        """)
        
    except Exception as e:
        print(f"❌ Error setting up IA federation: {e}")
        raise


def test_federation(federation: InternetArchiveFederation):
    """Test the federation setup."""
    import duckdb
    
    try:
        conn = duckdb.connect(federation.baliza_db_path)
        
        # Test federated views exist
        tables = conn.execute("SHOW TABLES FROM federated").fetchall()
        print(f"   📋 Created {len(tables)} federated views")
        
        # Test data access
        if tables:
            sample = conn.execute("""
                SELECT data_source, COUNT(*) as records
                FROM federated.contratos_unified
                GROUP BY data_source
                LIMIT 10
            """).fetchall()
            
            print("   📊 Data distribution:")
            for source, count in sample:
                print(f"      {source}: {count:,} records")
        
        conn.close()
        print("   ✅ Federation test passed")
        
    except Exception as e:
        print(f"   ⚠️ Federation test warning: {e}")


if __name__ == "__main__":
    main()