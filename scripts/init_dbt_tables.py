#!/usr/bin/env python3
"""Initialize DuckDB tables for dbt-driven task planning.

Creates the runtime tables that Python code will write to and dbt will read from.
This script ensures the database schema is ready for the new architecture.

ADR-009: dbt-Driven Task Planning Architecture
"""

import sys
from pathlib import Path

import duckdb

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

# Note: settings import removed as not used in this script


def init_planning_tables(db_path: str = "data/baliza.duckdb") -> None:
    """Initialize planning and runtime tables for dbt-driven task planning."""
    
    print(f"🚀 Initializing dbt task planning tables in {db_path}")
    
    with duckdb.connect(db_path) as conn:
        # Create schemas if they don't exist (using dbt schema naming convention)
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_planning")
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_runtime") 
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_analytics")
        
        print("✅ Created schemas: main_planning, main_runtime, main_analytics")
        
        # Planning schema tables (will be managed by dbt)
        # We create them here just to ensure they exist for the first run
        conn.execute("""
            CREATE TABLE IF NOT EXISTS main_planning.task_plan (
                task_id TEXT PRIMARY KEY,
                endpoint_name TEXT NOT NULL,
                data_date DATE NOT NULL,
                modalidade INTEGER,
                status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'CLAIMED', 'COMPLETED', 'FAILED')),
                plan_fingerprint TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS main_planning.task_plan_meta (
                plan_version TEXT PRIMARY KEY,
                plan_fingerprint TEXT UNIQUE NOT NULL,
                environment TEXT NOT NULL CHECK (environment IN ('dev', 'staging', 'prod')),
                date_range_start DATE NOT NULL,
                date_range_end DATE NOT NULL,
                generated_at TIMESTAMP NOT NULL,
                config_version TEXT NOT NULL,
                task_count INTEGER NOT NULL
            )
        """)
        
        print("✅ Created planning tables: task_plan, task_plan_meta")
        
        # Runtime schema tables (written by Python, read by dbt)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS main_runtime.task_claims (
                claim_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                task_id TEXT NOT NULL,
                claimed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                worker_id TEXT NOT NULL,
                status TEXT DEFAULT 'CLAIMED' CHECK (status IN ('CLAIMED', 'EXECUTING', 'COMPLETED', 'FAILED'))
            )
        """)
        
        # Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_claims_task_id ON main_runtime.task_claims(task_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_claims_status ON main_runtime.task_claims(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_claims_expires_at ON main_runtime.task_claims(expires_at)")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS main_runtime.task_results (
                result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                task_id TEXT NOT NULL,
                request_id UUID,
                page_number INTEGER,
                records_count INTEGER,
                completed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for performance  
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_results_task_id ON main_runtime.task_results(task_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_results_completed_at ON main_runtime.task_results(completed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_results_request_id ON main_runtime.task_results(request_id)")
        
        print("✅ Created runtime tables: task_claims, task_results")
        
        # Add foreign key constraints (DuckDB supports them)
        try:
            # Note: These will fail if tables already exist with data that violates constraints
            # In production, we'd need a proper migration strategy
            conn.execute("""
                ALTER TABLE main_runtime.task_claims 
                ADD CONSTRAINT fk_task_claims_task_id 
                FOREIGN KEY (task_id) REFERENCES main_planning.task_plan(task_id)
            """)
            print("✅ Added foreign key constraint: task_claims -> task_plan")
        except Exception as e:
            print(f"⚠️  Foreign key constraint might already exist: {e}")
        
        try:
            conn.execute("""
                ALTER TABLE main_runtime.task_results 
                ADD CONSTRAINT fk_task_results_task_id 
                FOREIGN KEY (task_id) REFERENCES main_planning.task_plan(task_id)
            """)
            print("✅ Added foreign key constraint: task_results -> task_plan")
        except Exception as e:
            print(f"⚠️  Foreign key constraint might already exist: {e}")
        
        # Note: DuckDB doesn't support stored procedures like PostgreSQL
        # Claim reaper will be implemented as a simple SQL statement in Python
        print("📝 Note: Claim reaper will be implemented as Python function (no stored procedures in DuckDB)")
        
        # Show table counts
        planning_count = conn.execute("SELECT COUNT(*) FROM main_planning.task_plan").fetchone()[0]
        claims_count = conn.execute("SELECT COUNT(*) FROM main_runtime.task_claims").fetchone()[0]
        results_count = conn.execute("SELECT COUNT(*) FROM main_runtime.task_results").fetchone()[0]
        
        print("📊 Current table counts:")
        print(f"   - main_planning.task_plan: {planning_count:,} tasks")
        print(f"   - main_runtime.task_claims: {claims_count:,} claims")
        print(f"   - main_runtime.task_results: {results_count:,} results")
        
        print("\n🎯 Database ready for dbt-driven task planning!")
        print("   Next: Run `dbt run --models planning` to generate initial task plan")


if __name__ == "__main__":
    init_planning_tables()