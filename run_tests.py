#!/usr/bin/env python3
"""
Baliza Test Runner - Comprehensive End-to-End Testing

This script runs the complete Baliza test suite to validate all functionality.
"""
import subprocess
import sys
import argparse
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"\n🔍 {description}")
    print(f"📝 Command: {' '.join(cmd)}")
    print("-" * 60)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False, text=True)
        print(f"✅ {description} - PASSED")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - FAILED (exit code: {e.returncode})")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run Baliza end-to-end tests")
    parser.add_argument("--quick", action="store_true", help="Run only quick tests")
    parser.add_argument("--integration", action="store_true", help="Run integration tests")
    parser.add_argument("--performance", action="store_true", help="Run performance tests") 
    parser.add_argument("--category", choices=["federation", "integration", "notebook", "dbt", "actions", "archive"], help="Run specific test category")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Base pytest command
    base_cmd = ["uv", "run", "pytest"]
    if args.verbose:
        base_cmd.append("-v")
    
    # Test configurations
    test_configs = []
    
    if args.category:
        # Run specific category
        category_map = {
            "federation": "tests/test_ia_federation.py",
            "integration": "tests/test_integration.py", 
            "notebook": "tests/test_colab_notebook.py",
            "dbt": "tests/test_dbt_models.py",
            "actions": "tests/test_github_actions.py",
            "archive": "tests/test_archive_first_flow.py"
        }
        
        test_configs.append({
            "cmd": base_cmd + [category_map[args.category]],
            "description": f"{args.category.title()} Tests"
        })
    
    elif args.quick:
        # Quick validation tests
        test_configs.extend([
            {
                "cmd": base_cmd + [
                    "tests/test_ia_federation.py::test_federation_init",
                    "tests/test_integration.py::test_database_initialization", 
                    "tests/test_colab_notebook.py::test_notebook_exists"
                ],
                "description": "Quick Validation Tests"
            }
        ])
    
    elif args.integration:
        # Integration tests
        test_configs.extend([
            {
                "cmd": base_cmd + ["tests/test_integration.py", "--run-integration"],
                "description": "Integration Tests"
            },
            {
                "cmd": base_cmd + ["tests/test_archive_first_flow.py", "--run-integration"],
                "description": "Archive-First Integration Tests"
            }
        ])
    
    elif args.performance:
        # Performance tests
        test_configs.extend([
            {
                "cmd": base_cmd + ["tests/", "-m", "performance", "--run-performance"],
                "description": "Performance Tests"
            }
        ])
    
    else:
        # Full test suite (default)
        test_configs.extend([
            {
                "cmd": base_cmd + ["tests/test_ia_federation.py"],
                "description": "Internet Archive Federation Tests"
            },
            {
                "cmd": base_cmd + ["tests/test_integration.py"],
                "description": "Data Pipeline Integration Tests"
            },
            {
                "cmd": base_cmd + ["tests/test_colab_notebook.py"],
                "description": "Google Colab Notebook Tests"
            },
            {
                "cmd": base_cmd + ["tests/test_dbt_models.py"],
                "description": "DBT Coverage Models Tests"
            },
            {
                "cmd": base_cmd + ["tests/test_github_actions.py"],
                "description": "GitHub Actions Workflow Tests"
            },
            {
                "cmd": base_cmd + ["tests/test_archive_first_flow.py::TestArchiveFirstFlow"],
                "description": "Archive-First Data Flow Tests"
            }
        ])
    
    # Print test plan
    print("🧪 BALIZA END-TO-END TESTING SUITE")
    print("=" * 60)
    print(f"📋 Running {len(test_configs)} test suites")
    print(f"🎯 Test mode: {args.category or ('quick' if args.quick else 'full comprehensive')}")
    
    # Run tests
    passed = 0
    failed = 0
    
    for config in test_configs:
        success = run_command(config["cmd"], config["description"])
        if success:
            passed += 1
        else:
            failed += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST EXECUTION SUMMARY")
    print("=" * 60)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Success Rate: {(passed/(passed+failed)*100):.1f}%" if (passed+failed) > 0 else "N/A")
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED! Baliza is ready for deployment.")
        sys.exit(0)
    else:
        print(f"\n⚠️ {failed} test suite(s) failed. Review the output above for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()