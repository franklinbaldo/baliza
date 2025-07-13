#!/usr/bin/env python3
"""
Baliza Code Quality Runner

Comprehensive linting, formatting, and quality checks for the Baliza project.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def run_command(cmd: list[str], description: str, check: bool = True) -> dict[str, Any]:
    """Run a command and return results."""
    print(f"\n🔍 {description}")
    print(f"📝 Command: {' '.join(cmd)}")
    print("-" * 60)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
            cwd=Path(__file__).parent.parent,
        )

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)

        status = "✅ PASSED" if result.returncode == 0 else "❌ FAILED"
        print(f"{status} - {description}")

        return {
            "description": description,
            "command": " ".join(cmd),
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "success": result.returncode == 0,
        }

    except subprocess.CalledProcessError as e:
        print(f"❌ FAILED - {description}")
        if e.stdout:
            print(e.stdout)
        if e.stderr:
            print(e.stderr)

        return {
            "description": description,
            "command": " ".join(cmd),
            "returncode": e.returncode,
            "stdout": e.stdout or "",
            "stderr": e.stderr or "",
            "success": False,
        }


def main():
    parser = argparse.ArgumentParser(description="Run Baliza code quality checks")
    parser.add_argument(
        "--fix", action="store_true", help="Auto-fix issues where possible"
    )
    parser.add_argument("--check", action="store_true", help="Check only, don't fix")
    parser.add_argument("--format", action="store_true", help="Run formatting only")
    parser.add_argument("--lint", action="store_true", help="Run linting only")
    parser.add_argument(
        "--type-check", action="store_true", help="Run type checking only"
    )
    parser.add_argument(
        "--security", action="store_true", help="Run security checks only"
    )
    parser.add_argument("--test", action="store_true", help="Run tests only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--output", "-o", help="Output results to JSON file")

    args = parser.parse_args()

    # Determine what to run
    run_all = not any(
        [args.format, args.lint, args.type_check, args.security, args.test]
    )

    results = []
    failed_checks = []

    print("🧪 BALIZA CODE QUALITY RUNNER")
    print("=" * 60)

    # Formatting
    if run_all or args.format:
        if args.fix:
            results.append(
                run_command(
                    ["uv", "run", "ruff", "format", "."], "Ruff Formatting (Auto-fix)"
                )
            )
        else:
            results.append(
                run_command(
                    ["uv", "run", "ruff", "format", "--check", "--diff", "."],
                    "Ruff Formatting Check",
                    check=False,
                )
            )

    # Linting
    if run_all or args.lint:
        ruff_cmd = ["uv", "run", "ruff", "check", "."]
        if args.fix:
            ruff_cmd.append("--fix")
        if args.verbose:
            ruff_cmd.extend(["--output-format", "full"])
        else:
            ruff_cmd.extend(["--output-format", "concise"])

        results.append(
            run_command(
                ruff_cmd,
                "Ruff Linting" + (" (Auto-fix)" if args.fix else ""),
                check=False,
            )
        )

    # Import sorting check
    if run_all or args.lint:
        results.append(
            run_command(
                ["uv", "run", "ruff", "check", "--select", "I", "."],
                "Import Sorting Check",
                check=False,
            )
        )

    # Type checking
    if run_all or args.type_check:
        results.append(
            run_command(
                ["uv", "run", "mypy", "src/", "--show-error-codes"],
                "MyPy Type Checking",
                check=False,
            )
        )

    # Security checks
    if run_all or args.security:
        results.append(
            run_command(
                ["uv", "run", "bandit", "-r", "src/", "-f", "txt"],
                "Bandit Security Check",
                check=False,
            )
        )

    # Tests
    if run_all or args.test:
        test_cmd = ["uv", "run", "pytest", "tests/"]
        if args.verbose:
            test_cmd.extend(["-v", "--tb=short"])
        else:
            test_cmd.extend(["-q", "--tb=line"])

        results.append(run_command(test_cmd, "PyTest Test Suite", check=False))

    # Code complexity check
    if run_all:
        results.append(
            run_command(
                [
                    "uv",
                    "run",
                    "python",
                    "-c",
                    """
import ast
import glob
import sys

def get_complexity(node):
    complexity = 1
    for child in ast.walk(node):
        if isinstance(child, (ast.If, ast.While, ast.For, ast.comprehension)):
            complexity += 1
        elif isinstance(child, ast.BoolOp):
            complexity += len(child.values) - 1
    return complexity

high_complexity = []
for file in glob.glob('src/**/*.py', recursive=True):
    try:
        with open(file, 'r') as f:
            tree = ast.parse(f.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                complexity = get_complexity(node)
                if complexity > 10:
                    high_complexity.append((file, node.name, complexity))
    except Exception:
        pass

if high_complexity:
    print('Functions with high complexity (>10):')
    for file, func, complexity in high_complexity:
        print(f'  {file}:{func} - {complexity}')
    sys.exit(1)
else:
    print('✅ No functions with high complexity found')
            """,
                ],
                "Code Complexity Check",
                check=False,
            )
        )

    # Summary
    print("\n" + "=" * 60)
    print("📊 QUALITY CHECK SUMMARY")
    print("=" * 60)

    passed = sum(1 for r in results if r["success"])
    failed = len(results) - passed

    for result in results:
        status = "✅" if result["success"] else "❌"
        print(f"{status} {result['description']}")

        if not result["success"]:
            failed_checks.append(result["description"])

    print(f"\n📈 Results: {passed} passed, {failed} failed")

    if failed_checks:
        print("\n⚠️ Failed checks:")
        for check in failed_checks:
            print(f"  - {check}")

    # Output to JSON if requested
    if args.output:
        output_data = {
            "summary": {
                "total": len(results),
                "passed": passed,
                "failed": failed,
                "success_rate": f"{(passed / len(results) * 100):.1f}%"
                if results
                else "0%",
            },
            "results": results,
            "failed_checks": failed_checks,
        }

        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\n📁 Results saved to {args.output}")

    # Exit with error if any checks failed
    if failed_checks and not args.fix:
        print("\n💡 Tip: Run with --fix to auto-fix some issues")
        sys.exit(1)
    else:
        print("\n🎉 All quality checks passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
