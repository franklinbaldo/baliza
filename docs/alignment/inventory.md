# Baliza Project Inventory

This document provides the canonical commands for building, testing, and running the Baliza project.

## Dependency Management

To install all project dependencies, including those for development and testing, run:

```bash
uv sync --all-extras
```

## Running Tests

To execute the full test suite, use the following command:

```bash
uv run pytest
```
