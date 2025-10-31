# Baliza Documentation

This directory contains the documentation for Baliza, built with MkDocs and Material theme.

## Building the Documentation

### Install Dependencies

```bash
# Install documentation dependencies
uv sync --extra docs
```

### Build Documentation

```bash
# Build static site
uv run mkdocs build

# Build with strict mode (fail on warnings)
uv run mkdocs build --strict
```

The generated site will be in the `site/` directory.

### Serve Documentation Locally

```bash
# Start local development server
uv run mkdocs serve
```

Then open http://127.0.0.1:8000 in your browser. The server will auto-reload when you make changes.

### Preview with Live Reload

```bash
# Serve on a different port
uv run mkdocs serve -a localhost:8080
```

## Documentation Structure

```
docs/
├── index.md                    # Homepage
├── getting-started/            # Getting started guides
│   ├── installation.md         # Installation instructions
│   ├── quickstart.md           # Quick start guide
│   └── configuration.md        # Configuration guide
├── api/                        # API reference (auto-generated from docstrings)
│   ├── cli.md                  # CLI module
│   ├── pipelines.md            # Pipelines module
│   ├── state.md                # State management module
│   └── utils.md                # Utilities module
├── technical/                  # Technical documentation
│   ├── endpoint_extraction_strategy.md
│   ├── request-deduplication-strategy.md
│   ├── extraction_resumability_plan.md
│   └── ...
├── baliza-site/                # Baliza Site documentation
│   └── index.md
├── openapi/                    # OpenAPI specs
│   └── MANUAL-PNCP-CONSULSTAS-VERSAO-1.md
├── ARCHITECTURE.md             # System architecture
└── ROADMAP.md                  # Project roadmap
```

## Writing Documentation

### Adding a New Page

1. Create a new markdown file in the appropriate directory
2. Add it to the `nav` section in `mkdocs.yml`
3. Use standard Markdown syntax with MkDocs extensions

### API Documentation

API documentation is auto-generated from docstrings using mkdocstrings. Use Google-style docstrings:

```python
def my_function(arg1: str, arg2: int) -> bool:
    """Short description.

    Longer description with more details about what the function does.

    Args:
        arg1: Description of arg1
        arg2: Description of arg2

    Returns:
        Description of return value

    Raises:
        ValueError: When something goes wrong

    Examples:
        >>> my_function("test", 42)
        True
    """
    pass
```

### Markdown Extensions

The documentation supports these extensions:

- **Code blocks with syntax highlighting:**
  ````markdown
  ```python
  print("Hello, world!")
  ```
  ````

- **Admonitions:**
  ```markdown
  !!! note "Optional Title"
      Content here
  ```

- **Tabs:**
  ```markdown
  === "Tab 1"
      Content 1

  === "Tab 2"
      Content 2
  ```

- **Tables, task lists, and more**

See the [Material for MkDocs documentation](https://squidfunk.github.io/mkdocs-material/) for more features.

## Deploying

### GitHub Pages

To deploy to GitHub Pages:

```bash
# Deploy to gh-pages branch
uv run mkdocs gh-deploy
```

### Manual Deployment

Build the site and deploy the `site/` directory to your hosting provider:

```bash
uv run mkdocs build
# Upload the site/ directory to your web server
```

## Configuration

Documentation configuration is in `mkdocs.yml` at the project root. Key sections:

- `site_name`, `site_description`, `site_author` - Site metadata
- `theme` - Theme configuration (Material for MkDocs)
- `plugins` - Enabled plugins (search, autorefs, mkdocstrings)
- `markdown_extensions` - Enabled Markdown extensions
- `nav` - Navigation structure

## Troubleshooting

### Missing Module Errors

If you get import errors when building API docs, make sure the package is installed:

```bash
uv sync --extra docs
```

### Broken Links

Run build in strict mode to catch broken links:

```bash
uv run mkdocs build --strict
```

### Formatting Issues

Install Ruff for better signature formatting in API docs (already included in docs dependencies):

```bash
uv sync --extra docs
```

## Resources

- [MkDocs Documentation](https://www.mkdocs.org/)
- [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/)
- [mkdocstrings](https://mkdocstrings.github.io/)
