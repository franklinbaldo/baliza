# Baliza Package Initialization

# This file makes the 'baliza' directory a Python package.

# You can choose to expose certain functions or classes at the package level
# for easier imports, e.g.:
# from .cli import app
# from .pipeline import harvest_endpoint_data

# For now, we'll keep it simple and let users import from the specific modules directly,
# e.g., `from baliza.cli import app` or `from baliza.pipeline import harvest_endpoint_data`.

# Version of the baliza package
__version__ = "0.2.0" # Starting version for refactored code

# Define what `from baliza import *` imports, if anything.
# It's generally better to be explicit and import directly from modules.
# __all__ = ['cli', 'client', 'pipeline', 'storage', 'state'] # Example
__all__ = []

# You could also include package-level setup or logging configuration here if desired,
# though it's often handled in the application entry point (cli.py) or a dedicated config module.

# print("Baliza package loaded") # For debugging, remove in production
