# This file is now a compatibility wrapper.
# The main CLI logic has been moved to cli.py.
# To run the application, use `python -m baliza.cli` or the installed `baliza` command.

from .cli import app

if __name__ == "__main__":
    # This allows running `python src/baliza/main.py ...`
    # and it will behave the same as `python src/baliza/cli.py ...`
    app()
