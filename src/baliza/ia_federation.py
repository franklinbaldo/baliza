import typer
import json

app = typer.Typer(help="Internet Archive Federation for Baliza")

@app.command()
def discover():
    """Discover available data in Internet Archive."""
    federation = InternetArchiveFederation()
    items = federation.discover_ia_items()

    print(f"\n📋 Found {len(items)} items:")
    for item in items[:10]:  # Show first 10
        print(f"  🗂️ {item['identifier']} ({len(item['files'])} Parquet files)")

@app.command("update-catalog")
def update_catalog_command():
    """Update federated views with Internet Archive data."""
    federation = InternetArchiveFederation()
    federation.update_federation()

@app.command()
def status():
    """Show federation status and data availability."""
    federation = InternetArchiveFederation()
    availability = federation.get_data_availability()

    print(json.dumps(availability, indent=2, default=str))

if __name__ == "__main__":
    app()