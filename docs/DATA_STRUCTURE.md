# BALIZA Data Directory Structure

## 📁 **New Organized Structure**

BALIZA now follows XDG Base Directory standards for better organization:

### 🏠 **Development Mode** (default)
When running locally for development:
```
baliza/
├── data/                          # Main data directory
│   ├── baliza.duckdb             # Main database
│   ├── ia_catalog.duckdb         # Internet Archive catalog
│   └── parquet_files/            # Generated Parquet files
│       ├── pncp-contratos-*.parquet
│       └── ...
├── .cache/                       # Cache directory  
│   └── ia_cache/                 # Internet Archive file cache
│       └── *.parquet
└── .config/                      # Configuration
    └── ia_federation_last_update # IA federation timestamp
```

### 🌐 **Production Mode** (`BALIZA_PRODUCTION=1`)
When deployed in production:
```
~/.local/share/baliza/            # User data directory
├── baliza.duckdb
├── ia_catalog.duckdb  
└── parquet_files/

~/.cache/baliza/                  # User cache directory
└── ia_cache/

~/.config/baliza/                 # User config directory
└── ia_federation_last_update
```

## 🔧 **Usage**

### Development
```bash
# Uses local ./data/ directory
uv run baliza --auto
```

### Production
```bash
# Uses ~/.local/share/baliza/ 
BALIZA_PRODUCTION=1 uv run baliza --auto
```

## 🧹 **Migration from Old Structure**

### Old Structure (deprecated)
```
baliza/
├── state/                        # ❌ DEPRECATED
├── baliza_data/                  # ❌ DEPRECATED  
├── src/state/                    # ❌ DEPRECATED
└── src/baliza_data/              # ❌ DEPRECATED
```

### Benefits of New Structure
- ✅ **XDG Standard**: Follows Linux/Unix conventions
- ✅ **Clean Separation**: Data, cache, and config separated
- ✅ **Production Ready**: Works in user directories
- ✅ **No Path Dependencies**: Consistent regardless of working directory
- ✅ **Backup Friendly**: Easy to backup just the data directory
- ✅ **Multi-User**: Each user has their own data

## 🗂️ **File Types**

| Directory | Contents | Purpose |
|-----------|----------|---------|
| `data/` | `*.duckdb`, `parquet_files/` | Persistent historical archive |
| `.cache/` | `ia_cache/` | Temporary downloads, safe to delete |
| `.config/` | Settings, timestamps | Configuration state |

## 🔒 **Backup Recommendations**

For complete BALIZA backup, save:
- **Essential**: `data/` directory (contains all historical data)
- **Optional**: `.config/` directory (federation settings)
- **Skip**: `.cache/` directory (temporary files)

Example backup:
```bash
# Development
tar -czf baliza-backup.tar.gz data/ .config/

# Production  
tar -czf baliza-backup.tar.gz ~/.local/share/baliza/ ~/.config/baliza/
```