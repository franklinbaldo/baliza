# Baliza Daily Automation Setup

This guide covers setting up automated daily extraction, export, and upload to Internet Archive for Baliza.

## Quick Start with GitHub Actions

GitHub Actions is the recommended approach as it requires no server setup.

### Prerequisites

1. GitHub repository with Actions enabled
2. Internet Archive S3 credentials (AWS access key)
3. (Optional) Slack webhook for notifications

### Step 1: Add GitHub Secrets

Go to **Settings → Secrets and Variables → Actions** and add:

```
IA_ACCESS_KEY_ID       # Get from https://archive.org/account/s3.php
IA_SECRET_ACCESS_KEY   # Get from https://archive.org/account/s3.php
IA_BUCKET              # (Optional) Default: jules-mail-frank-2026
SLACK_WEBHOOK_URL      # (Optional) For Slack notifications
```

### Step 2: Configure the Workflow

The workflow is already configured at `.github/workflows/daily-extract.yml`

**Default Schedule:** Daily at 03:00 UTC

To change the schedule, edit the `cron` expression in the workflow file:

```yaml
on:
  schedule:
    - cron: '0 3 * * *'  # Change time here (HH MM format)
```

Common schedules:
- `0 3 * * *` → Daily at 03:00 UTC
- `0 2 * * *` → Daily at 02:00 UTC
- `0 */6 * * *` → Every 6 hours
- `0 0 * * 0` → Weekly on Sunday

### Step 3: Manual Trigger (Optional)

You can manually run the workflow anytime:

1. Go to **Actions → Daily Baliza Extract and Upload**
2. Click **Run workflow**
3. Choose "Dry run" option if you want to test without uploading

### Step 4: Monitor Execution

- Workflow logs: **Actions** tab in GitHub
- Uploaded files: https://archive.org/details/YOUR_BUCKET_NAME
- Slack notifications (if configured)

## How It Works

### Pipeline Steps

```
1. Extract yesterday's data
   ↓
2. Export to Parquet (contratos, orgaos, unidades)
   ↓
3. Upload to Internet Archive S3
   ↓
4. Record metadata in database
```

### What Gets Uploaded

For each day:
- `contratos.parquet` - Main contracts table
- `orgaos.parquet` - Deduplicated organizations
- `unidades.parquet` - Organizational units
- `_metadata.json` - Schema version and statistics

All files are uploaded to your IA bucket with metadata:
- `date: YYYY-MM-DD`
- `source: https://pncp.gov.br/api/consulta/v1`

### Exit Codes

- `0` - Success
- `1` - Partial failure (extraction/export succeeded, upload failed)
- `2` - Complete failure
- `3` - Configuration error

## Troubleshooting

### Workflow fails immediately

**Check:** Are secrets configured?
```
Settings → Secrets and Variables → Actions
```

Verify you have:
- `IA_ACCESS_KEY_ID`
- `IA_SECRET_ACCESS_KEY`

### Upload fails with 403

**Check:** Is your access key still valid?
1. Go to https://archive.org/account/s3.php
2. Generate new credentials if needed
3. Update GitHub secrets

### No data in export

**Check:** Has data been extracted?
1. Run workflow manually with `--dry-run`
2. Check if `baliza.duckdb` exists and has data
3. Verify date fields are populated (should be fixed by now)

### Internet Archive bucket not found

**Check:** Verify bucket name is correct
1. Default: `jules-mail-frank-2026`
2. Custom: Set `IA_BUCKET` secret

If creating a new bucket:
1. Go to https://archive.org/create/
2. Create an identifier (e.g., `baliza-pncp-data`)
3. Add to GitHub secrets

## Advanced Configuration

### Environment Variables

The pipeline respects these environment variables:

```bash
BALIZA_DB_PATH              # Database file (default: baliza.duckdb)
BALIZA_EXPORT_DIR           # Export directory (default: data/daily)
BALIZA_UPLOAD_TO_IA         # Enable upload (default: true)
BALIZA_CLEANUP_AFTER_UPLOAD # Delete temp files after upload (default: true)
BALIZA_DRY_RUN              # Test without uploading (default: false)
```

To modify in workflow, edit `.github/workflows/daily-extract.yml` and add to `env`:

```yaml
env:
  BALIZA_DB_PATH: /custom/path/baliza.duckdb
```

### Slack Notifications

If `SLACK_WEBHOOK_URL` secret is configured, you'll get:
- ✅ Success notifications
- ❌ Failure notifications

To set up Slack:
1. Create incoming webhook: https://api.slack.com/messaging/webhooks
2. Add webhook URL to GitHub secrets: `SLACK_WEBHOOK_URL`
3. Workflow will automatically send notifications

## Monitoring

### View Workflow Runs

```
Repository → Actions → Daily Baliza Extract and Upload
```

Click any run to see:
- Execution time
- Log output
- Success/failure status
- Uploaded artifacts

### Download Export Data

```
Actions → [Latest Run] → Artifacts → daily-pipeline-logs
```

Downloads all exported Parquet files for inspection.

### Check Internet Archive

View uploaded data:
```
https://archive.org/details/BUCKET_NAME
```

Replace `BUCKET_NAME` with your configured bucket.

## Changing the Schedule

To change when the pipeline runs:

**Option 1: Via GitHub UI**
1. Go to `.github/workflows/daily-extract.yml`
2. Edit the `cron` expression
3. Commit changes

**Option 2: Disable automated run**

Replace:
```yaml
on:
  schedule:
    - cron: '0 3 * * *'
  workflow_dispatch:
```

With:
```yaml
on:
  workflow_dispatch:  # Manual only
```

Then run manually via Actions tab.

## For CausaGanha Users

Baliza runs at 03:00 UTC (after CausaGanha's 02:00 UTC run). If you need different timing:

1. Edit `.github/workflows/daily-extract.yml`
2. Change the cron schedule
3. Commit and push

## Example: Complete Setup

```bash
# 1. Clone repository
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# 2. Set up Python environment
uv sync

# 3. Test extraction locally
uv run baliza extract --start 2026-01-22 --end 2026-01-23

# 4. Add secrets to GitHub:
#    - IA_ACCESS_KEY_ID
#    - IA_SECRET_ACCESS_KEY
#    - IA_BUCKET (optional)
#    - SLACK_WEBHOOK_URL (optional)

# 5. Trigger workflow manually to test
#    Go to Actions → Daily Baliza Extract and Upload → Run workflow

# 6. Check results at https://archive.org/details/YOUR_BUCKET
```

## Support

For issues or questions:
1. Check GitHub Actions logs: **Actions** tab
2. Review this guide's troubleshooting section
3. Create a GitHub issue: https://github.com/franklinbaldo/baliza/issues

## Related Documentation

- [README.md](../../README.md) - Main project documentation
- [Baliza CLI](../../README.md#comandos-disponíveis) - Available commands
- [Internet Archive S3](https://archive.org/account/s3.php) - Manage credentials
