# DRIVEUP: Google Drive Incremental Backup Script

This script provides an automated way to back up content from a user's "My Drive" and all accessible "Shared Drives" in Google Drive to local storage, with an optional upload to Amazon S3.

## Core Principle: Efficiency with Changes API

The script leverages the [Google Drive Changes API](https://developers.google.com/drive/api/guides/manage-changes) (`changes.list`). This allows for efficient incremental backups by only processing files and folders that have changed since the last run, avoiding the need for a full scan every time.

-   **First Run:** Performs a full sync, downloads everything, builds the local structure, and saves a state map and a `startPageToken`.
-   **Subsequent Runs:** Uses the saved token to fetch only the delta (new, modified, moved, deleted items) from the Changes API and updates the local backup accordingly.

A full sync is only performed initially or if the change token becomes invalid (e.g., too much time/too many changes have passed, or state files are manually deleted).

## Key Features

-   **Efficient Incremental Backups:** Uses the Google Drive Changes API.
-   **Shared Drive Support:** Backs up "My Drive" and all accessible Shared Drives.
-   **Structure Preservation:** Recreates the Google Drive folder structure locally.
-   **Google Format Conversion:** Converts Google Docs &rarr; `.docx`, Sheets &rarr; `.xlsx`, Presentations &rarr; `.pptx`, Drawings &rarr; `.png`.
-   **Sheets Formulas Export:** Optionally creates a `.csv` file with formulas for each Google Sheet.
-   **Optional S3 Upload:** Automatically uploads downloaded files to a specified S3 bucket.
-   **OAuth 2.0 Authentication:** Uses refresh tokens for persistent authorization.
-   **Dry Run Mode (`--dry-run`):** Test the process without making actual changes.
-   **Configuration via `.env`:** Easily configure paths and options.
-   **Docker Support:** Recommended way to run via `docker-compose`.

## Basic Usage

The script requires specifying a sync mode:

-   `--full`: Forces a full synchronization, deleting previous state.
-   `--incremental`: Performs an incremental backup using saved state (recommended for regular runs). Will perform a full sync if state is missing.

**Recommended execution via Docker Compose:**

```bash
# Ensure .env, credentials.json, token.json are present
# Run a full sync (e.g., first time)
docker compose run --rm driveup python main.py --full

# Run an incremental sync (for regular backups)
docker compose run --rm driveup python main.py --incremental

# Run incremental sync with S3 upload
docker compose run --rm driveup python main.py --incremental --s3-bucket your-bucket-name --s3-prefix your/prefix/
```

## Detailed Documentation

For complete setup instructions (Google Cloud prerequisites, first authorization, configuration options, troubleshooting, and more), please refer to the full documentation:

**[https://eblet.tech/scripts/driveup/docs.html](https://eblet.tech/scripts/driveup/docs.html)**