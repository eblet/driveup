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
-   **High-Performance Parallel Mode:** Significantly speeds up backups using multiple workers (`--max-workers`), featuring an adaptive rate limiter to prevent Google API SSL errors.

## High-Performance Parallel Mode

When backing up large amounts of data, processing files sequentially can be time-consuming. 
This script offers a parallel processing mode to significantly speed up the process by using multiple workers. 
However, running too many concurrent requests to the Google Drive API can lead to network-level SSL errors.

To solve this, the script includes an `AdaptiveRateLimiter`.

### Key Capabilities

*   **Concurrent Request Limiting:** Uses a semaphore to control concurrency, limiting simultaneous calls to `max_workers * 2`.
*   **Adaptive Throttling:** Automatically increases delays between API calls when SSL errors are detected and gracefully reduces them once the connection stabilizes.
*   **Overload Protection:** Implements a minimum delay (default: 300ms) and an exponential backoff strategy to prevent API abuse.
*   **Real-time Statistics:** Provides insights into API calls, SSL errors, and the current delay.

### Performance Comparison

| Mode       | Workers | Time  | SSL errors | Recommendation      |
|------------|---------|-------|------------|---------------------|
| Sequential | 1       | ~2.5h | 0          | ✅ Safe             |
| Parallel   | 2       | ~1.5h | <5         | ✅ Optimal          |
| Aggressive | 4       | ~1h   | 20+        | ⚠️ Not Recommended  |

### Best Practices

1.  **Start with 2 workers:** This typically provides the best balance of speed and stability.
2.  **Monitor statistics:** The error rate should ideally stay below 1%.
3.  **Increase workers gradually:** If stable, you can try increasing from 2 to 3, and so on.
4.  **Watch Google API quotas:** The default quota is 1,000 requests per 100 seconds per user.
5.  **If errors >5%:** Reduce the number of workers.

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

# To enable parallel mode, add the --max-workers flag (2 is recommended)
docker compose run --rm driveup python main.py --incremental --max-workers=2

# Run incremental sync with AWS S3 upload
docker compose run --rm driveup python main.py --incremental --s3-bucket your-bucket-name --s3-prefix your/prefix/

# Combine parallel mode with S3 upload
docker compose run --rm driveup python main.py --incremental --max-workers=2 --s3-bucket your-bucket-name --s3-prefix your/prefix/

# Run with S3-compatible storage (non-AWS)
docker compose run --rm driveup python main.py --incremental \
  --s3-bucket your-bucket-name \
  --s3-prefix your/prefix/ \
  --s3-endpoint https://s3.your-provider.com \
  --s3-region your-region \
  --s3-access-key your-access-key \
  --s3-secret-key your-secret-key
```