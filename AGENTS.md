# Developer Notes

## What this is

Python scripts for migrating a Nextcloud instance from local/FUSE filesystem storage to S3-compatible object storage (SeaweedFS, MinIO, AWS S3). There is no official Nextcloud migration path; these scripts handle it in steps:

- **Step 0 (`checksums.py`)** — Backfill/verify SHA1 checksums in `oc_filecache`. Nextcloud does not compute checksums server-side; this script reads files from disk and writes `SHA1:<hex>` to the database. Best-effort when source is FUSE-based.
- **Step 1 (`upload.py`)** — Upload files from disk to S3 as `urn:oid:<fileid>`. Skips already-uploaded files via HEAD check.
- **Step 2 (`verify.py`)** — Verify S3 objects against DB checksums. No disk access needed.
- **Step 2.5 (`s3_checksums.py`, optional)** — Download from S3, compute SHA1, write back to DB as authoritative checksums. Recommended when Step 0 ran against a FUSE source (JuiceFS cold-read mismatches). Run Step 2 first if you want an independent cross-check; after Step 2.5 runs, Step 2 will trivially pass.
- **Step 3** — Manual SQL cutover of `oc_storages` entries (documented in README.md).

## Running the scripts

```bash
# Dependencies
apt install python3-psycopg2 python3-boto3
# or: pip install psycopg2-binary boto3

# Required env vars (all scripts)
export DB_HOST=localhost DB_PORT=5432 DB_NAME=nextcloud DB_USER=nextcloud DB_PASS=secret
export PROGRESS_DIR=./migration

# S3 env vars (upload.py and verify.py only)
export S3_ENDPOINT=http://s3.example.com
export S3_BUCKET=nextcloud-primary
export S3_ACCESS_KEY=key S3_SECRET_KEY=secret

python3 checksums.py backfill --dry-run
python3 checksums.py backfill --workers 4
python3 checksums.py verify
python3 upload.py --dry-run
python3 upload.py --workers 4
python3 verify.py --size-only    # fast, HEAD only
python3 verify.py                # full SHA1 check
python3 s3_checksums.py          # Step 2.5: recompute checksums from S3
```

All scripts share: `--dry-run`, `--workers N`, `-v/--verbose`. All are safe to interrupt and re-run (progress tracked in `$PROGRESS_DIR/*.txt`). Errors are written to `$PROGRESS_DIR/*_errors.csv` as `fileid,owner,path,error`.

## Architecture

All scripts follow the same pattern:
1. Read config from environment variables at module level
2. Query `oc_filecache JOIN oc_storages` directly (no CSV intermediaries)
3. Process with `ThreadPoolExecutor`
4. Write progress to `$PROGRESS_DIR/<name>_progress.txt` (one fileid per line)
5. Write failures to `$PROGRESS_DIR/<name>_errors.csv`

**Key DB schema facts:**
- `oc_filecache`: `fileid`, `path`, `size`, `checksum` (`SHA1:<hex>` format), `storage` (FK to `oc_storages.numeric_id`)
- `oc_storages`: `numeric_id`, `id` (string like `home::alice` or `local::/path/`)
- Directories are excluded via `fc.mimetype != (SELECT id FROM oc_mimetypes WHERE mimetype = 'httpd/unix-directory')`

**Owner classification** (duplicated across scripts as `OWNER_EXPR`):
- `home::alice` → `user`, owner=`alice`
- `local::` + path `__groupfolders/5/…` → `groupfolder`, owner=`5`
- `local::` + path `appdata_…` → `appdata`
- anything else → `local`

**`%%` in SQL LIKE patterns:** psycopg2 treats `%` as a placeholder marker when params are passed to `execute()`. Literal `%` in LIKE patterns must be written `%%`. When `params=None`, psycopg2 sends the query verbatim and PostgreSQL receives `%%`, which it treats as two consecutive wildcards (equivalent to `%`). Either way the correct rows are matched.

**S3 key format:** `urn:oid:<fileid>` — this is Nextcloud's S3 primary storage convention and must not change.

**S3 addressing:** virtual-hosted style (`<bucket>.<endpoint>`). To switch to path-style, change `addressing_style` from `"virtual"` to `"path"` in the boto3 client config in `upload.py`, `verify.py`, and `s3_checksums.py`.
