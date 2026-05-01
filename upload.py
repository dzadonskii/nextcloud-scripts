#!/usr/bin/env python3
"""
Step 1: Upload files from JuiceFS to S3 as urn:oid:<fileid>
Queries DB directly. Incremental — skips already-uploaded files.

Usage:
  python3 migrate_upload.py
  python3 migrate_upload.py --dry-run
  python3 migrate_upload.py --workers 4
  python3 migrate_upload.py -v

Dependencies (Debian):
  apt install python3 python3-psycopg2 python3-boto3
  # or via pip:
  # pip install psycopg2-binary boto3

Environment:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
  S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY
  PROGRESS_DIR  — directory for progress/log files (default: .)
"""

import os
import sys
import csv
import logging
import argparse
import psycopg2
import boto3
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration (override via environment) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "nextcloud")
DB_PASS = os.environ.get("DB_PASS", "")

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:8333")
S3_BUCKET = os.environ.get("S3_BUCKET", "nextcloud-primary")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")

PROGRESS_DIR = os.environ.get("PROGRESS_DIR", "state")
PROGRESS_FILE = os.path.join(PROGRESS_DIR, "upload_progress.txt")
LOG_FILE = os.path.join(PROGRESS_DIR, "migrate_upload.log")

DEFAULT_WORKERS = 1
MULTIPART_THRESHOLD = 100 * 1024 * 1024
MULTIPART_CHUNKSIZE = 100 * 1024 * 1024
# --- End Configuration ---

OWNER_EXPR = """
    CASE
        WHEN s.id LIKE 'home::%%' THEN 'user'
        WHEN fc.path LIKE '__groupfolders/%%' THEN 'groupfolder'
        WHEN fc.path LIKE 'appdata_%%' THEN 'appdata'
        ELSE 'local'
    END AS type,
    CASE
        WHEN s.id LIKE 'home::%%' THEN REPLACE(s.id, 'home::', '')
        WHEN fc.path LIKE '__groupfolders/%%' THEN SPLIT_PART(fc.path, '/', 2)
        ELSE ''
    END AS owner
"""

SOURCE_PATH_EXPR = """
    CASE
        WHEN s.id LIKE 'home::%%'
        THEN CONCAT('/var/www/html/data/', REPLACE(s.id, 'home::', ''), '/', fc.path)
        WHEN s.id LIKE 'local::%%'
        THEN CONCAT(REPLACE(s.id, 'local::', ''), fc.path)
    END AS source_path
"""

QUERY_FILES = f"""
    SELECT fc.fileid, fc.size, fc.path,
        {OWNER_EXPR},
        {SOURCE_PATH_EXPR}
    FROM oc_filecache fc
    JOIN oc_storages s ON fc.storage = s.numeric_id
    WHERE fc.mimetype != (SELECT id FROM oc_mimetypes WHERE mimetype = 'httpd/unix-directory')
      AND fc.size > 0
      AND (s.id LIKE 'home::%%' OR s.id LIKE 'local::%%')
    ORDER BY fc.fileid
"""

log = logging.getLogger("upload")


def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(),
        ],
    )


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(
            s3={"addressing_style": "virtual"},
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
    )


def load_progress():
    done = set()
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            for line in f:
                line = line.strip()
                if line:
                    done.add(int(line))
    return done


def format_owner(type_, owner):
    if type_ == "user":
        return f"user={owner}"
    if type_ == "groupfolder":
        return f"gf={owner}"
    return type_


def upload_file(row, dry_run=False):
    fileid, expected_size, nc_path, type_, owner, source_path = row
    owner_str = format_owner(type_, owner)
    s3_key = f"urn:oid:{fileid}"

    if not source_path:
        return fileid, owner_str, nc_path, "NO_PATH"
    if not os.path.exists(source_path):
        return fileid, owner_str, nc_path, f"MISSING:{source_path}"

    actual_size = os.path.getsize(source_path)
    if actual_size != expected_size:
        return fileid, owner_str, nc_path, f"SIZE_MISMATCH:expected={expected_size},actual={actual_size}"

    s3 = get_s3_client()
    try:
        resp = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        if resp["ContentLength"] == expected_size:
            return fileid, owner_str, nc_path, "skipped-s3"
    except s3.exceptions.ClientError:
        pass

    if dry_run:
        return fileid, owner_str, nc_path, f"would-upload ({expected_size} bytes)"

    transfer_config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=MULTIPART_THRESHOLD,
        multipart_chunksize=MULTIPART_CHUNKSIZE,
        max_concurrency=4,
    )
    s3.upload_file(source_path, S3_BUCKET, s3_key, Config=transfer_config)
    return fileid, owner_str, nc_path, "uploaded"


def main():
    parser = argparse.ArgumentParser(
        description="Upload Nextcloud files to S3 as urn:oid:<fileid>",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS  — PostgreSQL connection
  S3_ENDPOINT      S3 endpoint URL (default: http://localhost:8333)
  S3_BUCKET        Target bucket (default: nextcloud-primary)
  S3_ACCESS_KEY    S3 access key (required)
  S3_SECRET_KEY    S3 secret key (required)
  PROGRESS_DIR     Directory for progress/log files (default: .)
        """,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be uploaded without uploading")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel uploads (default: {DEFAULT_WORKERS})")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print every file (OK, skipped, and errors)")
    args = parser.parse_args()

    os.makedirs(PROGRESS_DIR, exist_ok=True)
    setup_logging(verbose=args.verbose)

    for var in ("DB_PASS", "S3_ACCESS_KEY", "S3_SECRET_KEY"):
        if not os.environ.get(var):
            log.error(f"{var} environment variable is required")
            sys.exit(1)

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    cur.execute(QUERY_FILES)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    done = load_progress()
    rows = [r for r in rows if r[0] not in done]
    total = len(rows)
    log.info(f"Files to process: {total} (skipping {len(done)} already done)")
    log.info(f"S3 endpoint: {S3_ENDPOINT} bucket: {S3_BUCKET}")

    if total == 0:
        log.info("Nothing to do")
        return

    if args.dry_run:
        log.info("DRY RUN — no files will be uploaded")

    progress_fh = None if args.dry_run else open(PROGRESS_FILE, "a")
    uploaded = 0
    skipped = 0
    errors = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(upload_file, row, args.dry_run): row for row in rows
        }
        for i, future in enumerate(as_completed(futures), 1):
            fileid, owner_str, nc_path, status = future.result()

            if status == "uploaded":
                uploaded += 1
                progress_fh.write(f"{fileid}\n")
                if uploaded % 100 == 0:
                    progress_fh.flush()
                log.debug(f"UPLOADED fileid={fileid} [{owner_str}] {nc_path}")
            elif status.startswith("skipped"):
                skipped += 1
                log.debug(f"SKIPPED  fileid={fileid} [{owner_str}] {nc_path}")
            elif status.startswith("would-upload"):
                uploaded += 1
                log.debug(f"WOULD    fileid={fileid} [{owner_str}] {nc_path} {status}")
            else:
                errors.append((fileid, owner_str, nc_path, status))
                log.warning(f"FAIL     fileid={fileid} [{owner_str}] {nc_path} — {status}")

            if i % 1000 == 0:
                log.info(f"[{i}/{total}] uploaded={uploaded} skipped={skipped} errors={len(errors)}")

    if progress_fh:
        progress_fh.flush()
        progress_fh.close()

    log.info(f"Done: {uploaded} uploaded, {skipped} skipped, {len(errors)} errors")
    if errors:
        err_file = os.path.join(PROGRESS_DIR, "upload_errors.csv")
        with open(err_file, "w") as f:
            writer = csv.writer(f)
            for fid, owner_str, path, err in errors:
                writer.writerow([fid, owner_str, path, err])
        log.info(f"Errors written to {err_file}")


if __name__ == "__main__":
    main()
