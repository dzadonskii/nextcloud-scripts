#!/usr/bin/env python3
"""
Step 2.5: Compute authoritative SHA1 checksums from S3 and write to oc_filecache.

Use this after Step 1 (upload) to establish checksums from the S3 source of truth,
overwriting any disk-computed checksums that may be unreliable (e.g. FUSE cold reads).
No disk access needed — reads entirely from S3.

This is most useful when Step 0 checksums are suspect (JuiceFS cache inconsistency,
FUSE mount issues).  After running this script, Step 2 (verify) will trivially pass
because checksums were computed from the same S3 content — run Step 2 before this
script if you want an independent cross-check.

Usage:
  python3 s3_checksums.py --dry-run
  python3 s3_checksums.py
  python3 s3_checksums.py --workers 4
  python3 s3_checksums.py -v

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
import hashlib
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
PROGRESS_FILE = os.path.join(PROGRESS_DIR, "s3_checksums_progress.txt")
LOG_FILE = os.path.join(PROGRESS_DIR, "migrate_s3_checksums.log")

DEFAULT_WORKERS = 1
BATCH_SIZE = 500
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

QUERY_FILES = f"""
    SELECT fc.fileid, fc.size, fc.path,
        {OWNER_EXPR}
    FROM oc_filecache fc
    JOIN oc_storages s ON fc.storage = s.numeric_id
    WHERE fc.mimetype != (SELECT id FROM oc_mimetypes WHERE mimetype = 'httpd/unix-directory')
      AND fc.size > 0
      AND (s.id LIKE 'home::%%' OR s.id LIKE 'local::%%')
    ORDER BY fc.fileid
"""

log = logging.getLogger("s3_checksums")


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
        config=Config(s3={"addressing_style": "virtual"}),
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


def process_file(row):
    fileid, expected_size, nc_path, type_, owner = row
    owner_str = format_owner(type_, owner)
    s3_key = f"urn:oid:{fileid}"

    s3 = get_s3_client()
    try:
        resp = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
    except Exception:
        return fileid, owner_str, nc_path, None, "MISSING_IN_S3"

    s3_size = resp["ContentLength"]
    if s3_size != expected_size:
        return fileid, owner_str, nc_path, None, \
            f"SIZE_MISMATCH:s3={s3_size},db={expected_size}"

    try:
        sha1 = hashlib.sha1()
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        for chunk in obj["Body"].iter_chunks(chunk_size=8 * 1024 * 1024):
            sha1.update(chunk)
        return fileid, owner_str, nc_path, sha1.hexdigest(), None
    except Exception as e:
        return fileid, owner_str, nc_path, None, f"ERROR:{e}"


def main():
    parser = argparse.ArgumentParser(
        description="Compute authoritative SHA1 from S3 objects and write to oc_filecache",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS  — PostgreSQL connection
  S3_ENDPOINT      S3 endpoint URL (default: http://localhost:8333)
  S3_BUCKET        Source bucket (default: nextcloud-primary)
  S3_ACCESS_KEY    S3 access key (required)
  S3_SECRET_KEY    S3 secret key (required)
  PROGRESS_DIR     Directory for progress/log files (default: .)
        """,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without writing to DB")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print every file, not just errors and summaries")
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

    done = load_progress()
    rows = [r for r in rows if r[0] not in done]
    total = len(rows)
    log.info(f"Files to process: {total} (skipping {len(done)} already done)")
    log.info(f"S3 endpoint: {S3_ENDPOINT} bucket: {S3_BUCKET}")

    if total == 0:
        log.info("Nothing to do")
        conn.close()
        return

    if args.dry_run:
        log.info("DRY RUN — showing first 20 files that would be processed:")
        for row in rows[:20]:
            fileid, size, nc_path, type_, owner = row
            log.info(f"  fileid={fileid} [{format_owner(type_, owner)}] size={size} {nc_path}")
        if total > 20:
            log.info(f"  ... and {total - 20} more")
        conn.close()
        return

    update_cur = conn.cursor()
    progress_fh = open(PROGRESS_FILE, "a")
    updated = 0
    errors = []
    batch = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_file, row): row for row in rows}
        for i, future in enumerate(as_completed(futures), 1):
            fileid, owner_str, nc_path, sha1_hex, err = future.result()

            if err:
                errors.append((fileid, owner_str, nc_path, err))
                log.warning(f"FAIL fileid={fileid} [{owner_str}] {nc_path} — {err}")
            else:
                batch.append((f"SHA1:{sha1_hex}", fileid))
                updated += 1
                log.debug(f"OK   fileid={fileid} [{owner_str}] {nc_path} SHA1:{sha1_hex}")

            if len(batch) >= BATCH_SIZE:
                update_cur.executemany(
                    "UPDATE oc_filecache SET checksum = %s WHERE fileid = %s", batch
                )
                conn.commit()
                for _, fid in batch:
                    progress_fh.write(f"{fid}\n")
                progress_fh.flush()
                batch.clear()

            if i % 5000 == 0:
                log.info(f"  [{i}/{total}] updated={updated} errors={len(errors)}")

    if batch:
        update_cur.executemany(
            "UPDATE oc_filecache SET checksum = %s WHERE fileid = %s", batch
        )
        conn.commit()
        for _, fid in batch:
            progress_fh.write(f"{fid}\n")
        progress_fh.flush()

    progress_fh.close()
    update_cur.close()
    conn.close()

    log.info(f"Done: {updated} updated, {len(errors)} errors out of {total}")
    if errors:
        err_file = os.path.join(PROGRESS_DIR, "s3_checksums_errors.csv")
        with open(err_file, "w") as f:
            writer = csv.writer(f)
            for fid, owner_str, path, err in errors:
                writer.writerow([fid, owner_str, path, err])
        log.info(f"Errors written to {err_file}")
    else:
        log.info("ALL FILES PROCESSED SUCCESSFULLY")


if __name__ == "__main__":
    main()
