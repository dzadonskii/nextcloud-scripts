#!/usr/bin/env python3
"""
Step 2: Verify all files in S3 match expected size and SHA1 checksum.
Queries DB directly for file list and checksums (populated by Step 0).
No JuiceFS access needed — verifies S3 against DB.

Usage:
  python3 migrate_verify.py                    # size + checksum
  python3 migrate_verify.py --size-only        # fast, HEAD requests only
  python3 migrate_verify.py --dry-run          # show stats only
  python3 migrate_verify.py --workers 4
  python3 migrate_verify.py -v

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
import hashlib
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
PROGRESS_FILE = os.path.join(PROGRESS_DIR, "verify_progress.txt")
LOG_FILE = os.path.join(PROGRESS_DIR, "migrate_verify.log")

DEFAULT_WORKERS = 1
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
    SELECT fc.fileid, fc.size, fc.checksum, fc.path,
        {OWNER_EXPR}
    FROM oc_filecache fc
    JOIN oc_storages s ON fc.storage = s.numeric_id
    WHERE fc.mimetype != (SELECT id FROM oc_mimetypes WHERE mimetype = 'httpd/unix-directory')
      AND fc.size > 0
      AND (s.id LIKE 'home::%%' OR s.id LIKE 'local::%%')
    ORDER BY fc.fileid
"""

log = logging.getLogger("verify")


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


def parse_sha1(checksum_field):
    if not checksum_field:
        return None
    for part in checksum_field.split():
        if part.startswith("SHA1:"):
            return part[5:]
    return None


def format_owner(type_, owner):
    if type_ == "user":
        return f"user={owner}"
    if type_ == "groupfolder":
        return f"gf={owner}"
    return type_


def verify_file(row, size_only=False):
    fileid, expected_size, checksum, nc_path, type_, owner = row
    owner_str = format_owner(type_, owner)
    s3_key = f"urn:oid:{fileid}"
    expected_sha1 = parse_sha1(checksum)

    s3 = get_s3_client()

    try:
        resp = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
    except Exception:
        return fileid, owner_str, nc_path, "MISSING_IN_S3"

    s3_size = resp["ContentLength"]
    if s3_size != expected_size:
        return fileid, owner_str, nc_path, f"SIZE_MISMATCH:s3={s3_size},expected={expected_size}"

    if size_only:
        return fileid, owner_str, nc_path, "OK"

    if expected_sha1:
        sha1 = hashlib.sha1()
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        for chunk in obj["Body"].iter_chunks(chunk_size=8 * 1024 * 1024):
            sha1.update(chunk)
        actual_sha1 = sha1.hexdigest()
        if actual_sha1 != expected_sha1:
            return fileid, owner_str, nc_path, f"CHECKSUM_MISMATCH:s3={actual_sha1},db={expected_sha1}"
        return fileid, owner_str, nc_path, "OK"

    return fileid, owner_str, nc_path, "OK_SIZE_ONLY"


def main():
    parser = argparse.ArgumentParser(
        description="Verify S3 uploads against Nextcloud DB checksums",
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
                        help="Show file counts and stats only")
    parser.add_argument("--size-only", action="store_true",
                        help="Skip checksum verification, HEAD requests only")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel verifications (default: {DEFAULT_WORKERS})")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print every file (OK, size-only, and errors)")
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

    has_checksum = sum(1 for r in rows if parse_sha1(r[2]))
    log.info(f"Files to verify: {total} (skipping {len(done)} already done)")
    log.info(f"  with SHA1 checksum: {has_checksum}")
    log.info(f"  size-only (no checksum in DB): {total - has_checksum}")
    log.info(f"S3 endpoint: {S3_ENDPOINT} bucket: {S3_BUCKET}")

    if args.dry_run:
        log.info("DRY RUN — nothing to do")
        return

    if total == 0:
        log.info("Nothing to verify")
        return

    progress_fh = open(PROGRESS_FILE, "a")
    ok = 0
    ok_size_only = 0
    errors = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(verify_file, row, args.size_only): row for row in rows
        }
        for i, future in enumerate(as_completed(futures), 1):
            fileid, owner_str, nc_path, status = future.result()

            if status == "OK":
                ok += 1
                progress_fh.write(f"{fileid}\n")
                log.debug(f"OK        fileid={fileid} [{owner_str}] {nc_path}")
            elif status == "OK_SIZE_ONLY":
                ok_size_only += 1
                progress_fh.write(f"{fileid}\n")
                log.debug(f"OK (size) fileid={fileid} [{owner_str}] {nc_path}")
            else:
                errors.append((fileid, owner_str, nc_path, status))
                log.warning(f"FAIL      fileid={fileid} [{owner_str}] {nc_path} — {status}")

            if i % 5000 == 0:
                progress_fh.flush()
                log.info(f"[{i}/{total}] ok={ok} size_only={ok_size_only} errors={len(errors)}")

    progress_fh.flush()
    progress_fh.close()

    log.info(f"Done: {ok} checksum-verified, {ok_size_only} size-only, {len(errors)} errors")
    if errors:
        err_file = os.path.join(PROGRESS_DIR, "verify_errors.csv")
        with open(err_file, "w") as f:
            writer = csv.writer(f)
            for fid, owner_str, path, err in errors:
                writer.writerow([fid, owner_str, path, err])
        log.info(f"Errors written to {err_file}")
    else:
        log.info("ALL FILES VERIFIED SUCCESSFULLY")


if __name__ == "__main__":
    main()
