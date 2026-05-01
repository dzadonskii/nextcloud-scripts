#!/usr/bin/env python3
"""
Step 0: Backfill and verify SHA1 checksums in oc_filecache.

Modes:
  backfill  — compute SHA1 for files missing checksums, write to DB
  verify    — verify existing checksums match files on disk
  all       — backfill first, then verify

Incremental — tracks progress, safe to re-run.

Usage:
  python3 migrate_checksums.py backfill
  python3 migrate_checksums.py verify
  python3 migrate_checksums.py all
  python3 migrate_checksums.py backfill --dry-run
  python3 migrate_checksums.py backfill --workers 4
  python3 migrate_checksums.py backfill -v
  # Re-process specific fileids from a CSV (one fileid per line, or CSV with
  # fileid as first column — error CSVs from previous runs work directly).
  # Use --force to re-process even if fileid is in the progress file.
  python3 migrate_checksums.py backfill --from-csv verify_errors.csv --force

Dependencies (Debian):
  apt install python3 python3-psycopg2
  # or via pip:
  # pip install psycopg2-binary

Environment:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
  PROGRESS_DIR  — directory for progress/log files (default: .)
"""

import os
import sys
import csv
import hashlib
import logging
import argparse
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration (override via environment) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "nextcloud")
DB_PASS = os.environ.get("DB_PASS", "")

PROGRESS_DIR = os.environ.get("PROGRESS_DIR", "state")
PROGRESS_FILE_BACKFILL = os.path.join(PROGRESS_DIR, "checksum_backfill_progress.txt")
PROGRESS_FILE_VERIFY = os.path.join(PROGRESS_DIR, "checksum_verify_progress.txt")
LOG_FILE = os.path.join(PROGRESS_DIR, "migrate_checksums.log")

DEFAULT_WORKERS = 1
BATCH_SIZE = 500
# --- End Configuration ---

# Owner classification:
#   home::alice                    → type='user',        owner='alice'
#   local:: + path __groupfolders/5/... → type='groupfolder', owner='5'
#   local:: + path appdata_*       → type='appdata',     owner=''
#   local:: + anything else        → type='local',       owner=''
#
# %% is required: psycopg2 treats % as a placeholder marker when params are
# passed to execute(), so literal SQL % in LIKE patterns must be written %%.
# When params=None, psycopg2 sends the query verbatim and PostgreSQL receives
# %% in the LIKE pattern, which it treats as two consecutive wildcards (= same
# as %).  Either way the correct rows are matched.
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

QUERY_BASE_MISSING = f"""
    SELECT fc.fileid, fc.size, fc.path,
        {OWNER_EXPR},
        {SOURCE_PATH_EXPR}
    FROM oc_filecache fc
    JOIN oc_storages s ON fc.storage = s.numeric_id
    WHERE (fc.checksum IS NULL OR fc.checksum = '')
      AND fc.mimetype != (SELECT id FROM oc_mimetypes WHERE mimetype = 'httpd/unix-directory')
      AND fc.size > 0
      AND (s.id LIKE 'home::%%' OR s.id LIKE 'local::%%')
"""

# Used with --force: same as QUERY_BASE_MISSING but without the checksum filter,
# so files that already have a checksum are also returned and overwritten.
QUERY_BASE_ALL = f"""
    SELECT fc.fileid, fc.size, fc.path,
        {OWNER_EXPR},
        {SOURCE_PATH_EXPR}
    FROM oc_filecache fc
    JOIN oc_storages s ON fc.storage = s.numeric_id
    WHERE fc.mimetype != (SELECT id FROM oc_mimetypes WHERE mimetype = 'httpd/unix-directory')
      AND fc.size > 0
      AND (s.id LIKE 'home::%%' OR s.id LIKE 'local::%%')
"""

QUERY_BASE_EXISTING = f"""
    SELECT fc.fileid, fc.size, fc.checksum, fc.path,
        {OWNER_EXPR},
        {SOURCE_PATH_EXPR}
    FROM oc_filecache fc
    JOIN oc_storages s ON fc.storage = s.numeric_id
    WHERE fc.checksum IS NOT NULL AND fc.checksum != ''
      AND fc.mimetype != (SELECT id FROM oc_mimetypes WHERE mimetype = 'httpd/unix-directory')
      AND fc.size > 0
      AND (s.id LIKE 'home::%%' OR s.id LIKE 'local::%%')
"""

# For --from-csv: fetch specific fileids
QUERY_BASE_BY_IDS = f"""
    SELECT fc.fileid, fc.size, fc.path,
        {OWNER_EXPR},
        {SOURCE_PATH_EXPR}
    FROM oc_filecache fc
    JOIN oc_storages s ON fc.storage = s.numeric_id
    WHERE fc.fileid = ANY(%s)
      AND fc.mimetype != (SELECT id FROM oc_mimetypes WHERE mimetype = 'httpd/unix-directory')
      AND fc.size > 0
      AND (s.id LIKE 'home::%%' OR s.id LIKE 'local::%%')
"""


def build_query(base_query, path_filter=None, user=None):
    """Append optional filters and ORDER BY."""
    q = base_query
    if user:
        q += "\n      AND s.id = 'home::' || %s"
    if path_filter:
        q += "\n      AND fc.path LIKE %s"
    q += "\n    ORDER BY fc.fileid"
    return q


def build_params(base_params=None, user=None, path_filter=None):
    """Build parameter list matching build_query filter order."""
    params = list(base_params) if base_params else []
    if user:
        params.append(user)
    if path_filter:
        params.append(path_filter)
    return tuple(params) if params else None

log = logging.getLogger("checksums")


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


def load_progress(path):
    done = set()
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    done.add(int(line))
    return done


def load_fileids_from_csv(csv_path):
    """Read fileids from a CSV. Accepts:
      - plain list: one fileid per line
      - CSV with fileid as first column (e.g. error CSVs from previous runs)
    Lines where first column isn't an integer are skipped.
    """
    ids = []
    with open(csv_path) as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            try:
                ids.append(int(row[0].strip()))
            except (ValueError, IndexError):
                continue
    return ids


def compute_sha1(source_path):
    sha1 = hashlib.sha1()
    with open(source_path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            sha1.update(chunk)
    return sha1.hexdigest()


def parse_sha1(checksum_field):
    """Extract SHA1 from 'SHA1:<hex>' or 'SHA1:<hex> MD5:<hex> ...'"""
    if not checksum_field:
        return None
    for part in checksum_field.split():
        if part.startswith("SHA1:"):
            return part[5:]
    return None


def format_owner(type_, owner, nc_path):
    """Display-friendly owner string for log lines."""
    if type_ == "user":
        return f"user={owner}"
    if type_ == "groupfolder":
        return f"gf={owner}"
    return type_  # 'appdata' or 'local'


def process_file_backfill(row):
    fileid, size, nc_path, type_, owner, source_path = row
    owner_str = format_owner(type_, owner, nc_path)

    if not source_path:
        return fileid, owner_str, nc_path, None, "NO_PATH"
    if not os.path.exists(source_path):
        return fileid, owner_str, nc_path, None, f"MISSING:{source_path}"
    try:
        actual_size = os.path.getsize(source_path)
        if actual_size != size:
            return fileid, owner_str, nc_path, None, f"SIZE_MISMATCH:db={size},disk={actual_size}"
        sha1_hex = compute_sha1(source_path)
        return fileid, owner_str, nc_path, sha1_hex, None
    except Exception as e:
        return fileid, owner_str, nc_path, None, f"ERROR:{e}"


def process_file_verify(row):
    fileid, size, checksum, nc_path, type_, owner, source_path = row
    owner_str = format_owner(type_, owner, nc_path)

    if not source_path:
        return fileid, owner_str, nc_path, "NO_PATH"
    if not os.path.exists(source_path):
        return fileid, owner_str, nc_path, f"MISSING:{source_path}"
    try:
        actual_size = os.path.getsize(source_path)
        if actual_size != size:
            return fileid, owner_str, nc_path, f"SIZE_MISMATCH:db={size},disk={actual_size}"
        expected_sha1 = parse_sha1(checksum)
        if not expected_sha1:
            return fileid, owner_str, nc_path, f"UNPARSEABLE_CHECKSUM:{checksum}"
        actual_sha1 = compute_sha1(source_path)
        if actual_sha1 != expected_sha1:
            return fileid, owner_str, nc_path, f"CHECKSUM_MISMATCH:db={expected_sha1},disk={actual_sha1}"
        return fileid, owner_str, nc_path, "OK"
    except Exception as e:
        return fileid, owner_str, nc_path, f"ERROR:{e}"


def run_backfill(workers, dry_run=False, from_csv=None, force=False,
                 path_filter=None, user=None):
    if not DB_PASS:
        log.error("DB_PASS environment variable is required")
        sys.exit(1)

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)
    cur = conn.cursor()

    if user:
        log.info(f"User filter: {user}")
    if path_filter:
        log.info(f"Path filter: {path_filter}")

    if from_csv:
        fileids = load_fileids_from_csv(from_csv)
        log.info(f"Loaded {len(fileids)} fileids from {from_csv}")
        if not fileids:
            log.warning("No valid fileids found in CSV")
            conn.close()
            return
        query = build_query(QUERY_BASE_BY_IDS, path_filter, user)
        params = build_params([fileids], user, path_filter)
        cur.execute(query, params)
    else:
        base = QUERY_BASE_ALL if force else QUERY_BASE_MISSING
        query = build_query(base, path_filter, user)
        params = build_params(user=user, path_filter=path_filter)
        cur.execute(query, params)

    rows = cur.fetchall()
    cur.close()

    if force:
        log.info("--force: ignoring progress file, re-processing all listed files")
        done = set()
    else:
        done = load_progress(PROGRESS_FILE_BACKFILL)
        rows = [r for r in rows if r[0] not in done]

    total = len(rows)
    log.info(f"Backfill: {total} files to process "
             f"(skipping {len(done)} already done)" if not force else
             f"Backfill: {total} files to process (--force, progress ignored)")

    if total == 0:
        conn.close()
        return

    if dry_run:
        log.info("DRY RUN — showing first 20 files that would be processed:")
        for row in rows[:20]:
            fileid, size, nc_path, type_, owner, source_path = row
            owner_str = format_owner(type_, owner, nc_path)
            log.info(f"  fileid={fileid} [{owner_str}] size={size} {nc_path}")
        if total > 20:
            log.info(f"  ... and {total - 20} more")
        conn.close()
        return

    update_cur = conn.cursor()
    progress_fh = open(PROGRESS_FILE_BACKFILL, "a")
    processed = 0
    updated = 0
    errors = []
    batch = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_file_backfill, row): row for row in rows}
        for future in as_completed(futures):
            fileid, owner_str, nc_path, sha1_hex, err = future.result()
            processed += 1

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

            if processed % 5000 == 0:
                log.info(f"  [{processed}/{total}] updated={updated} errors={len(errors)}")

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

    log.info(f"Backfill done: {updated} updated, {len(errors)} errors out of {processed}")
    if errors:
        err_file = os.path.join(PROGRESS_DIR, "checksum_backfill_errors.csv")
        with open(err_file, "w") as f:
            writer = csv.writer(f)
            for fid, owner_str, path, err in errors:
                writer.writerow([fid, owner_str, path, err])
        log.info(f"Errors written to {err_file}")


def run_verify(workers, dry_run=False, path_filter=None, user=None):
    if not DB_PASS:
        log.error("DB_PASS environment variable is required")
        sys.exit(1)

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    if user:
        log.info(f"User filter: {user}")
    if path_filter:
        log.info(f"Path filter: {path_filter}")
    query = build_query(QUERY_BASE_EXISTING, path_filter, user)
    params = build_params(user=user, path_filter=path_filter)
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    done = load_progress(PROGRESS_FILE_VERIFY)
    rows = [r for r in rows if r[0] not in done]
    total = len(rows)
    log.info(f"Verify: {total} files to check (skipping {len(done)} already verified)")

    if total == 0:
        return

    if dry_run:
        log.info(f"DRY RUN — would verify {total} files with existing checksums")
        return

    progress_fh = open(PROGRESS_FILE_VERIFY, "a")
    ok = 0
    errors = []
    processed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_file_verify, row): row for row in rows}
        for future in as_completed(futures):
            fileid, owner_str, nc_path, status = future.result()
            processed += 1

            if status == "OK":
                ok += 1
                progress_fh.write(f"{fileid}\n")
                log.debug(f"OK   fileid={fileid} [{owner_str}] {nc_path}")
            else:
                errors.append((fileid, owner_str, nc_path, status))
                log.warning(f"FAIL fileid={fileid} [{owner_str}] {nc_path} — {status}")

            if processed % 5000 == 0:
                progress_fh.flush()
                log.info(f"  [{processed}/{total}] ok={ok} errors={len(errors)}")

    progress_fh.flush()
    progress_fh.close()

    log.info(f"Verify done: {ok}/{total} OK, {len(errors)} errors")
    if errors:
        err_file = os.path.join(PROGRESS_DIR, "checksum_verify_errors.csv")
        with open(err_file, "w") as f:
            writer = csv.writer(f)
            for fid, owner_str, path, err in errors:
                writer.writerow([fid, owner_str, path, err])
        log.info(f"Errors written to {err_file}")
    else:
        log.info("ALL FILES VERIFIED SUCCESSFULLY")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill/verify oc_filecache checksums",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables:
  DB_HOST          PostgreSQL host (default: localhost)
  DB_PORT          PostgreSQL port (default: 5432)
  DB_NAME          Database name (default: nextcloud)
  DB_USER          Database user (default: nextcloud)
  DB_PASS          Database password (required)
  PROGRESS_DIR     Directory for progress/log files (default: .)
        """,
    )
    parser.add_argument("mode", choices=["backfill", "verify", "all"],
                        help="backfill=fill missing, verify=check existing, all=both")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print every file (OK and errors), not just errors and summaries")
    parser.add_argument("--from-csv", metavar="FILE",
                        help="Backfill only fileids listed in this CSV "
                             "(one fileid per line, or CSV with fileid as first column). "
                             "Only valid with 'backfill' mode.")
    parser.add_argument("--force", action="store_true",
                        help="Re-process files even if they're in the progress file. "
                             "Useful with --from-csv to overwrite existing checksums.")
    parser.add_argument("--path-filter", metavar="PATTERN",
                        help="SQL LIKE pattern to filter by fc.path. "
                             "Examples: 'files/Photos/%%', '__groupfolders/5/%%', "
                             "'appdata_%%/preview/%%'")
    parser.add_argument("--user", metavar="USERNAME",
                        help="Process only files for this user (home:: storage). "
                             "Can be combined with --path-filter.")
    args = parser.parse_args()

    os.makedirs(PROGRESS_DIR, exist_ok=True)
    setup_logging(verbose=args.verbose)

    if args.from_csv and args.mode != "backfill":
        log.error("--from-csv is only supported with 'backfill' mode")
        sys.exit(1)

    if args.mode in ("backfill", "all"):
        run_backfill(args.workers, args.dry_run, args.from_csv, args.force,
                     args.path_filter, args.user)
    if args.mode in ("verify", "all"):
        run_verify(args.workers, args.dry_run, args.path_filter, args.user)


if __name__ == "__main__":
    main()
