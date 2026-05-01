# Nextcloud Local-to-S3 Migration Scripts

Migrate a Nextcloud instance from local/FUSE filesystem storage to S3-compatible
object storage (e.g. SeaweedFS, MinIO, AWS S3) as primary storage.

There is no official migration path. These scripts handle it by:

1. Backfilling SHA1 checksums in the database (Nextcloud doesn't compute them server-side)
2. Uploading every file to S3 as `urn:oid:<fileid>`
3. Verifying S3 contents against database checksums
4. Converting `oc_storages` entries (two SQL UPDATE statements, done manually)

## How Nextcloud S3 primary storage works

**Local storage:** files live on disk at `/data/<user>/<path>`. The database
tracks them in `oc_filecache` (fileid, path, size, checksum) with storage types
in `oc_storages` as `home::<user>` or `local::<path>`.

**S3 primary storage:** files live in S3 as `urn:oid:<fileid>`. The `oc_filecache`
table is **unchanged** — same fileids, same paths. Only `oc_storages` entries change:

```
home::alice                    → object::user:alice
local::/var/www/html/data/     → object::store:amazon::<bucket>
```

Group folders live under the `local::` storage — no separate type.

## Scripts

All scripts are self-contained, query the database directly (no CSV intermediaries),
and share the same interface patterns:

- `--dry-run` — show what would be done
- `--workers N` — parallel workers (default: 1)
- `-v / --verbose` — print every file, not just errors and summaries
- Progress files — safe to interrupt and re-run
- Error CSVs — `fileid,owner,path,error` format

### Environment variables

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=nextcloud
export DB_USER=nextcloud
export DB_PASS=secret

export S3_ENDPOINT=http://s3.example.com        # Steps 1 and 2 only
export S3_BUCKET=nextcloud-primary              # Steps 1 and 2 only
export S3_ACCESS_KEY=key                        # Steps 1 and 2 only
export S3_SECRET_KEY=secret                     # Steps 1 and 2 only

export PROGRESS_DIR=./migration  # progress files, logs, error CSVs (default: ./state)
```

### Step 0: `checksums.py`

Backfill and verify SHA1 checksums in `oc_filecache`. Nextcloud does NOT
compute checksums server-side on upload (only the desktop sync client sends
them). This script reads files from disk, computes SHA1, and writes
`SHA1:<hex>` to the database.

```bash
# Backfill missing checksums
python3 checksums.py backfill
python3 checksums.py backfill --user alice              # single user
python3 checksums.py backfill --path-filter 'files/Photos/%'  # by path

# Verify existing checksums against disk
python3 checksums.py verify

# Both
python3 checksums.py all

# Re-process specific files from an error CSV, overwriting existing checksums
python3 checksums.py backfill --from-csv errors.csv --force
```

Dependencies: `python3-psycopg2`

### Step 1: `upload.py`

Upload files from disk to S3 as `urn:oid:<fileid>`. Skips files already
present in S3 with correct size (HEAD check). Runs live, read-only on disk.

```bash
python3 upload.py --dry-run
python3 upload.py
python3 upload.py --workers 4
```

Dependencies: `python3-psycopg2`, `python3-boto3`

### Step 2: `verify.py`

Verify S3 objects against database checksums (from Step 0). Downloads each
file from S3, computes SHA1, compares. **No disk access needed.**

```bash
python3 verify.py --dry-run       # show stats
python3 verify.py --size-only     # fast, HEAD requests only
python3 verify.py                 # full SHA1 verification
```

Dependencies: `python3-psycopg2`, `python3-boto3`

### Step 2.5: `s3_checksums.py` (optional, recommended for FUSE sources)

Compute authoritative SHA1 checksums by downloading from S3 and writing back to
`oc_filecache`. Run this after Step 1 when Step 0 checksums may be unreliable
(JuiceFS cache inconsistency, any FUSE-based source). No disk access needed.

**Ordering note:** run Step 2 (verify) *before* this script if you want an
independent cross-check against disk-computed checksums. After running this
script, Step 2 will trivially pass since checksums now match S3 content.

```bash
python3 s3_checksums.py --dry-run
python3 s3_checksums.py
python3 s3_checksums.py --workers 4
```

Dependencies: `python3-psycopg2`, `python3-boto3`

### Step 3: Database cutover (manual)

After Steps 0-2 are clean, in maintenance mode:

```sql
BEGIN;
UPDATE oc_storages SET id = CONCAT('object::user:', SUBSTRING(id FROM 7)) WHERE id LIKE 'home::%';
UPDATE oc_storages SET id = 'object::store:amazon::<bucket>' WHERE id LIKE 'local::%';
COMMIT;
```

Then add the `objectstore` config to `config.php` and disable maintenance mode.

## Log output

With `-v`, every file is logged with its owner classification:

```
OK   fileid=42 [user=alice] files/Photos/vacation.jpg SHA1:abc123...
OK   fileid=99 [gf=5] Finance/Invoices/2025/invoice.pdf SHA1:def456...
OK   fileid=200 [appdata] appdata_xxx/preview/a/b/c.jpg SHA1:789...
FAIL fileid=301 [user=bob] files/Documents/broken.pdf — MISSING:/var/www/html/data/...
```

## Docker

```yaml
migration:
  image: python:3.12-alpine
  volumes:
    - ./scripts:/scripts:ro
    - ./migration:/migration
    - /path/to/nextcloud/data:/var/www/html/data:ro
  environment:
    - DB_HOST=pgsql
    - DB_PORT=5432
    - DB_NAME=nextcloud
    - DB_USER=nextcloud
    - DB_PASS=${POSTGRES_PASSWORD}
    - S3_ENDPOINT=http://seaweedfs:8333
    - S3_BUCKET=nextcloud-primary
    - S3_ACCESS_KEY=${S3_ACCESS_KEY}
    - S3_SECRET_KEY=${S3_SECRET_KEY}
    - PROGRESS_DIR=/migration
  command: ["sleep", "infinity"]
```

Install dependencies inside the container:
```bash
apk add --no-cache postgresql-dev gcc musl-dev
pip install psycopg2 boto3
```

Or on Debian:
```bash
apt install python3-psycopg2 python3-boto3
```

## S3 addressing style

Scripts use **DNS-style (virtual-hosted)** S3 addressing. Requests go to
`<bucket>.<endpoint>`. Ensure wildcard DNS resolves `*.<s3-hostname>` to your
S3 backend. To switch to path-style, change `addressing_style` from `"virtual"`
to `"path"` in the scripts.

## Known issues

### Nextcloud does not compute checksums server-side

Only the desktop sync client sends checksums on upload. Web UI, mobile apps,
WebDAV clients without the `OC-Checksum` header, and `occ files:scan` all
leave the `checksum` column empty. This is a known gap
([nextcloud/server#56057](https://github.com/nextcloud/server/issues/56057)).
Step 0 exists specifically to fill this gap before migration.

### FUSE storage read reliability

If your source filesystem is FUSE-based (JuiceFS, rclone mount, etc.),
aggressive caching or prefetch settings can cause transient read inconsistencies
where the same file produces different SHA1 hashes on different reads. This is
not file corruption — it's the FUSE layer serving stale/partial cache entries.

Symptoms: checksum mismatches that "fix themselves" on re-run, with different
subsets failing each time. Mitigation: use conservative FUSE mount options
during migration, run with `--workers 1`, and treat Step 0 as best-effort.
After uploading (Step 1), run Step 2.5 (`s3_checksums.py`) to overwrite
disk-computed checksums with authoritative values derived from S3.

## License

AGPL-3.0
