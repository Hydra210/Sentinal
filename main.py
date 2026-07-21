# SENTINEL - Roblox Audio Moderation Backend
# Start command: uvicorn main:app --host 0.0.0.0 --port $PORT

from __future__ import annotations
import asyncio, json, os, sqlite3, time, secrets, string, hashlib, uuid, gc, collections, ctypes
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict

import httpx
import psutil
import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool
from fastapi import FastAPI, HTTPException, Request, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="SENTINEL API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── ASSET TYPES ───────────────────────────────────────────────────────────────

ALL_ASSET_TYPES = [
    "Audio", "Decal", "Video", "Mesh",
    "Plugin", "Animation", "Model", "Package"
]
# Note: "Image" is excluded — Roblox does not allow archiving images via API.
# Images are system-generated from Decal uploads and cannot be archived.
NON_ARCHIVABLE_TYPES = {"Image"}

# ── SQLITE (local data — groups, history, config) ─────────────────────────────

DB_PATH = os.environ.get("DB_PATH", "sentinel.db")

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT, profile_id TEXT, name TEXT DEFAULT '', added_at REAL,
            PRIMARY KEY (id, profile_id));
        CREATE TABLE IF NOT EXISTS history (
            id TEXT PRIMARY KEY, profile_id TEXT DEFAULT '',
            username TEXT DEFAULT '', display_name TEXT DEFAULT '',
            user_id TEXT DEFAULT '', audio_name TEXT DEFAULT '',
            audio_id TEXT DEFAULT '', asset_type TEXT DEFAULT 'Audio',
            group_id TEXT DEFAULT '', group_name TEXT DEFAULT '',
            time TEXT, dm_status TEXT DEFAULT 'n/a', archived INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS config (
            profile_id TEXT, key TEXT, value TEXT,
            PRIMARY KEY (profile_id, key));
        CREATE TABLE IF NOT EXISTS connect_codes (
            code TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            expiry REAL NOT NULL);
    """)
    conn.commit()

    # ── Schema migrations: add columns that may be missing from old DB ──────────
    migrations = [
        "ALTER TABLE history ADD COLUMN profile_id TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN display_name TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN user_id TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN asset_type TEXT DEFAULT 'Audio'",
        "ALTER TABLE history ADD COLUMN group_id TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN group_name TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN dm_status TEXT DEFAULT 'n/a'",
        "ALTER TABLE history ADD COLUMN archived INTEGER DEFAULT 1",
        "ALTER TABLE groups ADD COLUMN profile_id TEXT DEFAULT ''",
        "ALTER TABLE groups ADD COLUMN name TEXT DEFAULT ''",
        "ALTER TABLE config ADD COLUMN profile_id TEXT DEFAULT ''",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass  # Column already exists — safe to ignore

    conn.close()

init_db()

# ── POSTGRES (profiles + saved credentials) ───────────────────────────────────

PG_URL = os.environ.get("DATABASE_URL", "")

_pg_pool = None

def init_pg_pool():
    global _pg_pool
    if not PG_URL:
        return
    _pg_pool = pg_pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        dsn=PG_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    print("[SENTINEL] Postgres connection pool initialized (min=1, max=20)")

def get_pg(retries: int = 20, delay: float = 0.1):
    if _pg_pool is None:
        raise RuntimeError("Postgres pool not initialized")
    last_err = None
    for _ in range(retries):
        try:
            return _pg_pool.getconn()
        except pg_pool.PoolError as e:
            # Pool is momentarily exhausted (e.g. a burst of rapid clicks).
            # Wait briefly for a connection to free up instead of crashing
            # the request with an unhandled 500.
            last_err = e
            time.sleep(delay)
    print(f"[SENTINEL] Postgres pool exhausted after retries: {last_err}")
    raise HTTPException(503, "Server is busy, please try again in a moment")

def release_pg(conn):
    if _pg_pool:
        _pg_pool.putconn(conn)

def init_pg():
    if not PG_URL:
        print("[SENTINEL] No DATABASE_URL set — Postgres features disabled")
        return
    conn1 = None
    try:
        # Step 1 — create tables
        conn1 = get_pg()
        cur1 = conn1.cursor()
        cur1.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                pin_hash TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS saved_credentials (
                profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                roblox_user_id TEXT NOT NULL DEFAULT '',
                cookie_encrypted TEXT,
                account_info JSONB,
                saved_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (profile_id, roblox_user_id)
            );
        """)
        conn1.commit()
    except Exception as e:
        print(f"[SENTINEL] Postgres table creation error: {e}")
        if conn1:
            try: conn1.rollback()
            except: pass
    finally:
        if conn1:
            try: cur1.close()
            except: pass
            release_pg(conn1)

    conn2 = None
    try:
        # Step 2 — fresh connection, force correct schema
        conn2 = get_pg()
        cur2 = conn2.cursor()

        # Check if id column is wrong type and drop if so
        cur2.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_name='profiles' AND column_name='id'
        """)
        row = cur2.fetchone()
        if row and row['data_type'] != 'text':
            print("[SENTINEL] Dropping profiles table — wrong id type, recreating...")
            cur2.execute("DROP TABLE IF EXISTS saved_credentials CASCADE;")
            cur2.execute("DROP TABLE IF EXISTS profiles CASCADE;")
            conn2.commit()

        # Recreate with correct schema
        cur2.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                pin_hash TEXT NOT NULL,
                avatar_url TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS saved_credentials (
                profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                roblox_user_id TEXT NOT NULL DEFAULT '',
                cookie_encrypted TEXT,
                account_info JSONB,
                saved_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (profile_id, roblox_user_id)
            );
        """)
        conn2.commit()

        # Safe per-column migrations
        for migration_sql in [
            "ALTER TABLE profiles ADD COLUMN IF NOT EXISTS avatar_url TEXT DEFAULT ''",
            "ALTER TABLE profiles ADD COLUMN IF NOT EXISTS pin_length INTEGER DEFAULT 4",
            "ALTER TABLE profiles ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE",
        ]:
            try:
                cur2.execute(migration_sql + ";")
                conn2.commit()
            except Exception as _me:
                conn2.rollback()
                print(f"[SENTINEL] Migration skipped: {_me}")

        # New tables
        cur2.execute("""
            CREATE TABLE IF NOT EXISTS access_requests (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                reason TEXT NOT NULL,
                email TEXT NOT NULL DEFAULT '',
                status TEXT DEFAULT 'pending',
                invite_code TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS invite_codes (
                code TEXT PRIMARY KEY,
                created_by TEXT NOT NULL,
                used BOOLEAN DEFAULT FALSE,
                used_by TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS review_tokens (
                token TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                action TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                used BOOLEAN DEFAULT FALSE
            );
        """)
        conn2.commit()
        # Add email col to access_requests if it was created without it
        try:
            cur2.execute("ALTER TABLE access_requests ADD COLUMN IF NOT EXISTS email TEXT NOT NULL DEFAULT '';")
            conn2.commit()
        except Exception as _me:
            conn2.rollback()

        # ── App data tables (groups, history, config, connect_codes) ──────────
        cur2.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id TEXT,
                profile_id TEXT DEFAULT '',
                name TEXT DEFAULT '',
                added_at FLOAT,
                PRIMARY KEY (id, profile_id)
            );
            CREATE TABLE IF NOT EXISTS history (
                id TEXT PRIMARY KEY,
                profile_id TEXT DEFAULT '',
                username TEXT DEFAULT '',
                display_name TEXT DEFAULT '',
                user_id TEXT DEFAULT '',
                audio_name TEXT DEFAULT '',
                audio_id TEXT DEFAULT '',
                asset_type TEXT DEFAULT 'Audio',
                group_id TEXT DEFAULT '',
                group_name TEXT DEFAULT '',
                time TEXT,
                dm_status TEXT DEFAULT 'n/a',
                archived INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS config (
                profile_id TEXT,
                key TEXT,
                value TEXT,
                PRIMARY KEY (profile_id, key)
            );
            CREATE TABLE IF NOT EXISTS connect_codes (
                code TEXT PRIMARY KEY,
                profile_id TEXT NOT NULL,
                expiry FLOAT NOT NULL
            );
        """)
        conn2.commit()
        print("[SENTINEL] Postgres initialized")
    except Exception as e:
        print(f"[SENTINEL] Postgres migration error: {e}")
        if conn2:
            try: conn2.rollback()
            except: pass
    finally:
        if conn2:
            try: cur2.close()
            except: pass
            release_pg(conn2)

init_pg_pool()
init_pg()

def hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.encode()).hexdigest()

# ── EMAIL ─────────────────────────────────────────────────────────────────────
GMAIL_USER      = os.environ.get("GMAIL_USER", "")
GMAIL_PASS      = os.environ.get("GMAIL_PASS", "")
ADMIN_EMAIL     = os.environ.get("ADMIN_EMAIL", "")
BASE_URL        = os.environ.get("BASE_URL", "").rstrip("/")
RESEND_API_KEY  = os.environ.get("RESEND_API_KEY", "")
EMAIL_FROM      = os.environ.get("EMAIL_FROM", "Sentinel <onboarding@resend.dev>")

def send_email(to: str, subject: str, html: str):
    # Preferred path: Resend's HTTP API. Render (and many free-tier hosts)
    # firewall outbound SMTP ports 25/465/587, which is what produces
    # "[Errno 101] Network is unreachable" from smtplib — an HTTP call on
    # port 443 sidesteps that entirely.
    if RESEND_API_KEY:
        try:
            r = httpx.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
                json={"from": EMAIL_FROM, "to": [to], "subject": subject, "html": html},
                timeout=10,
            )
            if r.status_code >= 400:
                print(f"[SENTINEL] Email error: {r.status_code} {r.text}")
            else:
                print(f"[SENTINEL] Email sent to {to}")
        except Exception as e:
            print(f"[SENTINEL] Email error: {e}")
        return

    # Fallback: Gmail SMTP (works locally / on hosts that allow SMTP egress)
    if not GMAIL_USER or not GMAIL_PASS:
        print(f"[SENTINEL] Email not configured, skipping: {subject}")
        return
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Sentinel <{GMAIL_USER}>"
    msg["To"]      = to
    msg.attach(MIMEText(html, "html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_USER, GMAIL_PASS)
            s.sendmail(GMAIL_USER, to, msg.as_string())
        print(f"[SENTINEL] Email sent to {to}")
    except Exception as e:
        print(f"[SENTINEL] Email error: {e}")

# ── DEBUG / MEMORY / LOG SYSTEM ───────────────────────────────────────────────

_LOG_BUFFER: collections.deque = collections.deque(maxlen=500)
_DEBUG_MODE: bool = False
_DEGRADED:   bool = False
_MEMORY_MB:  float = 0.0
_MEMORY_PCT: float = 0.0
_MEM_TOTAL_MB: float = 0.0

LOG_LEVELS = {"INFO", "WARN", "ERROR", "DEBUG", "ARCHIVE", "DM", "NETWORK", "MEMORY"}

def sentinel_log(msg: str, level: str = "INFO", source: str = "SENTINEL"):
    """Central log function — always buffers, prints always (debug filters on frontend)."""
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    entry = {
        "ts":     ts,
        "level":  level.upper(),
        "source": source,
        "msg":    msg,
    }
    _LOG_BUFFER.append(entry)
    print(f"[{entry['source']}][{entry['level']}] {msg}")

_libc = None
def _trim_memory():
    """Force glibc to release free memory back to the OS (fixes RSS bloat on Linux)."""
    global _libc
    try:
        if _libc is None:
            _libc = ctypes.CDLL("libc.so.6")
        gc.collect()
        _libc.malloc_trim(0)
    except Exception:
        pass  # Non-Linux or libc not available — safe to ignore

async def memory_watchdog():
    global _DEGRADED, _MEMORY_MB, _MEMORY_PCT, _MEM_TOTAL_MB
    process = psutil.Process()
    try:
        vm = psutil.virtual_memory()
        _MEM_TOTAL_MB = vm.total / 1024 / 1024
    except Exception:
        _MEM_TOTAL_MB = 512.0

    LIMIT_MB = float(os.environ.get("MEMORY_LIMIT_MB", 400))

    while True:
        try:
            rss = process.memory_info().rss / 1024 / 1024
            _MEMORY_MB  = round(rss, 1)
            _MEMORY_PCT = round((rss / LIMIT_MB) * 100, 1)

            # Trim every cycle — malloc_trim is cheap and keeps RSS stable
            _trim_memory()

            if rss > LIMIT_MB and not _DEGRADED:
                _DEGRADED = True
                _trim_memory()
                sentinel_log(f"High memory {rss:.1f}MB/{LIMIT_MB:.0f}MB — degraded mode ON", "MEMORY", "WATCHDOG")
            elif rss < LIMIT_MB * 0.75 and _DEGRADED:
                _DEGRADED = False
                sentinel_log(f"Memory normal {rss:.1f}MB — degraded mode OFF", "MEMORY", "WATCHDOG")
        except Exception as e:
            sentinel_log(f"Watchdog error: {e}", "ERROR", "WATCHDOG")
        await asyncio.sleep(4)


# ══════════════════════════════════════════════════════════════════════════════
# VAULT — master-key-protected backup/restore (works even with empty DB)
# ══════════════════════════════════════════════════════════════════════════════

# Set SENTINEL_MASTER_KEY in your Render env vars.
# If not set, one is generated and printed to logs on startup — grab it from there.
_MASTER_KEY: str = os.environ.get("SENTINEL_MASTER_KEY", "")

# All tables in import order (FK-safe: profiles before saved_credentials, etc.)
_VAULT_TABLES = [
    "profiles",
    "saved_credentials",
    "access_requests",
    "invite_codes",
    "review_tokens",
    "groups",
    "history",
    "config",
    "connect_codes",
]

_TABLE_PKS = {
    "profiles":          ["id"],
    "saved_credentials": ["profile_id", "roblox_user_id"],
    "access_requests":   ["id"],
    "invite_codes":      ["code"],
    "review_tokens":     ["token"],
    "groups":            ["id", "profile_id"],
    "history":           ["id"],
    "config":            ["profile_id", "key"],
    "connect_codes":     ["code"],
}

def _vault_check_key(key: str):
    if not key or not _MASTER_KEY:
        raise HTTPException(401, "Vault master key not configured or missing")
    if not secrets.compare_digest(key.strip(), _MASTER_KEY.strip()):
        raise HTTPException(403, "Invalid vault master key")

def _vault_export_data() -> dict:
    """Dump all Postgres tables to a plain-dict snapshot. Raises on failure."""
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    out: dict = {"_meta": {
        "exported_at":      datetime.utcnow().isoformat() + "Z",
        "sentinel_version": "1.0",
    }}
    try:
        for table in _VAULT_TABLES:
            try:
                cur.execute(f"SELECT * FROM {table}")
                rows = []
                for row in cur.fetchall():
                    d = {}
                    for k, v in dict(row).items():
                        if hasattr(v, "isoformat"):
                            d[k] = v.isoformat()
                        else:
                            d[k] = v
                    rows.append(d)
                out[table] = rows
                sentinel_log(f"Vault export: {table} → {len(rows)} rows", "INFO", "VAULT")
            except Exception as e:
                sentinel_log(f"Vault export: skipping {table} ({e})", "WARN", "VAULT")
                out[table] = []
        return out
    finally:
        cur.close(); release_pg(conn)

def _vault_import_data(data: dict) -> dict:
    """Upsert all rows from a vault snapshot. Returns per-table counts."""
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    if not isinstance(data, dict):
        raise HTTPException(400, "Invalid vault data")

    conn = get_pg(); cur = conn.cursor()
    results: dict = {}
    try:
        for table in _VAULT_TABLES:
            rows = data.get(table)
            if not isinstance(rows, list) or not rows:
                results[table] = 0
                continue
            pks = _TABLE_PKS.get(table)
            if not pks:
                continue
            upserted = 0
            for row in rows:
                if not isinstance(row, dict) or not row:
                    continue
                cols   = list(row.keys())
                vals   = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in row.values()]
                ph     = ", ".join(["%s"] * len(cols))
                colstr = ", ".join(f'"{c}"' for c in cols)
                non_pk = [c for c in cols if c not in pks]
                if non_pk:
                    set_clause = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in non_pk)
                    conflict   = f"DO UPDATE SET {set_clause}"
                else:
                    conflict   = "DO NOTHING"
                pk_str = ", ".join(f'"{p}"' for p in pks)
                sql = (
                    f'INSERT INTO "{table}" ({colstr}) VALUES ({ph}) '
                    f"ON CONFLICT ({pk_str}) {conflict}"
                )
                try:
                    cur.execute(sql, vals)
                    upserted += 1
                except Exception as row_err:
                    conn.rollback()
                    sentinel_log(f"Vault import row error ({table}): {row_err}", "WARN", "VAULT")
                    continue
            conn.commit()
            results[table] = upserted
            sentinel_log(f"Vault import: {table} → {upserted}/{len(rows)} rows", "INFO", "VAULT")
        sentinel_log("Vault import complete", "INFO", "VAULT")
        return results
    except HTTPException:
        raise
    except Exception as e:
        try: conn.rollback()
        except: pass
        raise HTTPException(500, f"Vault import failed: {e}")
    finally:
        cur.close(); release_pg(conn)


class VaultKeyBody(BaseModel):
    key: str

class VaultImportBody(BaseModel):
    key:  str
    data: dict

@app.post("/api/vault/export")
def api_vault_export(body: VaultKeyBody):
    """Export all data. Protected by SENTINEL_MASTER_KEY. No profile login required."""
    _vault_check_key(body.key)
    return _vault_export_data()

@app.post("/api/vault/import")
def api_vault_import(body: VaultImportBody):
    """Import/restore all data. Protected by SENTINEL_MASTER_KEY. No profile login required."""
    _vault_check_key(body.key)
    results = _vault_import_data(body.data)
    return {"imported": True, "tables": results}


async def vault_auto_restore():
    """
    On startup: if Postgres is connected but completely empty (no profiles),
    attempt to restore from the VAULT_AUTO_RESTORE_URL env var.

    Set VAULT_AUTO_RESTORE_URL to a direct URL that returns your vault JSON
    (e.g. a raw GitHub Gist URL, an R2 object URL, etc).
    When you export, upload the file to that URL manually once.
    After that, every fresh Postgres will auto-restore from it on boot.
    """
    restore_url = os.environ.get("VAULT_AUTO_RESTORE_URL", "").strip()
    if not restore_url or not PG_URL:
        return

    await asyncio.sleep(8)  # Let DB init fully settle

    try:
        conn = get_pg(); cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM profiles")
            row = cur.fetchone()
            count = list(row.values())[0] if row else 0
        finally:
            cur.close(); release_pg(conn)

        if count > 0:
            sentinel_log(f"Auto-restore: DB has {count} profile(s) — skipping", "INFO", "VAULT")
            return

        sentinel_log(f"Auto-restore: DB is empty — fetching backup from configured URL...", "INFO", "VAULT")
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(restore_url)
        if r.status_code != 200:
            sentinel_log(f"Auto-restore: fetch failed HTTP {r.status_code}", "ERROR", "VAULT")
            return

        data = r.json()
        if not isinstance(data, dict) or "_meta" not in data:
            sentinel_log("Auto-restore: URL did not return a valid vault snapshot", "ERROR", "VAULT")
            return

        results = _vault_import_data(data)
        total   = sum(results.values())
        sentinel_log(
            f"Auto-restore: SUCCESS — {total} rows restored across {len(results)} tables. "
            f"Details: {results}",
            "INFO", "VAULT"
        )
    except Exception as e:
        sentinel_log(f"Auto-restore: unexpected error — {e}", "ERROR", "VAULT")


# Routes all app-data queries to Postgres when DATABASE_URL is set,
# falls back to SQLite for local development.

def db_exec(sql: str, params: tuple = (), *, fetch: str = None):
    """
    Execute SQL and optionally return results.
    fetch='all' → list[dict], 'one' → dict|None, 'val' → scalar, None → None
    Handles ? vs %s placeholder conversion automatically.
    """
    if PG_URL:
        pg_sql = sql.replace("?", "%s")
        conn = get_pg()
        cur = conn.cursor()
        try:
            cur.execute(pg_sql, params)
            conn.commit()
            if fetch == "all":
                return [dict(r) for r in cur.fetchall()]
            if fetch == "one":
                row = cur.fetchone()
                return dict(row) if row else None
            if fetch == "val":
                row = cur.fetchone()
                return (list(row.values())[0] if row else None)
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close(); release_pg(conn)
    else:
        conn = get_db()
        try:
            cur = conn.execute(sql, params)
            conn.commit()
            if fetch == "all":
                return [dict(r) for r in cur.fetchall()]
            if fetch == "one":
                row = cur.fetchone()
                return dict(row) if row else None
            if fetch == "val":
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            conn.close()

def db_upsert(table: str, pk_cols: list, data: dict):
    """
    INSERT … ON CONFLICT (pk_cols) DO UPDATE for Postgres,
    INSERT OR REPLACE for SQLite.
    """
    cols   = list(data.keys())
    vals   = list(data.values())
    if PG_URL:
        col_str    = ", ".join(cols)
        ph_str     = ", ".join(["%s"] * len(cols))
        conflict   = ", ".join(pk_cols)
        update_str = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in pk_cols)
        sql = (f"INSERT INTO {table} ({col_str}) VALUES ({ph_str}) "
               f"ON CONFLICT ({conflict}) DO UPDATE SET {update_str}")
        conn = get_pg()
        cur = conn.cursor()
        try:
            cur.execute(sql, vals)
            conn.commit()
        except Exception:
            conn.rollback(); raise
        finally:
            cur.close(); release_pg(conn)
    else:
        col_str = ", ".join(cols)
        ph_str  = ", ".join(["?"] * len(cols))
        sql = f"INSERT OR REPLACE INTO {table} ({col_str}) VALUES ({ph_str})"
        conn = get_db()
        try:
            conn.execute(sql, vals)
            conn.commit()
        finally:
            conn.close()

def db_insert_ignore(table: str, data: dict):
    """INSERT OR IGNORE (SQLite) / INSERT … ON CONFLICT DO NOTHING (Postgres)."""
    cols  = list(data.keys())
    vals  = list(data.values())
    if PG_URL:
        col_str = ", ".join(cols)
        ph_str  = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({ph_str}) ON CONFLICT DO NOTHING"
        conn = get_pg()
        cur = conn.cursor()
        try:
            cur.execute(sql, vals)
            conn.commit()
        except Exception:
            conn.rollback(); raise
        finally:
            cur.close(); release_pg(conn)
    else:
        col_str = ", ".join(cols)
        ph_str  = ", ".join(["?"] * len(cols))
        sql = f"INSERT OR IGNORE INTO {table} ({col_str}) VALUES ({ph_str})"
        conn = get_db()
        try:
            conn.execute(sql, vals)
            conn.commit()
        finally:
            conn.close()

# ── APP STATE ─────────────────────────────────────────────────────────────────

class ProfileSession:
    def __init__(self):
        self.profile_id:          Optional[str]  = None
        self.cookie:              Optional[str]  = None
        self.monitoring:          bool           = False
        self.monitor_task:        Optional[asyncio.Task] = None
        self.known_assets:        dict           = {}
        self.account_info:        Optional[dict] = None
        self.extension_last_seen: float          = 0.0
        self.extension_token:     Optional[str]  = None   # shared secret issued at connect
        self.extension_token_valid: bool         = False  # True only when token matched recently
        self.pending_commands:    list           = []     # commands queued for extension to pick up
        self.pending_save:        bool           = False
        self.pending_save_info:   Optional[dict] = None
        self.add_account_mode:    bool           = False

_sessions: Dict[str, ProfileSession] = {}

# ── SANITY CHECK SYSTEM ───────────────────────────────────────────────────────
# Runs every 5 minutes (and immediately on startup) to validate all saved cookies.
# Results are stored per (profile_id, roblox_user_id) so the frontend can render
# expired labels without disrupting the monitor loop.

_SANITY_CHECK_INTERVAL = 300  # 5 minutes

# State shape:
#   _sanity_results[profile_id][roblox_user_id] = {
#       "valid": bool, "checked_at": float, "username": str, "userId": str
#   }
_sanity_results: Dict[str, Dict[str, dict]] = {}
_sanity_running: bool = False
_sanity_last_run: float = 0.0
_sanity_phase2_state: Dict[str, dict] = {}  # set inside _run_phase2_activity_check

async def _check_cookie_valid(cookie: str) -> bool:
    """Hit Roblox /users/authenticated to verify a cookie is still valid.
    Returns True if valid, False if expired/invalid. Never raises."""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://users.roblox.com/v1/users/authenticated",
                cookies={".ROBLOSECURITY": cookie},
            )
            return r.status_code == 200
    except Exception:
        return False

def _sanity_mark_valid(profile_id: str, roblox_user_id: str, username: str, cookie: str, acc_info: dict):
    """Immediately mark an account as valid in the sanity results cache.
    Called whenever a new account is added or activated so the UI doesn't show
    it as expired before the next scheduled sanity check runs."""
    if profile_id not in _sanity_results:
        _sanity_results[profile_id] = {}
    _sanity_results[profile_id][str(roblox_user_id)] = {
        "valid":      True,
        "checked_at": time.time(),
        "username":   username,
        "userId":     str(roblox_user_id),
        "cookie":     cookie,
        "acc_info":   acc_info,
    }
    sentinel_log(f"Sanity cache: marked {username} ({roblox_user_id}) as VALID after account add/activate", "DEBUG", "SANITY")

async def run_sanity_check():
    """
    Full sanity check — two phases run back-to-back per profile.

    PHASE 1 — Cookie validity:
      Validates every saved credential against Roblox API.
      If the active session's cookie just expired:
        • Attempt auto-switch to the next saved account with a valid cookie.
        • Send an email: either "switched to X" or "no valid account available — inactive".

    PHASE 2 — Activity check:
      Only runs if:
        - A valid active account exists after phase 1
        - Active monitoring is currently ON
      Temporarily stops the monitor for 5 seconds then restarts it.
      Frontend reads `phase2_active` + `phase2_countdown` from the status endpoint
      to show the grayed-out toggle + countdown label.

    Non-blocking — runs as an asyncio task, never touches the monitor loop except
    during the intentional 5-second pause in phase 2.
    """
    global _sanity_running, _sanity_last_run
    if _sanity_running:
        sentinel_log("Sanity check already running — skipping", "DEBUG", "SANITY")
        return
    if not PG_URL:
        sentinel_log("Sanity check skipped — no Postgres configured", "DEBUG", "SANITY")
        return

    _sanity_running = True
    sentinel_log("Sanity check Phase 1 started — cookie validation", "INFO", "SANITY")
    try:
        # ── Fetch all saved credentials ───────────────────────────────────────
        conn = get_pg(); cur = conn.cursor()
        try:
            cur.execute("SELECT profile_id, roblox_user_id, cookie_encrypted, account_info FROM saved_credentials")
            rows = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            sentinel_log(f"Sanity check DB error: {e}", "ERROR", "SANITY")
            return
        finally:
            cur.close(); release_pg(conn)

        sentinel_log(f"Sanity Phase 1: validating {len(rows)} saved credentials", "INFO", "SANITY")

        # Group rows by profile so we can do per-profile auto-switch logic
        by_profile: Dict[str, list] = {}
        for row in rows:
            pid_key = row.get("profile_id", "")
            if pid_key not in by_profile:
                by_profile[pid_key] = []
            by_profile[pid_key].append(row)

        # Profiles that need phase 2 after phase 1 completes
        phase2_profiles: list = []

        for profile_id, profile_rows in by_profile.items():
            if profile_id not in _sanity_results:
                _sanity_results[profile_id] = {}

            # Validate all cookies for this profile
            for row in profile_rows:
                roblox_user_id = row.get("roblox_user_id", "")
                cookie         = row.get("cookie_encrypted", "")
                acc_info       = row.get("account_info") or {}
                if isinstance(acc_info, str):
                    try: acc_info = json.loads(acc_info)
                    except: acc_info = {}

                username = acc_info.get("displayName") or acc_info.get("username") or roblox_user_id

                valid = await _check_cookie_valid(cookie) if cookie else False

                was_valid = _sanity_results[profile_id].get(roblox_user_id, {}).get("valid", True)
                _sanity_results[profile_id][roblox_user_id] = {
                    "valid":      valid,
                    "checked_at": time.time(),
                    "username":   username,
                    "userId":     roblox_user_id,
                    "cookie":     cookie,       # kept in memory only, never sent to frontend
                    "acc_info":   acc_info,
                }
                sentinel_log(
                    f"Sanity P1 [{profile_id[:8]}] {username} ({roblox_user_id}): "
                    f"{'VALID' if valid else 'EXPIRED'}",
                    "INFO", "SANITY"
                )

                # ── Detect active-account expiry ──────────────────────────────
                session = _sessions.get(profile_id)
                if session and session.cookie and not valid:
                    active_uid = str((session.account_info or {}).get("userId", ""))
                    if active_uid == str(roblox_user_id) and was_valid:
                        # Active account JUST expired — attempt auto-switch
                        sentinel_log(
                            f"ALERT: Active cookie expired for profile {profile_id} "
                            f"account {username} ({roblox_user_id}) — attempting auto-switch",
                            "ERROR", "SANITY"
                        )
                        switched_to = await _try_switch_account(profile_id, roblox_user_id, profile_rows)
                        if ADMIN_EMAIL:
                            _send_cookie_expired_email(
                                profile_id, username, roblox_user_id, switched_to
                            )

            # ── Determine if phase 2 should run for this profile ──────────────
            session = _sessions.get(profile_id)
            if session and session.cookie and session.monitoring:
                # Only run phase 2 if we have a valid active account
                active_uid = str((session.account_info or {}).get("userId", ""))
                acct_result = _sanity_results[profile_id].get(active_uid, {})
                if acct_result.get("valid", False):
                    phase2_profiles.append(profile_id)
                else:
                    sentinel_log(
                        f"Sanity Phase 2 skipped for {profile_id[:8]} — no valid active account",
                        "INFO", "SANITY"
                    )
            else:
                reason = "no active account" if not (session and session.cookie) else "monitoring is OFF"
                sentinel_log(
                    f"Sanity Phase 2 skipped for {profile_id[:8]} — {reason}",
                    "INFO", "SANITY"
                )

        _sanity_last_run = time.time()
        sentinel_log("Sanity Phase 1 complete", "INFO", "SANITY")

        # ── PHASE 2: Activity check ───────────────────────────────────────────
        for profile_id in phase2_profiles:
            await _run_phase2_activity_check(profile_id)

        sentinel_log("Sanity check complete (all phases)", "INFO", "SANITY")

    except Exception as e:
        sentinel_log(f"Sanity check unexpected error: {e}", "ERROR", "SANITY")
    finally:
        _sanity_running = False


async def _try_switch_account(profile_id: str, expired_uid: str, all_rows: list) -> Optional[dict]:
    """
    Attempt to switch the active session to another saved account with a valid cookie.
    Returns account info dict of the new account if switched, None if no valid account found.
    """
    session = _sessions.get(profile_id)
    if not session:
        return None

    # Find the first valid account that isn't the expired one
    profile_results = _sanity_results.get(profile_id, {})
    for uid, result in profile_results.items():
        if uid == expired_uid:
            continue
        if result.get("valid") and result.get("cookie"):
            new_cookie   = result["cookie"]
            new_acc_info = result.get("acc_info", {})
            new_username = result.get("username", uid)
            try:
                # Re-validate just to be sure
                still_valid = await _check_cookie_valid(new_cookie)
                if not still_valid:
                    continue
                # Switch session
                session.cookie       = new_cookie
                session.account_info = new_acc_info
                sentinel_log(
                    f"Auto-switched profile {profile_id[:8]} from expired {expired_uid} "
                    f"to {new_username} ({uid})",
                    "INFO", "SANITY"
                )
                return {"username": new_username, "userId": uid, "acc_info": new_acc_info}
            except Exception as e:
                sentinel_log(f"Auto-switch error for uid {uid}: {e}", "WARN", "SANITY")
                continue

    # No valid account found — clear the session cookie so monitoring stops cleanly
    session.cookie       = None
    session.account_info = None
    if session.monitoring:
        session.monitoring = False
        if session.monitor_task:
            session.monitor_task.cancel()
            try: await session.monitor_task
            except: pass
            session.monitor_task = None
        set_cfg(profile_id, "_monitoringActive", False)
        sentinel_log(
            f"No valid fallback account for {profile_id[:8]} — monitoring stopped",
            "ERROR", "SANITY"
        )
    return None


async def _run_phase2_activity_check(profile_id: str):
    """
    Phase 2 — Activity sanity check.
    Briefly pauses monitoring for 5 seconds then restarts it.
    Updates _sanity_phase2_state so the frontend can show the grayed-out toggle + countdown.
    """
    session = _sessions.get(profile_id)
    if not session or not session.monitoring or not session.cookie:
        sentinel_log(f"Phase 2 skipped for {profile_id[:8]} — preconditions not met at start", "INFO", "SANITY")
        return

    sentinel_log(f"Sanity Phase 2 started for {profile_id[:8]} — activity check", "INFO", "SANITY")

    # Mark phase 2 as active so frontend grays out toggle
    _sanity_phase2_state[profile_id] = {"active": True, "countdown": 5}

    try:
        # Pause monitoring
        session.monitoring = False
        if session.monitor_task:
            session.monitor_task.cancel()
            try: await session.monitor_task
            except: pass
            session.monitor_task = None

        # Countdown — update state each second so frontend gets live countdown
        for remaining in range(5, 0, -1):
            _sanity_phase2_state[profile_id]["countdown"] = remaining
            await asyncio.sleep(1)

        # Restart monitoring — clear unsupported cache so next cycle retries all types fresh
        _unsupported_group_types.clear()
        session.monitoring   = True
        session.monitor_task = asyncio.create_task(monitor_loop(profile_id))
        set_cfg(profile_id, "_monitoringActive", True)
        sentinel_log(f"Sanity Phase 2 complete for {profile_id[:8]} — monitoring restarted", "INFO", "SANITY")

    except Exception as e:
        sentinel_log(f"Phase 2 error for {profile_id[:8]}: {e}", "ERROR", "SANITY")
        # Best-effort restart on error
        try:
            if session.cookie and not session.monitoring:
                session.monitoring   = True
                session.monitor_task = asyncio.create_task(monitor_loop(profile_id))
        except Exception:
            pass
    finally:
        _sanity_phase2_state[profile_id] = {"active": False, "countdown": 0}


def _send_cookie_expired_email(
    profile_id: str,
    username: str,
    roblox_user_id: str,
    switched_to: Optional[dict],
):
    """Send an email alert that the active account's cookie has expired.
    If switched_to is provided, the email mentions the auto-switch.
    If None, the email warns that no valid account was available."""

    if switched_to:
        subject     = "[SENTINEL] ⚠ Cookie Expired — Auto-Switched Account"
        new_name    = switched_to.get("username", "Unknown")
        new_uid     = switched_to.get("userId", "—")
        status_block = f"""
  <div style="background:rgba(100,200,120,0.08);border:1px solid rgba(100,200,120,0.3);border-radius:8px;padding:16px;margin-bottom:20px;">
    <div style="color:#6ade80;font-weight:700;font-size:14px;letter-spacing:2px;margin-bottom:8px;">✓ AUTO-SWITCHED</div>
    <div style="color:#ccc;font-size:13px;line-height:1.6;">
      Sentinel automatically switched to another saved account with a valid cookie.
      Monitoring is <strong style="color:#6ade80;">continuing</strong> under the new account.
    </div>
  </div>
  <table style="width:100%;border-collapse:collapse;margin-bottom:12px;">
    <tr><td style="color:#888;padding:4px 0;width:140px;font-size:12px;">Switched To</td><td style="color:#fff;font-weight:600;">{new_name}</td></tr>
    <tr><td style="color:#888;padding:4px 0;font-size:12px;">New Account UID</td><td style="color:#aaa;font-size:12px;">{new_uid}</td></tr>
  </table>"""
    else:
        subject     = "[SENTINEL] ⚠ Cookie Expired — No Valid Account — Monitoring INACTIVE"
        status_block = f"""
  <div style="background:rgba(255,59,59,0.1);border:1px solid rgba(255,59,59,0.3);border-radius:8px;padding:16px;margin-bottom:20px;">
    <div style="color:#ff6b6b;font-weight:700;font-size:14px;letter-spacing:2px;margin-bottom:8px;">⚠ NO VALID FALLBACK ACCOUNT</div>
    <div style="color:#ccc;font-size:13px;line-height:1.6;">
      The active cookie expired and Sentinel found <strong style="color:#ff6b6b;">no other saved accounts with a valid cookie</strong> to automatically switch to.
      Monitoring is now <strong style="color:#ff6b6b;">INACTIVE</strong> and will not resume until you re-add a valid account.
    </div>
  </div>"""

    html = f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;background:#0f0f0f;color:#fff;padding:32px;border-radius:12px;border:1px solid rgba(255,59,59,0.3);">
  <h2 style="color:#fff;letter-spacing:4px;margin-top:0;font-size:20px;">SENTINEL</h2>
  {status_block}
  <div style="font-family:monospace;font-size:11px;color:#888;letter-spacing:1px;text-transform:uppercase;margin-bottom:8px;">Expired Account</div>
  <table style="width:100%;border-collapse:collapse;margin-bottom:20px;background:rgba(255,255,255,0.03);border-radius:6px;padding:12px;">
    <tr><td style="color:#888;padding:6px 8px;width:120px;font-size:12px;">Profile ID</td><td style="color:#aaa;font-size:12px;padding:6px 8px;">{profile_id}</td></tr>
    <tr><td style="color:#888;padding:6px 8px;font-size:12px;">Account</td><td style="color:#fff;font-weight:600;padding:6px 8px;">{username}</td></tr>
    <tr><td style="color:#888;padding:6px 8px;font-size:12px;">Roblox UID</td><td style="color:#aaa;font-size:12px;padding:6px 8px;">{roblox_user_id}</td></tr>
    <tr><td style="color:#888;padding:6px 8px;font-size:12px;">Detected At</td><td style="color:#aaa;font-size:12px;padding:6px 8px;">{datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</td></tr>
  </table>
  <p style="color:#555;font-size:10px;letter-spacing:1px;">SENTINEL AUTOMATED ALERT · SANITY CHECK SYSTEM · PHASE 1</p>
</div>"""
    try:
        send_email(ADMIN_EMAIL, subject, html)
    except Exception as e:
        sentinel_log(f"Failed to send cookie expired email: {e}", "ERROR", "SANITY")

async def sanity_check_loop():
    """Background task: run sanity check immediately on startup, then every 5 minutes."""
    sentinel_log("Running initial sanity check on startup", "INFO", "SANITY")
    await run_sanity_check()
    while True:
        await asyncio.sleep(_SANITY_CHECK_INTERVAL)
        await run_sanity_check()

def get_session(profile_id: str) -> ProfileSession:
    if profile_id not in _sessions:
        _sessions[profile_id] = ProfileSession()
        _sessions[profile_id].profile_id = profile_id
    return _sessions[profile_id]

# ── CONNECT CODES (SQLite-backed so they survive Render restarts) ─────────────

def generate_connect_code(profile_id: str) -> str:
    now = time.time()
    code = ''.join(secrets.choice(string.digits) for _ in range(4))
    db_exec("DELETE FROM connect_codes WHERE expiry < ?", (now,))
    db_upsert("connect_codes", ["code"],
              {"code": code, "profile_id": profile_id, "expiry": now + 300})
    return code

def validate_connect_code(code: str) -> Optional[str]:
    now = time.time()
    row = db_exec("SELECT profile_id, expiry FROM connect_codes WHERE code=?", (code,), fetch="one")
    if not row:
        return None
    if now > row["expiry"]:
        db_exec("DELETE FROM connect_codes WHERE code=?", (code,))
        return None
    profile_id = row["profile_id"]
    db_exec("DELETE FROM connect_codes WHERE code=?", (code,))
    return profile_id

# ── CONFIG ────────────────────────────────────────────────────────────────────

DEFAULT_CFG = {
    "pollingInterval":     60,
    "allowFastPolling":    False,
    "archiveDelay":        0,
    "archiveExisting":     False,
    "saveCookies":         False,
    "cookieSaveMode":      "ask",
    "autoStartMonitoring": False,
    "assetTypeFilters":    ALL_ASSET_TYPES,
    "whitelist_Audio":     [],
    "whitelist_Image":     [],
    "whitelist_Decal":     [],
    "whitelist_Video":     [],
    "whitelist_Mesh":      [],
    "whitelist_Plugin":    [],
    "whitelist_Animation": [],
    "whitelist_Model":     [],
    "whitelist_Package":   [],
    "whitelist_all":       [],
}

def get_config(profile_id: str) -> dict:
    rows = db_exec("SELECT key, value FROM config WHERE profile_id=?", (profile_id,), fetch="all")
    cfg = dict(DEFAULT_CFG)
    for row in (rows or []):
        try:
            cfg[row["key"]] = json.loads(row["value"])
        except Exception:
            cfg[row["key"]] = row["value"]
    return cfg

def set_cfg(profile_id: str, key: str, value):
    db_upsert("config", ["profile_id", "key"],
              {"profile_id": profile_id, "key": key, "value": json.dumps(value)})

# ── ROBLOX API HELPERS ────────────────────────────────────────────────────────

ASSET_TYPE_IDS = {
    "Audio": 3, "Image": 1, "Decal": 13, "Video": 62,
    "Mesh": 4, "Plugin": 38, "Animation": 24,
    "Model": 10, "Package": 32,
}

async def rblx_get(url: str, *, cookie=None, params=None) -> httpx.Response:
    cookies: dict = {}
    if cookie:
        cookies[".ROBLOSECURITY"] = cookie
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
        return await c.get(url, cookies=cookies, params=params)

_csrf_cache: dict = {}
# Tracks (group_id, asset_type) pairs that consistently 403 on the listing endpoint.
# Suppressed after the first logged failure to avoid log spam on every poll.
# Tracks asset types that returned a hard permanent error (403 permission denied)
# for a specific group. Only truly permanent failures go here — NOT network errors,
# NOT 429 rate limits, NOT temporary Roblox outages.
# Format: { (group_id, asset_type): timestamp_of_first_failure }
# Re-checked every 30 minutes in case permissions change.
_unsupported_group_types: dict = {}
_UNSUPPORTED_RECHECK_SECS = 1800  # 30 minutes

async def get_csrf(cookie: str) -> str:
    now = time.time()
    if cookie in _csrf_cache and now - _csrf_cache[cookie][1] < 110:
        return _csrf_cache[cookie][0]
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post("https://auth.roblox.com/v2/logout",
                         cookies={".ROBLOSECURITY": cookie})
        token = r.headers.get("x-csrf-token", "")
        _csrf_cache[cookie] = (token, now)
        sentinel_log(f"CSRF token refreshed", "DEBUG", "NETWORK")
        return token

async def validate_cookie(cookie: str) -> dict:
    r = await rblx_get("https://users.roblox.com/v1/users/authenticated", cookie=cookie)
    if r.status_code != 200:
        raise HTTPException(400, "Invalid or expired cookie")
    d = r.json()
    uid = str(d["id"])
    avatar_url = None
    try:
        ar = await rblx_get(
            "https://thumbnails.roblox.com/v1/users/avatar-headshot",
            cookie=cookie,
            params={"userIds": uid, "size": "150x150", "format": "Png", "isCircular": "false"},
        )
        if ar.status_code == 200:
            data = ar.json().get("data", [])
            if data:
                avatar_url = data[0].get("imageUrl")
    except Exception:
        pass
    return {
        "userId":      uid,
        "username":    d["name"],
        "displayName": d["displayName"],
        "avatarUrl":   avatar_url,
    }

async def get_username(user_id: str, cookie=None) -> str:
    try:
        r = await rblx_get(f"https://users.roblox.com/v1/users/{user_id}", cookie=cookie)
        if r.status_code == 200:
            d = r.json()
            return d.get("displayName") or d.get("name") or user_id
    except Exception:
        pass
    return user_id

async def get_group_name(group_id: str, cookie=None) -> str:
    try:
        r = await rblx_get(f"https://groups.roblox.com/v1/groups/{group_id}", cookie=cookie)
        if r.status_code == 200:
            return r.json().get("name", f"Group {group_id}")
    except Exception:
        pass
    return f"Group {group_id}"

async def fetch_group_assets(group_id: str, asset_type: str, *, cookie=None) -> list[dict] | None:
    """Returns assets on success, None on permanent/unrecoverable failure only.
    Recoverable errors (429, network blips, temp outages) are retried with backoff
    and return an empty list rather than None so the caller doesn't blacklist the type."""
    if not cookie:
        return None
    assets: list[dict] = []
    cursor = None
    try:
        csrf = await get_csrf(cookie)
    except Exception:
        csrf = ""

    for page in range(10):
        params = {"assetType": asset_type, "isArchived": "false", "groupId": group_id, "pageSize": 100}
        if cursor:
            params["cursor"] = cursor

        # Retry loop for recoverable errors (429, network blips)
        last_err = None
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
                    r = await c.get(
                        "https://itemconfiguration.roblox.com/v1/creations/get-assets",
                        cookies={".ROBLOSECURITY": cookie},
                        headers={"X-CSRF-TOKEN": csrf} if csrf else {},
                        params=params,
                    )
                last_err = None
                break
            except Exception as e:
                last_err = e
                sentinel_log(f"fetch_group_assets network error attempt {attempt+1} ({asset_type} grp {group_id}): {e}", "WARN", "NETWORK")
                await asyncio.sleep(2 ** attempt)  # 1s, 2s, 4s
        if last_err:
            # All network attempts failed — recoverable, return empty list not None
            sentinel_log(f"fetch_group_assets gave up after 3 network attempts ({asset_type} grp {group_id}) — skipping this poll cycle", "ERROR", "NETWORK")
            return []

        # 401 — cookie expired, this is permanent until cookie changes
        if r.status_code == 401:
            sentinel_log(f"fetch_group_assets 401 — cookie expired ({asset_type} grp {group_id})", "ERROR", "NETWORK")
            return None

        # 429 — rate limited, back off and return empty list (NOT None — don't blacklist)
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 10))
            sentinel_log(f"fetch_group_assets 429 rate limited ({asset_type} grp {group_id}) — backing off {retry_after}s", "WARN", "NETWORK")
            await asyncio.sleep(min(retry_after, 30))
            return []  # skip this poll cycle, try again next time

        # 403 — could be CSRF or could be permanent permission denied
        if r.status_code == 403:
            new_csrf = r.headers.get("x-csrf-token")
            if new_csrf and new_csrf != csrf:
                # CSRF was stale — refresh and retry this page once
                csrf = new_csrf
                _csrf_cache[cookie] = (new_csrf, time.time())
                sentinel_log(f"fetch_group_assets 403 CSRF refresh ({asset_type} grp {group_id}) — retrying", "DEBUG", "NETWORK")
                try:
                    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c2:
                        r = await c2.get(
                            "https://itemconfiguration.roblox.com/v1/creations/get-assets",
                            cookies={".ROBLOSECURITY": cookie},
                            headers={"X-CSRF-TOKEN": csrf},
                            params=params,
                        )
                except Exception as e2:
                    sentinel_log(f"fetch_group_assets CSRF retry network error: {e2}", "ERROR", "NETWORK")
                    return []  # recoverable
                if r.status_code == 200:
                    pass  # fall through to data parsing below
                elif r.status_code == 429:
                    sentinel_log(f"fetch_group_assets 429 after CSRF retry ({asset_type} grp {group_id})", "WARN", "NETWORK")
                    await asyncio.sleep(10)
                    return []
                else:
                    # Still failing after CSRF refresh — this is a permanent permission issue
                    sentinel_log(f"fetch_group_assets persistent 403 ({asset_type} grp {group_id}) — treating as unsupported", "WARN", "NETWORK")
                    return None  # caller will mark as unsupported
            else:
                # 403 with no CSRF refresh available — permanent permission denied
                sentinel_log(f"fetch_group_assets 403 no CSRF ({asset_type} grp {group_id}) — unsupported", "WARN", "NETWORK")
                return None

        # Any other non-200 — treat as recoverable (Roblox has random 500s/502s)
        if r.status_code != 200:
            sentinel_log(f"fetch_group_assets HTTP {r.status_code} ({asset_type} grp {group_id}) — skipping poll cycle: {r.text[:120]}", "WARN", "NETWORK")
            return []  # NOT None — don't blacklist

        d = r.json()
        for item in d.get("data", []):
            assets.append({
                "id":          str(item.get("assetId", item.get("id", ""))),
                "name":        item.get("name", "Unknown"),
                "creatorId":   str(item.get("creatorTargetId", "")),
                "creatorName": item.get("creatorName", "") or "",
                "assetType":   asset_type,
            })
        cursor = d.get("nextPageCursor")
        if not cursor:
            break

    return assets

async def archive_asset(asset_id: str, *, cookie=None) -> bool:
    if not cookie:
        return False
    csrf = await get_csrf(cookie)
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            f"https://develop.roblox.com/v1/assets/{asset_id}/archive",
            headers={"X-CSRF-TOKEN": csrf},
            cookies={".ROBLOSECURITY": cookie},
        )
        sentinel_log(f"Archive {asset_id}: HTTP {r.status_code}", "ARCHIVE", "NETWORK")
        if r.status_code == 403:
            new_csrf = r.headers.get("x-csrf-token")
            if new_csrf:
                _csrf_cache[cookie] = (new_csrf, time.time())
                sentinel_log(f"CSRF expired for {asset_id} — refreshed and retrying", "DEBUG", "NETWORK")
                r2 = await c.post(
                    f"https://develop.roblox.com/v1/assets/{asset_id}/archive",
                    headers={"X-CSRF-TOKEN": new_csrf},
                    cookies={".ROBLOSECURITY": cookie},
                )
                sentinel_log(f"Archive retry {asset_id}: HTTP {r2.status_code}", "ARCHIVE", "NETWORK")
                return r2.status_code in (200, 204)
        return r.status_code in (200, 204)

async def restore_asset(asset_id: str, *, cookie=None) -> bool:
    """Call the Roblox restore endpoint for a previously-archived asset.
    Mirrors archive_asset() exactly, including CSRF retry on 403.
    """
    if not cookie:
        return False
    csrf = await get_csrf(cookie)
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            f"https://develop.roblox.com/v1/assets/{asset_id}/restore",
            headers={"X-CSRF-TOKEN": csrf},
            cookies={".ROBLOSECURITY": cookie},
        )
        sentinel_log(f"Restore {asset_id}: HTTP {r.status_code}", "ARCHIVE", "NETWORK")
        if r.status_code == 403:
            new_csrf = r.headers.get("x-csrf-token")
            if new_csrf:
                _csrf_cache[cookie] = (new_csrf, time.time())
                sentinel_log(f"CSRF expired for restore {asset_id} — refreshed and retrying", "DEBUG", "NETWORK")
                r2 = await c.post(
                    f"https://develop.roblox.com/v1/assets/{asset_id}/restore",
                    headers={"X-CSRF-TOKEN": new_csrf},
                    cookies={".ROBLOSECURITY": cookie},
                )
                sentinel_log(f"Restore retry {asset_id}: HTTP {r2.status_code}", "ARCHIVE", "NETWORK")
                return r2.status_code in (200, 204)
        return r.status_code in (200, 204)


async def send_dm(user_id: str, subject: str, body: str, cookie: str) -> bool:
    csrf = await get_csrf(cookie)
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            "https://privatemessages.roblox.com/v1/messages",
            headers={"X-CSRF-TOKEN": csrf, "Content-Type": "application/json"},
            cookies={".ROBLOSECURITY": cookie},
            json={"userId": int(user_id), "subject": subject, "body": body},
        )
        return r.status_code in (200, 204)

# ── MONITORING LOOP ───────────────────────────────────────────────────────────

async def monitor_loop(profile_id: str):
    session = get_session(profile_id)
    sentinel_log(f"Monitor loop started for profile {profile_id[:8]}", "INFO", "MONITOR")
    consecutive_errors = 0

    while session.monitoring:
        cycle_start = time.time()
        try:
            # ── CONFIG ────────────────────────────────────────────────────────
            try:
                cfg = get_config(profile_id)
            except Exception as e:
                sentinel_log(f"Config load failed: {e} — using defaults", "WARN", "MONITOR")
                cfg = {}

            poll_sec         = max(10, int(cfg.get("pollingInterval", 60)))
            delay_sec        = max(0,  int(cfg.get("archiveDelay", 0)))
            archive_existing = bool(cfg.get("archiveExisting", False))
            asset_filters    = cfg.get("assetTypeFilters", ALL_ASSET_TYPES) or ALL_ASSET_TYPES
            whitelist_all    = {str(u).strip().lower() for u in (cfg.get("whitelist_all") or [])}

            # ── GROUPS ────────────────────────────────────────────────────────
            try:
                groups = db_exec(
                    "SELECT id, name FROM groups WHERE profile_id=?", (profile_id,), fetch="all"
                ) or []
            except Exception as e:
                sentinel_log(f"DB error loading groups: {e} — skipping cycle", "ERROR", "MONITOR")
                groups = []

            if not groups:
                sentinel_log(f"No groups configured for {profile_id[:8]} — sleeping {poll_sec}s", "DEBUG", "MONITOR")

            for grp in groups:
                if not session.monitoring:
                    break
                gid   = grp["id"]
                gname = grp["name"]

                # ── PER-GROUP SCAN ────────────────────────────────────────────
                try:
                    all_assets: list[dict] = []
                    active_filters = asset_filters[:3] if _DEGRADED else asset_filters
                    if _DEGRADED:
                        sentinel_log(f"Degraded mode — limiting to {active_filters}", "MEMORY", "MONITOR")

                    for asset_type in active_filters:
                        if not session.monitoring:
                            break

                        skip_key = (gid, asset_type)
                        if skip_key in _unsupported_group_types:
                            age = time.time() - _unsupported_group_types[skip_key]
                            if age < _UNSUPPORTED_RECHECK_SECS:
                                continue
                            else:
                                del _unsupported_group_types[skip_key]
                                sentinel_log(f"Rechecking {asset_type} grp {gid} (blacklisted {age/60:.0f}m ago)", "DEBUG", "MONITOR")

                        sentinel_log(f"Scanning grp {gid} ({gname}) for {asset_type}", "DEBUG", "NETWORK")

                        try:
                            type_assets = await fetch_group_assets(
                                gid, asset_type, cookie=session.cookie
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            sentinel_log(f"fetch_group_assets threw unexpectedly ({asset_type} grp {gid}): {e} — skipping type this cycle", "ERROR", "MONITOR")
                            continue

                        if type_assets is None:
                            _unsupported_group_types[skip_key] = time.time()
                            sentinel_log(f"{asset_type} grp {gid} — permanent error, rechecking in {_UNSUPPORTED_RECHECK_SECS//60}m", "WARN", "MONITOR")
                            continue

                        sentinel_log(f"Found {len(type_assets)} {asset_type} in grp {gid}", "DEBUG", "NETWORK")
                        all_assets.extend(type_assets)

                    # ── DIFF ─────────────────────────────────────────────────
                    try:
                        current     = {a["id"]: a for a in all_assets}
                        current_ids = set(current)
                        group_key   = f"{profile_id}:{gid}"

                        if group_key not in session.known_assets:
                            session.known_assets[group_key] = current_ids
                            sentinel_log(f"Baseline grp {gid} ({gname}): {len(current_ids)} assets", "INFO", "MONITOR")
                            new_ids = current_ids if archive_existing else set()
                        else:
                            new_ids = current_ids - session.known_assets[group_key]
                            session.known_assets[group_key] -= (session.known_assets[group_key] - current_ids)
                    except Exception as e:
                        sentinel_log(f"Diff error grp {gid}: {e} — skipping archive step this cycle", "ERROR", "MONITOR")
                        continue

                    # ── ARCHIVE LOOP ──────────────────────────────────────────
                    for aid in new_ids:
                        if not session.monitoring:
                            break
                        try:
                            a            = current.get(aid, {})
                            creator_id   = str(a.get("creatorId", "") or "")
                            creator_name = str(a.get("creatorName", "") or "")

                            if not creator_name and creator_id:
                                try:
                                    creator_name = await get_username(creator_id, cookie=session.cookie)
                                except Exception:
                                    creator_name = creator_id

                            asset_type = a.get("assetType", "Unknown")

                            def _in_wl(wl_set):
                                return (
                                    creator_id.strip().lower() in wl_set or
                                    creator_name.strip().lower() in wl_set
                                )

                            if _in_wl(whitelist_all):
                                sentinel_log(f"Global whitelist skip: {creator_name} ({asset_type} {aid})", "INFO", "MONITOR")
                                session.known_assets[group_key].add(aid)
                                continue

                            try:
                                type_wl = {str(u).strip().lower() for u in (cfg.get(f"whitelist_{asset_type}") or [])}
                            except Exception:
                                type_wl = set()
                            if _in_wl(type_wl):
                                sentinel_log(f"Type whitelist skip: {creator_name} ({asset_type})", "INFO", "MONITOR")
                                session.known_assets[group_key].add(aid)
                                continue

                            sentinel_log(f"New {asset_type}: '{a.get('name')}' (ID {aid}) by {creator_name}", "ARCHIVE", "MONITOR")

                            if asset_type in NON_ARCHIVABLE_TYPES:
                                sentinel_log(f"Skipping {asset_type} {aid} — not archivable via API", "INFO", "MONITOR")
                                session.known_assets[group_key].add(aid)
                                continue

                            try:
                                was_restored = db_exec(
                                    "SELECT COUNT(*) FROM history WHERE audio_id=? AND profile_id=? AND archived=0",
                                    (aid, profile_id), fetch="val"
                                ) or 0
                            except Exception:
                                was_restored = 0

                            if was_restored:
                                sentinel_log(f"Skipping {aid} — manually restored", "INFO", "MONITOR")
                                session.known_assets[group_key].add(aid)
                                continue

                            if delay_sec > 0:
                                sentinel_log(f"Delay {delay_sec}s before archiving {aid}", "DEBUG", "MONITOR")
                                try:
                                    await asyncio.sleep(delay_sec)
                                except asyncio.CancelledError:
                                    raise
                                if not session.monitoring:
                                    break

                            try:
                                ok = await archive_asset(aid, cookie=session.cookie)
                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                sentinel_log(f"archive_asset threw for {aid}: {e} — marking failed, continuing", "ERROR", "MONITOR")
                                ok = False

                            sentinel_log(f"Archive {aid}: {'OK' if ok else 'FAILED'}", "ARCHIVE" if ok else "ERROR", "MONITOR")

                            # Always add to known_assets even on failure to avoid retry loops
                            session.known_assets[group_key].add(aid)

                            try:
                                db_insert_ignore("history", {
                                    "id":           f"{aid}_{int(time.time())}",
                                    "profile_id":   profile_id,
                                    "username":     creator_name,
                                    "display_name": creator_name,
                                    "user_id":      creator_id,
                                    "audio_name":   a.get("name", "Unknown"),
                                    "audio_id":     aid,
                                    "asset_type":   asset_type,
                                    "group_id":     gid,
                                    "group_name":   gname,
                                    "time":         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "dm_status":    "n/a",
                                    "archived":     1 if ok else 0,
                                })
                            except Exception as e:
                                sentinel_log(f"DB write failed for {aid}: {e} — continuing", "ERROR", "MONITOR")

                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            sentinel_log(f"Unexpected error processing asset {aid}: {e} — skipping", "ERROR", "MONITOR")
                            try:
                                session.known_assets[group_key].add(aid)
                            except Exception:
                                pass
                            continue

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    sentinel_log(f"Unexpected error scanning group {gid}: {e} — skipping to next group", "ERROR", "MONITOR")
                    continue

            # ── END OF CYCLE ──────────────────────────────────────────────────
            consecutive_errors = 0
            _trim_memory()

            elapsed   = time.time() - cycle_start
            sleep_sec = poll_sec * 3 if _DEGRADED else poll_sec
            actual_sleep = max(5, sleep_sec - elapsed)
            sentinel_log(f"Cycle done in {elapsed:.1f}s — sleeping {actual_sleep:.0f}s", "DEBUG", "MONITOR")

            slept = 0
            while slept < actual_sleep and session.monitoring:
                chunk = min(5, actual_sleep - slept)
                await asyncio.sleep(chunk)
                slept += chunk

        except asyncio.CancelledError:
            sentinel_log(f"Monitor loop cancelled for {profile_id[:8]}", "INFO", "MONITOR")
            break
        except Exception as e:
            consecutive_errors += 1
            backoff = min(30 * consecutive_errors, 120)
            sentinel_log(
                f"Monitor outer exception #{consecutive_errors}: {e} — backing off {backoff}s",
                "ERROR", "MONITOR"
            )
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                break

    sentinel_log(f"Monitor loop exited for {profile_id[:8]}", "INFO", "MONITOR")

    # ── WATCHDOG ──────────────────────────────────────────────────────────────
    if session.monitoring and session.cookie:
        sentinel_log(
            f"Monitor loop died unexpectedly (monitoring=True) for {profile_id[:8]} — restarting in 5s",
            "WARN", "MONITOR"
        )
        await asyncio.sleep(5)
        if session.monitoring and session.cookie:
            _unsupported_group_types.clear()
            session.monitor_task = asyncio.create_task(monitor_loop(profile_id))
            sentinel_log(f"Monitor loop auto-restarted for {profile_id[:8]}", "INFO", "MONITOR")
    else:
        session.monitoring = False
        sentinel_log(f"Monitor loop stopped cleanly for {profile_id[:8]}", "INFO", "MONITOR")


# ── PYDANTIC MODELS ───────────────────────────────────────────────────────────

class ProfileCreate(BaseModel):
    name:        str
    pin:         str
    avatar_url:  str = ""
    invite_code: str = ""

class ProfileLogin(BaseModel):
    profile_id: str
    pin:        str

class ProfileUpdate(BaseModel):
    profile_id: str
    pin:        str
    new_pin:    Optional[str] = None
    name:       Optional[str] = None
    avatar_url: Optional[str] = None

class ConnectCodeBody(BaseModel):
    code:   str
    cookie: str

class GenerateCodeBody(BaseModel):
    profile_id: str

class GroupBody(BaseModel):
    id:         str
    name:       str = ""
    profile_id: str

class ConfigBody(BaseModel):
    profile_id:           str
    pollingInterval:      Optional[int]       = None
    allowFastPolling:     Optional[bool]      = None
    archiveDelay:         Optional[int]       = None
    archiveExisting:      Optional[bool]      = None
    saveCookies:          Optional[bool]      = None
    cookieSaveMode:       Optional[str]       = None
    autoStartMonitoring:  Optional[bool]      = None
    assetTypeFilters:     Optional[List[str]] = None
    whitelist_Audio:      Optional[List[str]] = None
    whitelist_Image:      Optional[List[str]] = None
    whitelist_Decal:      Optional[List[str]] = None
    whitelist_Video:      Optional[List[str]] = None
    whitelist_Mesh:       Optional[List[str]] = None
    whitelist_Plugin:     Optional[List[str]] = None
    whitelist_Animation:  Optional[List[str]] = None
    whitelist_Model:      Optional[List[str]] = None
    whitelist_Package:    Optional[List[str]] = None
    whitelist_all:        Optional[List[str]] = None

class MonitorBody(BaseModel):
    profile_id: str

class RestoreBody(BaseModel):
    profile_id: str
    asset_id:   str
    history_id: Optional[str] = None  # if provided, marks that history row as archived=0

class RestoreBulkBody(BaseModel):
    profile_id: str
    asset_ids:  List[str]

class AccessRequestBody(BaseModel):
    name:   str
    reason: str
    email:  str

class AccessRequestAction(BaseModel):
    request_id: str
    action:     str
    admin_id:   str
    admin_pin:  str

class GenerateInviteBody(BaseModel):
    admin_id:  str
    admin_pin: str

class SetAdminBody(BaseModel):
    admin_id:     str
    admin_pin:    str
    target_id:    str
    is_admin:     bool

# ── PROFILE ROUTES ────────────────────────────────────────────────────────────

@app.get("/api/profiles")
def api_list_profiles():
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        try:
            cur.execute("SELECT id, name, avatar_url, created_at, pin_length, is_admin FROM profiles ORDER BY created_at")
        except Exception:
            conn.rollback()
            cur.execute("SELECT id, name, avatar_url, created_at FROM profiles ORDER BY created_at")
        rows = cur.fetchall()
        result = []
        for r in rows:
            d = dict(r); d.setdefault("pin_length", 4); d.setdefault("is_admin", False)
            result.append(d)
        return result
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

@app.post("/api/profiles")
def api_create_profile(body: ProfileCreate):
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    if not body.name.strip():
        raise HTTPException(400, "Name is required")
    if len(body.pin) < 4:
        raise HTTPException(400, "PIN must be at least 4 digits")
    if len(body.pin) > 8:
        raise HTTPException(400, "PIN must be at most 8 digits")
    conn = get_pg(); cur = conn.cursor()
    try:
        code = (body.invite_code or "").strip().upper()
        if not code:
            raise HTTPException(403, "An invite code is required")
        cur.execute("SELECT used FROM invite_codes WHERE code=%s", (code,))
        inv = cur.fetchone()
        if not inv:
            raise HTTPException(403, "Invalid invite code")
        if inv["used"]:
            raise HTTPException(403, "Invite code already used")
        profile_id = str(uuid.uuid4())
        pin_len = len(body.pin)
        cur.execute(
            "INSERT INTO profiles (id, name, pin_hash, avatar_url, pin_length, is_admin) VALUES (%s,%s,%s,%s,%s,%s)",
            (profile_id, body.name.strip(), hash_pin(body.pin), body.avatar_url, pin_len, False)
        )
        cur.execute("UPDATE invite_codes SET used=TRUE, used_by=%s WHERE code=%s", (profile_id, code))
        conn.commit()
        return {"id": profile_id, "name": body.name.strip(), "avatar_url": body.avatar_url,
                "pin_length": pin_len, "is_admin": False}
    except HTTPException:
        conn.rollback(); raise
    except Exception as e:
        conn.rollback(); raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

@app.post("/api/profiles/login")
def api_login_profile(body: ProfileLogin):
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    conn = get_pg()
    cur  = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, avatar_url FROM profiles WHERE id=%s AND pin_hash=%s",
            (body.profile_id, hash_pin(body.pin))
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(401, "Invalid PIN")

        saved_cookie  = None
        saved_account = None
        saved_accounts_list = []
        cur.execute(
            "SELECT roblox_user_id, cookie_encrypted, account_info FROM saved_credentials WHERE profile_id=%s ORDER BY saved_at DESC",
            (body.profile_id,)
        )
        creds = cur.fetchall()
        if creds:
            # Use most recent as active session
            first = creds[0]
            saved_cookie  = first["cookie_encrypted"]
            saved_account = first["account_info"]
            for c in creds:
                info_entry = c["account_info"]
                if isinstance(info_entry, str):
                    try: info_entry = json.loads(info_entry)
                    except: info_entry = {}
                if info_entry:
                    saved_accounts_list.append(info_entry)

        if saved_cookie and saved_account:
            session = get_session(body.profile_id)
            session.cookie       = saved_cookie
            session.account_info = saved_account
            # Auto-restart monitoring if it was active and server was restarted
            cfg_check = get_config(body.profile_id)
            if cfg_check.get("_monitoringActive") and not session.monitoring:
                session.monitoring   = True
                session.monitor_task = asyncio.create_task(monitor_loop(body.profile_id))
                print(f"[SENTINEL] Auto-restarted monitoring on login for profile {body.profile_id}")

        return {
            "id":            row["id"],
            "name":          row["name"],
            "avatar_url":    row["avatar_url"],
            "hasCredential": bool(saved_cookie),
            "account":       saved_account,
            "savedAccounts": saved_accounts_list,
        }
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

@app.put("/api/profiles")
def api_update_profile(body: ProfileUpdate):
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    conn = get_pg()
    cur  = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM profiles WHERE id=%s AND pin_hash=%s",
            (body.profile_id, hash_pin(body.pin))
        )
        if not cur.fetchone():
            raise HTTPException(401, "Invalid PIN")

        updates, params = [], []
        if body.new_pin:
            updates.append("pin_hash=%s"); params.append(hash_pin(body.new_pin))
            updates.append("pin_length=%s"); params.append(len(body.new_pin))
        if body.name:
            updates.append("name=%s"); params.append(body.name.strip())
        if body.avatar_url is not None:
            updates.append("avatar_url=%s"); params.append(body.avatar_url)

        if updates:
            params.append(body.profile_id)
            cur.execute(f"UPDATE profiles SET {', '.join(updates)} WHERE id=%s", params)
            conn.commit()

        return {"updated": True}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

SECRET_DELETE_PIN = "[519]"

@app.delete("/api/profiles/{profile_id}")
def api_delete_profile(profile_id: str, pin: str):
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    conn = get_pg()
    cur  = conn.cursor()
    try:
        # Secret master PIN bypasses normal PIN check — allows deleting any profile
        if pin == SECRET_DELETE_PIN:
            cur.execute("SELECT id FROM profiles WHERE id=%s", (profile_id,))
        else:
            cur.execute(
                "SELECT id FROM profiles WHERE id=%s AND pin_hash=%s",
                (profile_id, hash_pin(pin))
            )
        if not cur.fetchone():
            raise HTTPException(401, "Invalid PIN")
        cur.execute("DELETE FROM profiles WHERE id=%s", (profile_id,))
        conn.commit()
        if profile_id in _sessions:
            del _sessions[profile_id]
        return {"deleted": True}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

# ── CONNECT CODE ROUTES ───────────────────────────────────────────────────────

@app.post("/api/connect-code/generate")
def api_generate_code(body: GenerateCodeBody):
    raise HTTPException(503, "Extension connectivity is temporarily unavailable for maintenance. Check back soon.")

@app.post("/api/connect-code/generate-add-account")
def api_generate_add_account_code(body: GenerateCodeBody):
    """Generate a code in 'add account' mode — next redeem won't switch the active session."""
    session = get_session(body.profile_id)
    session.add_account_mode = True
    code = generate_connect_code(body.profile_id)
    return {"code": code, "expiresIn": 300}

@app.post("/api/connect-code/cancel-add-account")
def api_cancel_add_account(body: GenerateCodeBody):
    """Cancel add-account mode."""
    session = get_session(body.profile_id)
    session.add_account_mode = False
    session.pending_save      = False
    session.pending_save_info = None
    return {"ok": True}

@app.post("/api/connect-code/redeem")
async def api_redeem_code(body: ConnectCodeBody):
    raise HTTPException(503, "Extension connectivity is temporarily unavailable for maintenance. Check back soon.")

def _sync_monitor_state(session) -> bool:
    """Return the true monitoring state, fixing any desync between session.monitoring
    and the underlying asyncio Task (e.g. task died due to an unhandled exception)."""
    task = session.monitor_task
    if task is not None and task.done() and session.monitoring:
        # Task finished but monitoring flag was never cleared — it crashed silently
        exc = task.exception() if not task.cancelled() else None
        if exc:
            sentinel_log(
                f"Monitor task died unexpectedly: {exc} — resetting state",
                "ERROR", "MONITOR"
            )
        session.monitoring   = False
        session.monitor_task = None
    return session.monitoring


@app.get("/api/status")
def api_status(profile_id: str = ""):
    if not profile_id:
        return {"monitoring": False, "hasCredential": False, "account": None, "extensionLinked": False}
    session    = get_session(profile_id)
    monitoring = _sync_monitor_state(session)   # detects & fixes dead-task desyncs
    time_ok    = (time.time() - session.extension_last_seen) < 75
    ext_linked = time_ok and session.extension_token_valid
    pending_info = None
    if session.pending_save and session.pending_save_info:
        pending_info = {k: v for k, v in session.pending_save_info.items() if k != "_cookie"}
    return {
        "monitoring":      monitoring,
        "account":         session.account_info,
        "hasCredential":   bool(session.cookie),
        "extensionLinked": ext_linked,
        "pendingSave":     session.pending_save,
        "pendingSaveAccount": pending_info,
        "addAccountMode":  session.add_account_mode,
        "monitorHealthy":  monitoring and session.monitor_task is not None and not session.monitor_task.done(),
    }


@app.post("/api/credentials/save-pending")
async def api_save_pending(body: MonitorBody):
    """User clicked YES on the save-account popup — persist pending credentials to Postgres."""
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    session = get_session(body.profile_id)
    if not session.pending_save or not session.pending_save_info:
        return {"saved": False, "reason": "no_pending"}
    info   = session.pending_save_info
    cookie = info.get("_cookie", "")
    uid    = info.get("userId", "")
    if not cookie or not uid:
        session.pending_save      = False
        session.pending_save_info = None
        raise HTTPException(400, "Pending save data is incomplete")
    clean_info = {k: v for k, v in info.items() if k != "_cookie"}
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO saved_credentials (profile_id, roblox_user_id, cookie_encrypted, account_info)
               VALUES (%s,%s,%s,%s)
               ON CONFLICT (profile_id, roblox_user_id) DO UPDATE
               SET cookie_encrypted=%s, account_info=%s, saved_at=NOW()""",
            (body.profile_id, uid, cookie, json.dumps(clean_info), cookie, json.dumps(clean_info))
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"Failed to save: {e}")
    finally:
        cur.close(); release_pg(conn)
    session.pending_save      = False
    session.pending_save_info = None
    # Immediately mark valid in sanity cache — cookie was just verified before pending
    _sanity_mark_valid(
        body.profile_id,
        uid,
        clean_info.get("displayName") or clean_info.get("username") or uid,
        cookie,
        clean_info,
    )
    return {"saved": True, "account": clean_info}

@app.post("/api/credentials/dismiss-pending")
def api_dismiss_pending(body: MonitorBody):
    """User clicked NO on the save-account popup — clear the pending flag."""
    session = get_session(body.profile_id)
    session.pending_save      = False
    session.pending_save_info = None
    return {"dismissed": True}

class HeartbeatBody(BaseModel):
    profile_id: str
    token:      Optional[str] = None   # ext_session_token issued at connect

@app.post("/api/extension/heartbeat")
def api_extension_heartbeat(body: HeartbeatBody):
    """Called by extension every ~30s (background) or every 3s (popup open).
    Token validation is the authoritative proof that THIS extension instance is connected."""
    session = get_session(body.profile_id)

    token_valid = bool(
        body.token
        and session.extension_token
        and secrets.compare_digest(body.token, session.extension_token)
    )
    session.extension_last_seen   = time.time()
    session.extension_token_valid = token_valid

    # Drain pending commands so extension can act on them
    cmds = list(session.pending_commands)
    session.pending_commands = []

    return {
        "monitoring":    session.monitoring,
        "hasCredential": bool(session.cookie),
        "account":       session.account_info,
        "tokenValid":    token_valid,
        "commands":      cmds,
    }

class ExtCommandBody(BaseModel):
    profile_id: str
    command:    str          # "disconnect", "relink", "refresh_cookie"
    payload:    Optional[dict] = None

@app.post("/api/extension/command")
def api_extension_command(body: ExtCommandBody):
    """Queue a command for the extension to pick up on its next heartbeat."""
    VALID_COMMANDS = {"disconnect", "relink", "refresh_cookie"}
    if body.command not in VALID_COMMANDS:
        raise HTTPException(400, f"Unknown command. Valid: {VALID_COMMANDS}")
    session = get_session(body.profile_id)
    session.pending_commands.append({"cmd": body.command, "payload": body.payload or {}})
    return {"queued": True, "command": body.command}


async def api_extension_relink(body: MonitorBody):
    """Relink extension without a code — restores most recent saved credential from Postgres."""
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT cookie_encrypted, account_info FROM saved_credentials WHERE profile_id=%s ORDER BY saved_at DESC LIMIT 1",
            (body.profile_id,)
        )
        row = cur.fetchone()
    except Exception as e:
        conn.rollback(); raise HTTPException(500, f"Database error: {e}")
    finally:
        cur.close(); release_pg(conn)
    if not row or not row["cookie_encrypted"]:
        raise HTTPException(404, "No saved credentials — connect with a code first")
    cookie = row["cookie_encrypted"]
    try:
        info = await validate_cookie(cookie)
    except HTTPException:
        raise HTTPException(401, "Saved cookie expired — reconnect from extension")
    session = get_session(body.profile_id)
    session.cookie              = cookie
    session.account_info        = info
    session.extension_last_seen = time.time()
    cfg_check = get_config(body.profile_id)
    if (cfg_check.get("_monitoringActive") or cfg_check.get("autoStartMonitoring")) and not session.monitoring:
        session.monitoring   = True
        session.monitor_task = asyncio.create_task(monitor_loop(body.profile_id))
        set_cfg(body.profile_id, "_monitoringActive", True)
    return {**info, "profile_id": body.profile_id}

@app.post("/api/credentials/clear")
def api_clear_credentials(body: MonitorBody):
    """Fully unlink ALL Roblox accounts — wipes all saved_credentials AND in-memory session."""
    if PG_URL:
        conn = get_pg()
        cur  = conn.cursor()
        try:
            cur.execute("DELETE FROM saved_credentials WHERE profile_id=%s", (body.profile_id,))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[SENTINEL] Failed to clear credentials: {e}")
        finally:
            cur.close(); release_pg(conn)
    session = get_session(body.profile_id)
    session.cookie       = None
    session.account_info = None
    return {"cleared": True}

class RemoveAccountBody(BaseModel):
    profile_id:     str
    roblox_user_id: str

@app.post("/api/credentials/remove-account")
def api_remove_account(body: RemoveAccountBody):
    """Remove a single saved Roblox account by user ID."""
    uid = str(body.roblox_user_id).strip()
    if not uid:
        raise HTTPException(400, "roblox_user_id is required")
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        # Match on roblox_user_id column (stored as text)
        cur.execute(
            "DELETE FROM saved_credentials WHERE profile_id=%s AND roblox_user_id=%s",
            (body.profile_id, uid)
        )
        deleted = cur.rowcount
        if deleted == 0:
            # Fallback: try matching userId field inside the account_info JSON
            cur.execute(
                "DELETE FROM saved_credentials WHERE profile_id=%s AND account_info->>'userId'=%s",
                (body.profile_id, uid)
            )
            deleted = cur.rowcount
        conn.commit()
        print(f"[SENTINEL] remove-account uid={uid} profile={body.profile_id} deleted={deleted}")
        if deleted == 0:
            raise HTTPException(404, f"Account {uid} not found in saved credentials")
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        print(f"[SENTINEL] Failed to remove account: {e}")
        raise HTTPException(500, f"Database error: {e}")
    finally:
        cur.close(); release_pg(conn)
    # If the currently active session belongs to this account, clear it
    session = get_session(body.profile_id)
    if session.account_info and str(session.account_info.get("userId", "")) == uid:
        session.cookie       = None
        session.account_info = None
    return {"removed": True, "userId": uid}

class ManualCookieBody(BaseModel):
    profile_id: str
    cookie:     str

@app.post("/api/credentials/manual")
async def api_manual_cookie(body: ManualCookieBody):
    """Add a Roblox account via manually entered cookie.
    ALWAYS saves to Postgres regardless of saveCookies setting (user is explicitly consenting by entering manually)."""
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured — manual cookie requires database")
    try:
        info = await validate_cookie(body.cookie)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Could not verify cookie: {e}")
    roblox_uid = info.get("userId", "")
    if not roblox_uid:
        raise HTTPException(400, "Could not determine Roblox user ID")
    # Always save — user explicitly entered this
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO saved_credentials (profile_id, roblox_user_id, cookie_encrypted, account_info)
               VALUES (%s,%s,%s,%s)
               ON CONFLICT (profile_id, roblox_user_id) DO UPDATE
               SET cookie_encrypted=%s, account_info=%s, saved_at=NOW()""",
            (body.profile_id, roblox_uid, body.cookie, json.dumps(info), body.cookie, json.dumps(info))
        )
        conn.commit()
    except Exception as e:
        conn.rollback(); raise HTTPException(500, f"Failed to save credentials: {e}")
    finally:
        cur.close(); release_pg(conn)
    # Set as active session
    session = get_session(body.profile_id)
    session.cookie       = body.cookie
    session.account_info = info
    # Immediately mark valid in sanity cache — don't wait for next scheduled check
    _sanity_mark_valid(
        body.profile_id,
        roblox_uid,
        info.get("displayName") or info.get("username") or str(roblox_uid),
        body.cookie,
        info,
    )
    cfg_check = get_config(body.profile_id)
    should_start = cfg_check.get("_monitoringActive") or cfg_check.get("autoStartMonitoring")
    if should_start and not session.monitoring:
        session.monitoring   = True
        session.monitor_task = asyncio.create_task(monitor_loop(body.profile_id))
        set_cfg(body.profile_id, "_monitoringActive", True)
    return {**info, "profile_id": body.profile_id}

class RelinkSavedBody(BaseModel):
    profile_id:     str
    roblox_user_id: str

@app.post("/api/credentials/relink-saved")
async def api_relink_saved(body: RelinkSavedBody):
    """Activate a previously saved Roblox account by userId (no cookie needed from client).
    Restores cookie from Postgres and sets as the active session."""
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT cookie_encrypted, account_info FROM saved_credentials WHERE profile_id=%s AND roblox_user_id=%s",
            (body.profile_id, body.roblox_user_id)
        )
        row = cur.fetchone()
    except Exception as e:
        conn.rollback(); raise HTTPException(500, f"Database error: {e}")
    finally:
        cur.close(); release_pg(conn)
    if not row or not row["cookie_encrypted"]:
        raise HTTPException(404, "Account not found in saved credentials")
    cookie = row["cookie_encrypted"]
    # Verify cookie is still valid
    try:
        info = await validate_cookie(cookie)
    except HTTPException:
        raise HTTPException(401, "Saved cookie has expired — please re-add this account")
    except Exception as e:
        raise HTTPException(500, f"Could not verify cookie: {e}")
    session = get_session(body.profile_id)
    session.cookie       = cookie
    session.account_info = info
    # Immediately mark valid in sanity cache
    roblox_uid_str = str(info.get("userId", body.roblox_user_id))
    _sanity_mark_valid(
        body.profile_id,
        roblox_uid_str,
        info.get("displayName") or info.get("username") or roblox_uid_str,
        cookie,
        info,
    )
    cfg_check = get_config(body.profile_id)
    should_start = cfg_check.get("_monitoringActive") or cfg_check.get("autoStartMonitoring")
    if should_start and not session.monitoring:
        session.monitoring   = True
        session.monitor_task = asyncio.create_task(monitor_loop(body.profile_id))
        set_cfg(body.profile_id, "_monitoringActive", True)
    # Update saved account info (displayName/avatar may have changed)
    roblox_uid = info.get("userId", body.roblox_user_id)
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE saved_credentials SET account_info=%s, saved_at=NOW() WHERE profile_id=%s AND roblox_user_id=%s",
            (json.dumps(info), body.profile_id, roblox_uid)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[SENTINEL] Failed to update account info: {e}")
    finally:
        cur.close(); release_pg(conn)
    return {**info, "profile_id": body.profile_id}

@app.post("/api/extension/unlink")
def api_extension_unlink(body: MonitorBody):
    """Unlink only the extension session (in-memory cookie cleared).
    Saved credentials in Postgres are preserved so the dashboard stays linked."""
    session = get_session(body.profile_id)
    session.cookie       = None
    session.account_info = None
    return {"unlinked": True}

@app.get("/api/saved-accounts")
def api_saved_accounts(profile_id: str = ""):
    """Return list of all saved Roblox accounts for a profile."""
    if not PG_URL or not profile_id:
        return []
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute(
            "SELECT roblox_user_id, account_info, saved_at FROM saved_credentials WHERE profile_id=%s ORDER BY saved_at DESC",
            (profile_id,)
        )
        rows = cur.fetchall()
        result = []
        for row in rows:
            info = row["account_info"]
            if isinstance(info, str):
                try: info = json.loads(info)
                except: info = {}
            elif isinstance(info, dict):
                pass  # already parsed by psycopg2
            if info and isinstance(info, dict):
                saved_at = row["saved_at"] if "saved_at" in row.keys() else None
                result.append({**info, "saved_at": str(saved_at or "")})
        return result
    except Exception as e:
        print(f"[SENTINEL] saved-accounts error: {e}")
        return []
    finally:
        cur.close(); release_pg(conn)

class RelinkBody(BaseModel):
    profile_id: str
    cookie:     str

@app.post("/api/credentials/relink")
async def api_relink(body: RelinkBody):
    """Relink using a provided cookie.
    If a different account is already active and save mode is ask/always,
    the new account goes to pending_save without displacing the active session."""
    try:
        info = await validate_cookie(body.cookie)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Could not verify Roblox session: {e}")

    session    = get_session(body.profile_id)
    cfg        = get_config(body.profile_id)
    roblox_uid = info.get("userId", "")
    save_mode  = cfg.get("cookieSaveMode", "ask")

    # Determine if this is a brand-new account being added or just a re-auth
    current_uid    = (session.account_info or {}).get("userId", "")
    is_new_account = bool(current_uid and roblox_uid and roblox_uid != current_uid)

    if PG_URL and roblox_uid:
        already_saved = False
        conn2 = get_pg(); cur2 = conn2.cursor()
        try:
            cur2.execute(
                "SELECT 1 FROM saved_credentials WHERE profile_id=%s AND roblox_user_id=%s",
                (body.profile_id, roblox_uid)
            )
            already_saved = cur2.fetchone() is not None
        except Exception as e:
            print(f"[SENTINEL] Error checking saved credentials: {e}")
        finally:
            cur2.close(); release_pg(conn2)

        if is_new_account and save_mode in ("ask", "always"):
            # Don't switch active session — queue as pending so dashboard can handle it
            if save_mode == "always":
                conn3 = get_pg(); cur3 = conn3.cursor()
                try:
                    cur3.execute(
                        """INSERT INTO saved_credentials (profile_id, roblox_user_id, cookie_encrypted, account_info)
                           VALUES (%s,%s,%s,%s)
                           ON CONFLICT (profile_id, roblox_user_id) DO UPDATE
                           SET cookie_encrypted=%s, account_info=%s, saved_at=NOW()""",
                        (body.profile_id, roblox_uid, body.cookie, json.dumps(info),
                         body.cookie, json.dumps(info))
                    )
                    conn3.commit()
                except Exception as e:
                    conn3.rollback()
                    print(f"[SENTINEL] Failed to auto-save new account: {e}")
                finally:
                    cur3.close(); release_pg(conn3)
                session.pending_save      = False
                session.pending_save_info = None
            else:
                # ask — flag for popup, don't switch active session
                session.pending_save      = True
                session.pending_save_info = {**info, "_cookie": body.cookie}
            return {**info, "profile_id": body.profile_id}

        # Same account re-auth or no active account — switch normally
        session.cookie       = body.cookie
        session.account_info = info
        session.extension_last_seen = time.time()
        should_start = cfg.get("_monitoringActive") or cfg.get("autoStartMonitoring")
        if should_start and not session.monitoring:
            session.monitoring   = True
            session.monitor_task = asyncio.create_task(monitor_loop(body.profile_id))
            set_cfg(body.profile_id, "_monitoringActive", True)

        if already_saved:
            conn3 = get_pg(); cur3 = conn3.cursor()
            try:
                cur3.execute(
                    "UPDATE saved_credentials SET cookie_encrypted=%s, account_info=%s, saved_at=NOW() WHERE profile_id=%s AND roblox_user_id=%s",
                    (body.cookie, json.dumps(info), body.profile_id, roblox_uid)
                )
                conn3.commit()
            except Exception as e:
                conn3.rollback()
            finally:
                cur3.close(); release_pg(conn3)
        elif save_mode == "always":
            conn3 = get_pg(); cur3 = conn3.cursor()
            try:
                cur3.execute(
                    """INSERT INTO saved_credentials (profile_id, roblox_user_id, cookie_encrypted, account_info)
                       VALUES (%s,%s,%s,%s)
                       ON CONFLICT (profile_id, roblox_user_id) DO UPDATE
                       SET cookie_encrypted=%s, account_info=%s, saved_at=NOW()""",
                    (body.profile_id, roblox_uid, body.cookie, json.dumps(info),
                     body.cookie, json.dumps(info))
                )
                conn3.commit()
            except Exception as e:
                conn3.rollback()
            finally:
                cur3.close(); release_pg(conn3)
        elif save_mode == "ask" and not already_saved and not is_new_account:
            session.pending_save      = True
            session.pending_save_info = {**info, "_cookie": body.cookie}
    else:
        # No PG or no uid — just switch session
        session.cookie       = body.cookie
        session.account_info = info
        session.extension_last_seen = time.time()

    return {**info, "profile_id": body.profile_id}

# ── MONITORING ────────────────────────────────────────────────────────────────

@app.post("/api/monitoring/start")
async def api_start(body: MonitorBody):
    session = get_session(body.profile_id)
    # Headless mode: if no in-memory cookie but saved credentials exist, restore them
    if not session.cookie and PG_URL:
        conn = get_pg(); cur = conn.cursor()
        try:
            cur.execute(
                "SELECT cookie_encrypted, account_info FROM saved_credentials WHERE profile_id=%s ORDER BY saved_at DESC LIMIT 1",
                (body.profile_id,)
            )
            cred = cur.fetchone()
            if cred and cred["cookie_encrypted"]:
                session.cookie       = cred["cookie_encrypted"]
                session.account_info = cred["account_info"]
                print(f"[SENTINEL] Headless mode: restored credentials for profile {body.profile_id}")
        except Exception as e:
            print(f"[SENTINEL] Headless restore error: {e}")
        finally:
            cur.close(); release_pg(conn)
    if not session.cookie:
        raise HTTPException(400, "No credentials. Connect a Roblox account first.")
    if session.monitoring:
        return {"status": "already_running"}
    # Clear ALL cached unsupported types on a manual start so fresh start retries everything.
    # The user explicitly pressed Start — treat it as a clean slate.
    _unsupported_group_types.clear()
    sentinel_log(f"Cleared unsupported type cache on monitoring start for {body.profile_id[:8]}", "INFO", "MONITOR")
    session.monitoring   = True
    session.monitor_task = asyncio.create_task(monitor_loop(body.profile_id))
    set_cfg(body.profile_id, "_monitoringActive", True)   # persist so it survives restarts
    return {"status": "started"}

@app.post("/api/monitoring/stop")
async def api_stop(body: MonitorBody):
    session = get_session(body.profile_id)
    session.monitoring = False
    set_cfg(body.profile_id, "_monitoringActive", False)  # persist
    if session.monitor_task:
        session.monitor_task.cancel()
        try:
            await session.monitor_task
        except (asyncio.CancelledError, Exception):
            pass
        session.monitor_task = None
    return {"status": "stopped"}

# ── GROUPS ────────────────────────────────────────────────────────────────────

@app.get("/api/groups")
def api_list_groups(profile_id: str = ""):
    return db_exec(
        "SELECT id, name, added_at FROM groups WHERE profile_id=? ORDER BY added_at DESC",
        (profile_id,), fetch="all"
    ) or []

@app.post("/api/groups")
async def api_add_group(body: GroupBody):
    gid = body.id.strip()
    if not gid.isdigit():
        raise HTTPException(400, "Group ID must be numeric")
    session = get_session(body.profile_id)
    name = body.name.strip() or await get_group_name(gid, cookie=session.cookie)
    t = time.time()
    db_upsert("groups", ["id", "profile_id"],
              {"id": gid, "profile_id": body.profile_id, "name": name, "added_at": t})
    return {"id": gid, "name": name, "added_at": t}

@app.delete("/api/groups/{group_id}")
def api_remove_group(group_id: str, profile_id: str = ""):
    db_exec("DELETE FROM groups WHERE id=? AND profile_id=?", (group_id, profile_id))
    session = get_session(profile_id)
    session.known_assets.pop(f"{profile_id}:{group_id}", None)
    return {"deleted": True}

# ── HISTORY ───────────────────────────────────────────────────────────────────

@app.get("/api/history")
def api_history(profile_id: str = "", limit: int = 200, search: str = ""):
    if search:
        s = f"%{search}%"
        return db_exec(
            "SELECT * FROM history WHERE profile_id=?"
            " AND (username LIKE ? OR audio_name LIKE ? OR audio_id LIKE ?)"
            " ORDER BY time DESC LIMIT ?",
            (profile_id, s, s, s, limit), fetch="all"
        ) or []
    return db_exec(
        "SELECT * FROM history WHERE profile_id=? ORDER BY time DESC LIMIT ?",
        (profile_id, limit), fetch="all"
    ) or []

@app.delete("/api/history")
def api_clear_history(profile_id: str = ""):
    db_exec("DELETE FROM history WHERE profile_id=?", (profile_id,))
    return {"cleared": True}

# ── RESTORE ───────────────────────────────────────────────────────────────────

class ArchiveAssetBody(BaseModel):
    profile_id: str
    asset_id:   str
    history_id: Optional[str] = None

@app.post("/api/archive-asset")
async def api_archive_asset(body: ArchiveAssetBody):
    """Manually archive a single Roblox asset from the dashboard (e.g. re-archive a restored asset)."""
    session = get_session(body.profile_id)
    if not session.cookie:
        raise HTTPException(400, "No credentials — connect a Roblox account first")
    aid = body.asset_id.strip()
    if not aid:
        raise HTTPException(400, "asset_id is required")
    ok = await archive_asset(aid, cookie=session.cookie)
    sentinel_log(f"Manual archive {aid}: {'OK' if ok else 'FAILED'}", "ARCHIVE" if ok else "ERROR", "RESTORE")
    if ok:
        # Mark all history rows for this asset as archived again
        db_exec(
            "UPDATE history SET archived=1 WHERE audio_id=? AND profile_id=?",
            (aid, body.profile_id),
        )
    return {"ok": ok, "asset_id": aid}

@app.post("/api/restore")
async def api_restore(body: RestoreBody):
    """Restore a single archived Roblox asset using the active session cookie."""
    session = get_session(body.profile_id)
    if not session.cookie:
        raise HTTPException(400, "No credentials — connect a Roblox account first")
    aid = body.asset_id.strip()
    if not aid:
        raise HTTPException(400, "asset_id is required")
    ok = await restore_asset(aid, cookie=session.cookie)
    sentinel_log(f"Manual restore {aid}: {'OK' if ok else 'FAILED'}", "ARCHIVE" if ok else "ERROR", "RESTORE")
    if ok:
        # Mark ALL history rows for this asset as restored (archived=0) so monitor won't re-archive
        db_exec(
            "UPDATE history SET archived=0 WHERE audio_id=? AND profile_id=?",
            (aid, body.profile_id),
        )
    return {"ok": ok, "asset_id": aid}


@app.post("/api/restore/bulk")
async def api_restore_bulk(body: RestoreBulkBody):
    """Restore multiple archived assets in sequence, returning per-asset results."""
    session = get_session(body.profile_id)
    if not session.cookie:
        raise HTTPException(400, "No credentials — connect a Roblox account first")
    if not body.asset_ids:
        raise HTTPException(400, "asset_ids list is empty")
    results: list[dict] = []
    for aid in body.asset_ids:
        aid = str(aid).strip()
        if not aid:
            continue
        ok = await restore_asset(aid, cookie=session.cookie)
        sentinel_log(f"Bulk restore {aid}: {'OK' if ok else 'FAILED'}", "ARCHIVE" if ok else "ERROR", "RESTORE")
        if ok:
            db_exec(
                "UPDATE history SET archived=0 WHERE audio_id=? AND profile_id=?",
                (aid, body.profile_id),
            )
        results.append({"asset_id": aid, "ok": ok})
        await asyncio.sleep(0.3)  # avoid rate-limiting
    succeeded = sum(1 for r in results if r["ok"])
    sentinel_log(f"Bulk restore complete: {succeeded}/{len(results)} succeeded", "ARCHIVE", "RESTORE")
    return {"results": results, "succeeded": succeeded, "total": len(results)}


# ── STATS ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
def api_stats(profile_id: str = ""):
    archived = db_exec("SELECT COUNT(*) AS c FROM history WHERE profile_id=? AND archived=1",
                       (profile_id,), fetch="val") or 0
    dms      = db_exec("SELECT COUNT(*) AS c FROM history WHERE profile_id=? AND dm_status='sent'",
                       (profile_id,), fetch="val") or 0
    groups   = db_exec("SELECT COUNT(*) AS c FROM groups WHERE profile_id=?",
                       (profile_id,), fetch="val") or 0
    wl = len(get_config(profile_id).get("whitelist_all", []))
    return {"archived": archived, "dms": dms, "groups": groups, "whitelisted": wl}

# ── CONFIG ────────────────────────────────────────────────────────────────────

@app.get("/api/config")
def api_get_config(profile_id: str = ""):
    return get_config(profile_id)

@app.post("/api/config")
def api_update_config(body: ConfigBody):
    data = body.model_dump(exclude_none=True)
    pid  = data.pop("profile_id", "")
    if "cookieSaveMode" in data and data["cookieSaveMode"] not in ("ask", "always", "never"):
        raise HTTPException(400, "cookieSaveMode must be 'ask', 'always', or 'never'")
    for k, v in data.items():
        set_cfg(pid, k, v)
    # If cookieSaveMode changed to "never", wipe saved credentials
    if data.get("cookieSaveMode") == "never" and PG_URL:
        conn2 = get_pg(); cur2 = conn2.cursor()
        try:
            cur2.execute("DELETE FROM saved_credentials WHERE profile_id=%s", (pid,))
            conn2.commit()
        except Exception as e:
            conn2.rollback()
            print(f"[SENTINEL] Error clearing credentials on mode=never: {e}")
        finally:
            cur2.close(); release_pg(conn2)
    return get_config(pid)

# ── ACCESS CONTROL ───────────────────────────────────────────────────────────

def _require_admin(profile_id: str, pin: str, cur) -> dict:
    cur.execute("SELECT id, name, is_admin FROM profiles WHERE id=%s AND pin_hash=%s",
                (profile_id, hash_pin(pin)))
    row = cur.fetchone()
    if not row or not row["is_admin"]:
        raise HTTPException(403, "Admin access required")
    return dict(row)

@app.post("/api/access/request")
def api_submit_access_request(body: AccessRequestBody, background_tasks: BackgroundTasks):
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    if not body.name.strip() or not body.reason.strip() or not body.email.strip():
        raise HTTPException(400, "Name, email and reason are required")
    req_id = str(uuid.uuid4())
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO access_requests (id, name, reason, email) VALUES (%s,%s,%s,%s)",
            (req_id, body.name.strip(), body.reason.strip(), body.email.strip())
        )
        approve_token = secrets.token_urlsafe(32)
        deny_token    = secrets.token_urlsafe(32)
        expires       = datetime.utcnow() + timedelta(hours=24)
        cur.execute("INSERT INTO review_tokens (token,request_id,action,expires_at) VALUES (%s,%s,%s,%s)",
                    (approve_token, req_id, "approve", expires))
        cur.execute("INSERT INTO review_tokens (token,request_id,action,expires_at) VALUES (%s,%s,%s,%s)",
                    (deny_token, req_id, "deny", expires))
        conn.commit()
        if ADMIN_EMAIL and BASE_URL:
            background_tasks.add_task(send_email, ADMIN_EMAIL, f"[Sentinel] Access Request from {body.name.strip()}",
                f"""<div style="font-family:sans-serif;max-width:520px;margin:0 auto;background:#0f0f0f;color:#fff;padding:32px;border-radius:12px;">
  <h2 style="color:#fff;letter-spacing:2px;margin-top:0;">SENTINEL</h2>
  <h3 style="color:#aaa;font-weight:400;">New Access Request</h3>
  <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
    <tr><td style="color:#888;padding:6px 0;width:80px;">Name</td><td style="color:#fff;">{body.name.strip()}</td></tr>
    <tr><td style="color:#888;padding:6px 0;">Email</td><td style="color:#fff;">{body.email.strip()}</td></tr>
    <tr><td style="color:#888;padding:6px 0;vertical-align:top;">Reason</td><td style="color:#fff;">{body.reason.strip()}</td></tr>
  </table>
  <p style="color:#888;font-size:12px;">Links expire in 24 hours.</p>
  <a href="{BASE_URL}/review.html?token={approve_token}" style="display:inline-block;background:#4ade80;color:#000;font-weight:700;padding:12px 28px;border-radius:6px;text-decoration:none;margin-right:12px;">Approve</a>
  <a href="{BASE_URL}/review.html?token={deny_token}" style="display:inline-block;background:#ef4444;color:#fff;font-weight:700;padding:12px 28px;border-radius:6px;text-decoration:none;">Deny</a>
</div>""")
        return {"id": req_id, "status": "pending"}
    except HTTPException:
        conn.rollback(); raise
    except Exception as e:
        conn.rollback(); raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

@app.get("/api/review")
def api_review_get(token: str):
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM review_tokens WHERE token=%s", (token,))
        tok = cur.fetchone()
        if not tok: raise HTTPException(404, "Invalid token")
        if tok["used"]: return {"status":"already_used","action":tok["action"]}
        if datetime.utcnow() > tok["expires_at"].replace(tzinfo=None):
            return {"status":"expired","action":tok["action"]}
        cur.execute("SELECT * FROM access_requests WHERE id=%s", (tok["request_id"],))
        req = cur.fetchone()
        if not req: raise HTTPException(404, "Request not found")
        if req["status"] != "pending":
            return {"status":"already_actioned","action":req["status"],"name":req["name"]}
        return {"status":"pending","action":tok["action"],"name":req["name"],
                "email":req["email"],"reason":req["reason"],"expires_at":str(tok["expires_at"])}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback(); raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

@app.post("/api/review")
def api_review_post(token: str, background_tasks: BackgroundTasks):
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM review_tokens WHERE token=%s", (token,))
        tok = cur.fetchone()
        if not tok: raise HTTPException(404, "Invalid token")
        if tok["used"]: return {"status":"already_used"}
        if datetime.utcnow() > tok["expires_at"].replace(tzinfo=None):
            return {"status":"expired"}
        cur.execute("SELECT * FROM access_requests WHERE id=%s", (tok["request_id"],))
        req = cur.fetchone()
        if not req: raise HTTPException(404, "Request not found")
        if req["status"] != "pending":
            return {"status":"already_actioned"}
        action = tok["action"]
        if action == "approve":
            code = "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
            cur.execute("INSERT INTO invite_codes (code,created_by) VALUES (%s,%s)", (code,"admin-email"))
            cur.execute("UPDATE access_requests SET status='approved',invite_code=%s WHERE id=%s", (code,tok["request_id"]))
            cur.execute("UPDATE review_tokens SET used=TRUE WHERE request_id=%s", (tok["request_id"],))
            conn.commit()
            background_tasks.add_task(send_email, req["email"], "[Sentinel] You've been approved!", f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;background:#0f0f0f;color:#fff;padding:32px;border-radius:12px;">
  <h2 style="color:#fff;letter-spacing:2px;margin-top:0;">SENTINEL</h2>
  <p style="color:#ccc;">Hey {req["name"]}, your Sentinel access request has been approved!</p>
  <p style="color:#888;font-size:13px;margin-bottom:8px;">Your one-time invite code:</p>
  <div style="background:#1a1a1a;border:1px solid #333;border-radius:8px;padding:16px;text-align:center;font-family:monospace;font-size:28px;letter-spacing:6px;color:#4ade80;margin-bottom:24px;">{code}</div>
  <p style="color:#888;font-size:12px;">Enter this when creating your profile. It can only be used once.</p>
</div>""")
            return {"status":"approved","name":req["name"],"email":req["email"],"invite_code":code}
        else:
            cur.execute("UPDATE access_requests SET status='denied' WHERE id=%s", (tok["request_id"],))
            cur.execute("UPDATE review_tokens SET used=TRUE WHERE request_id=%s", (tok["request_id"],))
            conn.commit()
            background_tasks.add_task(send_email, req["email"], "[Sentinel] Access request update", f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;background:#0f0f0f;color:#fff;padding:32px;border-radius:12px;">
  <h2 style="color:#fff;letter-spacing:2px;margin-top:0;">SENTINEL</h2>
  <p style="color:#ccc;">Hey {req["name"]}, your Sentinel access request has been denied.</p>
  <p style="color:#888;font-size:12px;">Contact the admin if you think this is a mistake.</p>
</div>""")
            return {"status":"denied","name":req["name"]}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback(); raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

@app.get("/api/admin/requests")
def api_admin_requests(profile_id: str, pin: str):
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        _require_admin(profile_id, pin, cur)
        cur.execute("SELECT * FROM access_requests ORDER BY created_at DESC")
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except HTTPException: raise
    except Exception as e: conn.rollback(); raise HTTPException(500, str(e))
    finally: cur.close(); release_pg(conn)

@app.post("/api/admin/generate-invite")
def api_generate_invite(body: GenerateInviteBody):
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        _require_admin(body.admin_id, body.admin_pin, cur)
        code = "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        cur.execute("INSERT INTO invite_codes (code,created_by) VALUES (%s,%s)", (code, body.admin_id))
        conn.commit()
        return {"code": code}
    except HTTPException: conn.rollback(); raise
    except Exception as e: conn.rollback(); raise HTTPException(500, str(e))
    finally: cur.close(); release_pg(conn)

@app.post("/api/admin/set-admin")
def api_set_admin(body: SetAdminBody):
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        _require_admin(body.admin_id, body.admin_pin, cur)
        cur.execute("UPDATE profiles SET is_admin=%s WHERE id=%s", (body.is_admin, body.target_id))
        conn.commit()
        return {"updated": True}
    except HTTPException: conn.rollback(); raise
    except Exception as e: conn.rollback(); raise HTTPException(500, str(e))
    finally: cur.close(); release_pg(conn)

class ApproveRequestBody(BaseModel):
    admin_id:    str
    admin_pin:   str
    request_id:  str
    invite_code: str

class DenyRequestBody(BaseModel):
    admin_id:   str
    admin_pin:  str
    request_id: str

@app.post("/api/admin/approve-request")
def api_approve_request(body: ApproveRequestBody, background_tasks: BackgroundTasks):
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        _require_admin(body.admin_id, body.admin_pin, cur)
        cur.execute("SELECT * FROM access_requests WHERE id=%s", (body.request_id,))
        req = cur.fetchone()
        if not req: raise HTTPException(404, "Request not found")
        cur.execute("UPDATE access_requests SET status='approved', invite_code=%s WHERE id=%s",
                    (body.invite_code, body.request_id))
        conn.commit()
        background_tasks.add_task(send_email, req["email"], "[Sentinel] You've been approved!", f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;background:#0f0f0f;color:#fff;padding:32px;border-radius:12px;">
  <h2 style="color:#fff;letter-spacing:2px;margin-top:0;">SENTINEL</h2>
  <p style="color:#ccc;">Hey {req["name"]}, your access has been approved!</p>
  <p style="color:#888;font-size:13px;margin-bottom:8px;">Your one-time invite code:</p>
  <div style="background:#1a1a1a;border:1px solid #333;border-radius:8px;padding:16px;text-align:center;font-family:monospace;font-size:28px;letter-spacing:6px;color:#4ade80;margin-bottom:24px;">{body.invite_code}</div>
  <p style="color:#888;font-size:12px;">Enter this when creating your profile. It can only be used once.</p>
</div>""")
        return {"ok": True}
    except HTTPException: conn.rollback(); raise
    except Exception as e: conn.rollback(); raise HTTPException(500, str(e))
    finally: cur.close(); release_pg(conn)

@app.post("/api/admin/deny-request")
def api_deny_request(body: DenyRequestBody, background_tasks: BackgroundTasks):
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    conn = get_pg(); cur = conn.cursor()
    try:
        _require_admin(body.admin_id, body.admin_pin, cur)
        cur.execute("SELECT * FROM access_requests WHERE id=%s", (body.request_id,))
        req = cur.fetchone()
        if not req: raise HTTPException(404, "Request not found")
        cur.execute("UPDATE access_requests SET status='denied' WHERE id=%s", (body.request_id,))
        conn.commit()
        background_tasks.add_task(send_email, req["email"], "[Sentinel] Access request update", f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;background:#0f0f0f;color:#fff;padding:32px;border-radius:12px;">
  <h2 style="color:#fff;letter-spacing:2px;margin-top:0;">SENTINEL</h2>
  <p style="color:#ccc;">Hey {req["name"]}, your request has been denied.</p>
  <p style="color:#888;font-size:12px;">Contact the admin if you think this is a mistake.</p>
</div>""")
        return {"ok": True}
    except HTTPException: conn.rollback(); raise
    except Exception as e: conn.rollback(); raise HTTPException(500, str(e))
    finally: cur.close(); release_pg(conn)

@app.get("/api/admin/make-me-admin")
def make_me_admin():
    if not PG_URL: raise HTTPException(503, "Postgres not configured")
    target_id = "f5087f66-a860-49dd-8d38-46e6b68ac99d"
    conn = get_pg(); cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;")
        conn.commit()
        cur.execute("SELECT name, is_admin FROM profiles WHERE id=%s", (target_id,))
        row = cur.fetchone()
        if not row: raise HTTPException(404, "Profile not found")
        if row["is_admin"]: return {"ok": True, "message": "Already admin!"}
        cur.execute("UPDATE profiles SET is_admin=TRUE WHERE id=%s", (target_id,))
        conn.commit()
        return {"ok": True, "message": f"✓ '{row['name']}' is now admin!"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); release_pg(conn)

@app.get("/review.html", response_class=HTMLResponse)
def serve_review():
    p = BASE_DIR / "static" / "review.html"
    if p.exists(): return HTMLResponse(p.read_text(), 200)
    raise HTTPException(404, "review.html not found in static/")

# ── MISC ──────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    global _MASTER_KEY
    asyncio.create_task(memory_watchdog())
    asyncio.create_task(sanity_check_loop())   # cookie validity checks every 5 min + on boot

    # Print / generate master key
    if not _MASTER_KEY:
        _MASTER_KEY = secrets.token_urlsafe(32)
        sentinel_log(
            f"SENTINEL_MASTER_KEY not set — generated for this session: {_MASTER_KEY}  "
            "Set this as an env var on Render to make it permanent.",
            "WARN", "VAULT"
        )
    else:
        sentinel_log("Vault master key loaded from environment", "INFO", "VAULT")

    asyncio.create_task(vault_auto_restore())   # restore from URL if DB is empty
    sentinel_log("SENTINEL backend started", "INFO", "SYSTEM")
    if PG_URL:
        _sc = None
        try:
            _sc = get_pg(); _scur = _sc.cursor()
            for sql in [
                "ALTER TABLE profiles ADD COLUMN IF NOT EXISTS avatar_url TEXT DEFAULT ''",
                "ALTER TABLE profiles ADD COLUMN IF NOT EXISTS pin_length INTEGER DEFAULT 4",
                "ALTER TABLE profiles ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE",
                # Multi-account credential migration — drop old single-row PK, add roblox_user_id col
                "ALTER TABLE saved_credentials ADD COLUMN IF NOT EXISTS roblox_user_id TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE saved_credentials DROP CONSTRAINT IF EXISTS saved_credentials_pkey",
                # Re-add composite PK now that roblox_user_id exists
                # (IF NOT EXISTS not available for ADD CONSTRAINT, handled via unique index instead)
                "CREATE UNIQUE INDEX IF NOT EXISTS saved_credentials_uid ON saved_credentials(profile_id, roblox_user_id)",
            ]:
                try: _scur.execute(sql + ";"); _sc.commit()
                except Exception as _e: _sc.rollback(); print(f"[SENTINEL] Migration: {_e}")
            _scur.execute("""CREATE TABLE IF NOT EXISTS access_requests (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, reason TEXT NOT NULL,
                email TEXT NOT NULL DEFAULT '', status TEXT DEFAULT 'pending',
                invite_code TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW());""")
            _scur.execute("""CREATE TABLE IF NOT EXISTS invite_codes (
                code TEXT PRIMARY KEY, created_by TEXT NOT NULL,
                used BOOLEAN DEFAULT FALSE, used_by TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW());""")
            _scur.execute("""CREATE TABLE IF NOT EXISTS review_tokens (
                token TEXT PRIMARY KEY, request_id TEXT NOT NULL,
                action TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL,
                used BOOLEAN DEFAULT FALSE);""")
            try: _scur.execute("ALTER TABLE access_requests ADD COLUMN IF NOT EXISTS email TEXT NOT NULL DEFAULT '';"); _sc.commit()
            except Exception as _e: _sc.rollback()
            _sc.commit()
            sentinel_log("Startup migrations OK", "INFO", "SYSTEM")
        except Exception as _e:
            sentinel_log(f"Startup migration error: {_e}", "ERROR", "SYSTEM")
            if _sc:
                try: _sc.rollback()
                except: pass
        finally:
            if _sc:
                try: _scur.close()
                except: pass
                release_pg(_sc)

@app.get("/api/health")
def health():
    # RENDER_GIT_COMMIT is set automatically by Render on every deploy and
    # only changes when a new build actually ships — a crash/restart of the
    # same build keeps the same value. Falls back to "dev" outside Render.
    version = os.environ.get("RENDER_GIT_COMMIT", "dev")
    return {"ok": True, "version": version}

@app.get("/api/sanity-check/status")
def api_sanity_status(profile_id: str = ""):
    """Return sanity check results for a profile's saved accounts.
    Returns per-account validity + global check state (running, last_run, phase2)."""
    # Strip internal-only fields (cookie, acc_info) before sending to frontend
    raw_results = _sanity_results.get(profile_id, {}) if profile_id else {}
    safe_results = {}
    for uid, data in raw_results.items():
        safe_results[uid] = {
            "valid":      data.get("valid", True),
            "checked_at": data.get("checked_at", 0),
            "username":   data.get("username", uid),
            "userId":     data.get("userId", uid),
        }

    # Determine if active account is expired
    active_expired = False
    if profile_id:
        session = _sessions.get(profile_id)
        if session and session.account_info:
            active_uid  = str(session.account_info.get("userId", ""))
            acct_result = raw_results.get(active_uid, {})
            if acct_result and not acct_result.get("valid", True):
                active_expired = True

    # Phase 2 state for this profile
    p2 = _sanity_phase2_state.get(profile_id, {"active": False, "countdown": 0})

    return {
        "running":          _sanity_running,
        "last_run":         _sanity_last_run,
        "accounts":         safe_results,
        "active_expired":   active_expired,
        "phase2_active":    p2.get("active", False),
        "phase2_countdown": p2.get("countdown", 0),
    }

@app.post("/api/sanity-check/run")
async def api_trigger_sanity_check():
    """Manually trigger a sanity check (non-blocking — fires and returns immediately)."""
    asyncio.create_task(run_sanity_check())
    return {"triggered": True}

class ValidateCookieBody(BaseModel):
    cookie: str

@app.post("/api/validate-cookie")
async def api_validate_cookie(body: ValidateCookieBody):
    """Validate a Roblox cookie and return account info. Used by extension during account scanning."""
    try:
        info = await validate_cookie(body.cookie)
        return info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Invalid cookie: {e}")

@app.get("/api/asset-types")
def api_asset_types():
    return ALL_ASSET_TYPES

# ── DEBUG ROUTES ──────────────────────────────────────────────────────────────

@app.get("/api/debug/logs")
def api_debug_logs(limit: int = 200, level: str = "", source: str = ""):
    logs = list(_LOG_BUFFER)
    if level:
        logs = [l for l in logs if l["level"] == level.upper()]
    if source:
        logs = [l for l in logs if l["source"] == source.upper()]
    return list(reversed(logs))[-limit:]

@app.get("/api/debug/memory")
def api_debug_memory():
    process = psutil.Process()
    mem     = process.memory_info()
    cpu     = psutil.cpu_percent(interval=None)
    vm      = psutil.virtual_memory()
    return {
        "rss_mb":       round(mem.rss / 1024 / 1024, 2),
        "vms_mb":       round(mem.vms / 1024 / 1024, 2),
        "pct":          _MEMORY_PCT,
        "limit_mb":     float(os.environ.get("MEMORY_LIMIT_MB", 400)),
        "total_mb":     round(vm.total / 1024 / 1024, 2),
        "available_mb": round(vm.available / 1024 / 1024, 2),
        "sys_pct":      vm.percent,
        "cpu_pct":      cpu,
        "degraded":     _DEGRADED,
        "sessions":     len(_sessions),
        "log_count":    len(_LOG_BUFFER),
    }

@app.post("/api/debug/gc")
def api_debug_gc():
    before = psutil.Process().memory_info().rss / 1024 / 1024
    collected = gc.collect()
    _trim_memory()
    after  = psutil.Process().memory_info().rss / 1024 / 1024
    freed  = round(before - after, 2)
    sentinel_log(f"Manual GC+trim: collected {collected} objects, freed ~{freed}MB", "MEMORY", "DEBUG")
    return {"collected": collected, "freed_mb": freed, "rss_after_mb": round(after, 2)}

@app.delete("/api/debug/logs")
def api_clear_logs():
    _LOG_BUFFER.clear()
    sentinel_log("Log buffer cleared", "INFO", "DEBUG")
    return {"cleared": True}

@app.get("/api/debug/sessions")
def api_debug_sessions():
    result = []
    for pid_key, sess in _sessions.items():
        result.append({
            "profile_id":    pid_key,
            "monitoring":    sess.monitoring,
            "has_cookie":    bool(sess.cookie),
            "known_groups":  len(sess.known_assets),
            "known_assets":  sum(len(v) for v in sess.known_assets.values()),
            "has_task":      sess.monitor_task is not None and not sess.monitor_task.done(),
        })
    return result

# ── SERVE FRONTEND ────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

MOBILE_AGENTS = ("android","iphone","ipad","ipod","mobile","opera mini","blackberry","windows phone")

@app.get("/", response_class=HTMLResponse)
def serve_root(request: Request):
    ua = request.headers.get("user-agent", "").lower()
    if any(t in ua for t in MOBILE_AGENTS):
        mp = STATIC_DIR / "sentinel_mobile.html"
        if mp.exists(): return HTMLResponse(mp.read_text(), 200)
    p = STATIC_DIR / "index.html"
    return HTMLResponse(p.read_text() if p.exists() else "<h1>Frontend missing</h1>", 200)

@app.get("/uploader", response_class=HTMLResponse)
def serve_uploader(request: Request):
    ua = request.headers.get("user-agent", "").lower()
    is_mobile = any(t in ua for t in MOBILE_AGENTS)
    fname = "sentinel_uploader.html" if is_mobile else "sentinel_uploader_desktop.html"
    p = STATIC_DIR / fname
    return HTMLResponse(p.read_text() if p.exists() else "<h1>Uploader missing</h1>", 200)



@app.post("/api/upload-asset")
async def upload_asset(
    file: UploadFile = File(...),
    profile_id: str = None,
):
    if not profile_id:
        raise HTTPException(400, "profile_id required")
    session = _sessions.get(profile_id)
    if not session or not session.cookie:
        raise HTTPException(403, "No active Roblox account for this profile")
    cookie = session.cookie
    csrf = await get_csrf(cookie)
    data = await file.read()
    fname = file.filename or "upload.mp3"
    ctype = file.content_type or "audio/mpeg"
    url = "https://data.roblox.com/Data/Upload.ashx"
    params = {"assetTypeId": 3, "name": fname, "description": "", "ispublic": "false", "allowComments": "false", "groupId": ""}
    async with httpx.AsyncClient(timeout=30) as c:
        hdrs = {"Content-Type": ctype, "User-Agent": "Roblox/WinInet", "X-CSRF-TOKEN": csrf}
        r = await c.post(url, content=data, params=params, headers=hdrs, cookies={".ROBLOSECURITY": cookie})
        if r.status_code == 403:
            nc = r.headers.get("x-csrf-token")
            if nc:
                hdrs["X-CSRF-TOKEN"] = nc
                r = await c.post(url, content=data, params=params, headers=hdrs, cookies={".ROBLOSECURITY": cookie})
        if r.status_code not in (200, 201):
            raise HTTPException(502, f"Roblox upload failed: {r.status_code}")
    asset_id = r.text.strip()
    if not asset_id.isdigit():
        raise HTTPException(502, f"Unexpected Roblox response: {asset_id[:80]}")
    return {"asset_id": asset_id, "filename": fname}

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
