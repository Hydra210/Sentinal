# SENTINEL - Roblox Audio Moderation Backend
# Start command: uvicorn main:app --host 0.0.0.0 --port $PORT

from __future__ import annotations
import asyncio, json, os, sqlite3, time, secrets, string, hashlib, uuid, gc, collections, ctypes
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict

import httpx
import psutil
import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool
from fastapi import FastAPI, HTTPException
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
    "Audio", "Image", "Decal", "Video", "Mesh",
    "Plugin", "Animation", "Model", "Package"
]

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
        maxconn=5,
        dsn=PG_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    print("[SENTINEL] Postgres connection pool initialized (min=1, max=5)")

def get_pg():
    if _pg_pool is None:
        raise RuntimeError("Postgres pool not initialized")
    return _pg_pool.getconn()

def release_pg(conn):
    if _pg_pool:
        _pg_pool.putconn(conn)

def init_pg():
    if not PG_URL:
        print("[SENTINEL] No DATABASE_URL set — Postgres features disabled")
        return
    try:
        # Step 1 — create tables
        conn = get_pg()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                pin_hash TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS saved_credentials (
                profile_id TEXT PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,
                cookie_encrypted TEXT,
                account_info JSONB,
                saved_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[SENTINEL] Postgres table creation error: {e}")

    try:
        # Step 2 — fresh connection, force correct schema
        conn = get_pg()
        cur = conn.cursor()

        # Check if id column is wrong type and drop if so
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_name='profiles' AND column_name='id'
        """)
        row = cur.fetchone()
        if row and row['data_type'] != 'text':
            print("[SENTINEL] Dropping profiles table — wrong id type, recreating...")
            cur.execute("DROP TABLE IF EXISTS saved_credentials CASCADE;")
            cur.execute("DROP TABLE IF EXISTS profiles CASCADE;")
            conn.commit()

        # Recreate with correct schema
        cur.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                pin_hash TEXT NOT NULL,
                avatar_url TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS saved_credentials (
                profile_id TEXT PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,
                cookie_encrypted TEXT,
                account_info JSONB,
                saved_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()

        # Add avatar_url column if missing
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS avatar_url TEXT DEFAULT '';")
        conn.commit()

        # ── App data tables (groups, history, config, connect_codes) ──────────
        cur.execute("""
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
        conn.commit()

        cur.close()
        conn.close()
        print("[SENTINEL] Postgres initialized")
    except Exception as e:
        print(f"[SENTINEL] Postgres migration error: {e}")

init_pg()
init_pg_pool()

def hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.encode()).hexdigest()

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
        self.profile_id:   Optional[str]  = None
        self.cookie:       Optional[str]  = None
        self.monitoring:   bool           = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.known_assets: dict           = {}
        self.account_info: Optional[dict] = None

_sessions: Dict[str, ProfileSession] = {}

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
    "notifyEnabled":       True,
    "saveCookies":         False,
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
    "dmTemplate": (
        "Hi [USER_NAME], your asset [AUDIO_NAME] was removed from [GROUP_NAME] "
        "because we only accept uploads through approved channels.\n\n"
        "If you believe this was a mistake, please contact group staff."
    ),
    "altAccount": "",
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

async def get_csrf(cookie: str) -> str:
    now = time.time()
    if cookie in _csrf_cache and now - _csrf_cache[cookie][1] < 60:
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

async def fetch_group_assets(group_id: str, asset_type: str, *, cookie=None) -> list[dict]:
    assets: list[dict] = []
    if not cookie:
        return assets
    cursor = None
    for _ in range(10):
        params = {
            "assetType":  asset_type,
            "isArchived": "false",
            "groupId":    group_id,
            "pageSize":   100,
        }
        if cursor:
            params["cursor"] = cursor
        r = await rblx_get(
            "https://itemconfiguration.roblox.com/v1/creations/get-assets",
            cookie=cookie, params=params,
        )
        if r.status_code != 200:
            print(f"[SENTINEL] fetch_group_assets {asset_type} group {group_id}: HTTP {r.status_code} — {r.text[:200]}")
            break
        d = r.json()
        for item in d.get("data", []):
            creator_id   = str(item.get("creatorTargetId", ""))
            creator_name = item.get("creatorName", "") or ""
            assets.append({
                "id":          str(item.get("assetId", item.get("id", ""))),
                "name":        item.get("name", "Unknown"),
                "creatorId":   creator_id,
                "creatorName": creator_name,
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
    sentinel_log(f"Monitor loop started for profile {profile_id}", "INFO", "MONITOR")

    while session.monitoring:
        cfg              = get_config(profile_id)
        poll_sec         = int(cfg.get("pollingInterval", 60))
        delay_sec        = int(cfg.get("archiveDelay", 0))
        notify           = bool(cfg.get("notifyEnabled", True))
        archive_existing = bool(cfg.get("archiveExisting", False))
        asset_filters    = cfg.get("assetTypeFilters", ALL_ASSET_TYPES)
        whitelist_all    = {str(u).strip().lower() for u in cfg.get("whitelist_all", [])}
        dm_tmpl          = str(cfg.get("dmTemplate", DEFAULT_CFG["dmTemplate"]))
        alt              = str(cfg.get("altAccount", ""))

        groups = db_exec(
            "SELECT id, name FROM groups WHERE profile_id=?", (profile_id,), fetch="all"
        ) or []

        for grp in groups:
            gid, gname = grp["id"], grp["name"]
            try:
                all_assets: list[dict] = []
                active_filters = asset_filters[:3] if _DEGRADED else asset_filters
                if _DEGRADED:
                    sentinel_log(f"Degraded mode — limiting asset scan to {active_filters}", "MEMORY", "MONITOR")
                for asset_type in active_filters:
                    sentinel_log(f"Scanning group {gid} ({gname}) for {asset_type}", "DEBUG", "NETWORK")
                    type_assets = await fetch_group_assets(
                        gid, asset_type, cookie=session.cookie
                    )
                    sentinel_log(f"Found {len(type_assets)} {asset_type} assets in group {gid}", "DEBUG", "NETWORK")
                    all_assets.extend(type_assets)

                current    = {a["id"]: a for a in all_assets}
                current_ids = set(current)
                group_key  = f"{profile_id}:{gid}"

                if group_key not in session.known_assets:
                    session.known_assets[group_key] = current_ids
                    sentinel_log(f"Group {gid} ({gname}) baseline: {len(current_ids)} assets", "INFO", "MONITOR")
                    if archive_existing:
                        new_ids = current_ids
                    else:
                        continue
                else:
                    new_ids = current_ids - session.known_assets[group_key]
                    session.known_assets[group_key] = current_ids

                for aid in new_ids:
                    a            = current.get(aid, {})
                    creator_id   = a.get("creatorId", "")
                    creator_name = a.get("creatorName", "") or ""
                    if not creator_name and creator_id:
                        creator_name = await get_username(creator_id, cookie=session.cookie)
                    asset_type   = a.get("assetType", "Unknown")

                    if creator_id.lower() in whitelist_all or creator_name.lower() in whitelist_all:
                        sentinel_log(f"Global whitelist skip: {creator_name} ({asset_type} {aid})", "INFO", "MONITOR")
                        continue

                    type_wl = {str(u).strip().lower() for u in cfg.get(f"whitelist_{asset_type}", [])}
                    if creator_id.lower() in type_wl or creator_name.lower() in type_wl:
                        sentinel_log(f"Type whitelist skip: {creator_name} ({asset_type})", "INFO", "MONITOR")
                        continue

                    sentinel_log(f"New {asset_type} detected: '{a.get('name')}' (ID {aid}) by {creator_name}", "ARCHIVE", "MONITOR")

                    if delay_sec > 0:
                        sentinel_log(f"Waiting {delay_sec}s before archiving {aid}", "DEBUG", "MONITOR")
                        await asyncio.sleep(delay_sec)
                        if not session.monitoring:
                            break

                    ok = await archive_asset(aid, cookie=session.cookie)
                    sentinel_log(f"Archive {aid}: {'OK' if ok else 'FAILED'}", "ARCHIVE" if ok else "ERROR", "MONITOR")

                    dm_status = "n/a"
                    if notify and session.cookie and creator_id:
                        try:
                            msg = (dm_tmpl
                                   .replace("[USER_NAME]",  creator_name)
                                   .replace("[AUDIO_NAME]", a.get("name", ""))
                                   .replace("[ALT_ACCOUNT]", alt)
                                   .replace("[GROUP_NAME]", gname))
                            sent = await send_dm(creator_id, "Asset Policy Notice", msg, session.cookie)
                            dm_status = "sent" if sent else "failed"
                            sentinel_log(f"DM to {creator_name} (UID {creator_id}): {'sent' if sent else 'failed'}", "DM", "MONITOR")
                        except Exception as e:
                            sentinel_log(f"DM error for {creator_id}: {e}", "ERROR", "MONITOR")
                            dm_status = "failed"

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
                        "dm_status":    dm_status,
                        "archived":     1,
                    })
                    sentinel_log(f"History written: {aid} archived={ok} dm={dm_status}", "DEBUG", "MONITOR")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                sentinel_log(f"Error in group {gid}: {e}", "ERROR", "MONITOR")

        # Trim after every scan cycle so archived asset memory is returned to OS immediately
        _trim_memory()

        sleep_sec = poll_sec * 3 if _DEGRADED else poll_sec
        if _DEGRADED:
            sentinel_log(f"Degraded mode: sleeping {sleep_sec}s instead of {poll_sec}s", "MEMORY", "MONITOR")
        await asyncio.sleep(sleep_sec)

    sentinel_log(f"Monitor loop stopped for profile {profile_id}", "INFO", "MONITOR")

# ── PYDANTIC MODELS ───────────────────────────────────────────────────────────

class ProfileCreate(BaseModel):
    name:       str
    pin:        str
    avatar_url: str = ""

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
    profile_id:          str
    pollingInterval:     Optional[int]       = None
    allowFastPolling:    Optional[bool]      = None
    archiveDelay:        Optional[int]       = None
    archiveExisting:     Optional[bool]      = None
    notifyEnabled:       Optional[bool]      = None
    saveCookies:         Optional[bool]      = None
    assetTypeFilters:    Optional[List[str]] = None
    whitelist_Audio:     Optional[List[str]] = None
    whitelist_Image:     Optional[List[str]] = None
    whitelist_Decal:     Optional[List[str]] = None
    whitelist_Video:     Optional[List[str]] = None
    whitelist_Mesh:      Optional[List[str]] = None
    whitelist_Plugin:    Optional[List[str]] = None
    whitelist_Animation: Optional[List[str]] = None
    whitelist_Model:     Optional[List[str]] = None
    whitelist_Package:   Optional[List[str]] = None
    whitelist_all:       Optional[List[str]] = None
    dmTemplate:          Optional[str]       = None
    altAccount:          Optional[str]       = None

class MonitorBody(BaseModel):
    profile_id: str

# ── PROFILE ROUTES ────────────────────────────────────────────────────────────

@app.get("/api/profiles")
def api_list_profiles():
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    try:
        conn = get_pg()
        cur  = conn.cursor()
        cur.execute("SELECT id, name, avatar_url, created_at FROM profiles ORDER BY created_at")
        rows = cur.fetchall()
        cur.close(); release_pg(conn)
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/profiles")
def api_create_profile(body: ProfileCreate):
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    if not body.name.strip():
        raise HTTPException(400, "Name is required")
    if len(body.pin) < 4:
        raise HTTPException(400, "PIN must be at least 4 digits")
    try:
        profile_id = str(uuid.uuid4())
        conn = get_pg()
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO profiles (id, name, pin_hash, avatar_url) VALUES (%s,%s,%s,%s)",
            (profile_id, body.name.strip(), hash_pin(body.pin), body.avatar_url)
        )
        conn.commit()
        cur.close(); release_pg(conn)
        return {"id": profile_id, "name": body.name.strip(), "avatar_url": body.avatar_url}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/profiles/login")
def api_login_profile(body: ProfileLogin):
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    try:
        conn = get_pg()
        cur  = conn.cursor()
        cur.execute(
            "SELECT id, name, avatar_url FROM profiles WHERE id=%s AND pin_hash=%s",
            (body.profile_id, hash_pin(body.pin))
        )
        row = cur.fetchone()
        if not row:
            cur.close(); release_pg(conn)
            raise HTTPException(401, "Invalid PIN")

        saved_cookie  = None
        saved_account = None
        cur.execute(
            "SELECT cookie_encrypted, account_info FROM saved_credentials WHERE profile_id=%s",
            (body.profile_id,)
        )
        cred = cur.fetchone()
        if cred:
            saved_cookie  = cred["cookie_encrypted"]
            saved_account = cred["account_info"]

        cur.close(); release_pg(conn)

        if saved_cookie and saved_account:
            session = get_session(body.profile_id)
            session.cookie       = saved_cookie
            session.account_info = saved_account

        return {
            "id":            row["id"],
            "name":          row["name"],
            "avatar_url":    row["avatar_url"],
            "hasCredential": bool(saved_cookie),
            "account":       saved_account,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.put("/api/profiles")
def api_update_profile(body: ProfileUpdate):
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    try:
        conn = get_pg()
        cur  = conn.cursor()
        cur.execute(
            "SELECT id FROM profiles WHERE id=%s AND pin_hash=%s",
            (body.profile_id, hash_pin(body.pin))
        )
        if not cur.fetchone():
            cur.close(); release_pg(conn)
            raise HTTPException(401, "Invalid PIN")

        updates, params = [], []
        if body.new_pin:
            updates.append("pin_hash=%s"); params.append(hash_pin(body.new_pin))
        if body.name:
            updates.append("name=%s"); params.append(body.name.strip())
        if body.avatar_url is not None:
            updates.append("avatar_url=%s"); params.append(body.avatar_url)

        if updates:
            params.append(body.profile_id)
            cur.execute(f"UPDATE profiles SET {', '.join(updates)} WHERE id=%s", params)
            conn.commit()

        cur.close(); release_pg(conn)
        return {"updated": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.delete("/api/profiles/{profile_id}")
def api_delete_profile(profile_id: str, pin: str):
    if not PG_URL:
        raise HTTPException(503, "Postgres not configured")
    try:
        conn = get_pg()
        cur  = conn.cursor()
        cur.execute(
            "SELECT id FROM profiles WHERE id=%s AND pin_hash=%s",
            (profile_id, hash_pin(pin))
        )
        if not cur.fetchone():
            cur.close(); release_pg(conn)
            raise HTTPException(401, "Invalid PIN")
        cur.execute("DELETE FROM profiles WHERE id=%s", (profile_id,))
        conn.commit()
        cur.close(); release_pg(conn)
        if profile_id in _sessions:
            del _sessions[profile_id]
        return {"deleted": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

# ── CONNECT CODE ROUTES ───────────────────────────────────────────────────────

@app.post("/api/connect-code/generate")
def api_generate_code(body: GenerateCodeBody):
    code = generate_connect_code(body.profile_id)
    return {"code": code, "expiresIn": 300}

@app.post("/api/connect-code/redeem")
async def api_redeem_code(body: ConnectCodeBody):
    try:
        profile_id = validate_connect_code(body.code)
        if not profile_id:
            raise HTTPException(400, "Invalid or expired code — generate a fresh one from the dashboard")
        try:
            info = await validate_cookie(body.cookie)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"Could not verify Roblox session: {e}")
        session = get_session(profile_id)
        session.cookie       = body.cookie
        session.account_info = info

        # Auto-restart monitoring if it was active before the server restarted
        cfg_check = get_config(profile_id)
        if cfg_check.get("_monitoringActive") and not session.monitoring:
            session.monitoring   = True
            session.monitor_task = asyncio.create_task(monitor_loop(profile_id))
            print(f"[SENTINEL] Auto-restarted monitoring for profile {profile_id}")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

    cfg = get_config(profile_id)
    if cfg.get("saveCookies") and PG_URL:
        try:
            conn = get_pg()
            cur  = conn.cursor()
            cur.execute(
                """INSERT INTO saved_credentials (profile_id, cookie_encrypted, account_info)
                   VALUES (%s,%s,%s)
                   ON CONFLICT (profile_id) DO UPDATE
                   SET cookie_encrypted=%s, account_info=%s, saved_at=NOW()""",
                (profile_id, body.cookie, json.dumps(info), body.cookie, json.dumps(info))
            )
            conn.commit()
            cur.close(); release_pg(conn)
        except Exception as e:
            print(f"[SENTINEL] Failed to save credentials: {e}")

    return {**info, "profile_id": profile_id}

# ── STATUS ────────────────────────────────────────────────────────────────────

@app.get("/api/status")
def api_status(profile_id: str = ""):
    if not profile_id:
        return {"monitoring": False, "hasCredential": False, "account": None}
    session = get_session(profile_id)
    return {
        "monitoring":    session.monitoring,
        "account":       session.account_info,
        "hasCredential": bool(session.cookie),
    }

@app.post("/api/credentials/clear")
def api_clear_credentials(body: MonitorBody):
    if PG_URL:
        try:
            conn = get_pg()
            cur  = conn.cursor()
            cur.execute("DELETE FROM saved_credentials WHERE profile_id=%s", (body.profile_id,))
            conn.commit()
            cur.close(); release_pg(conn)
        except Exception as e:
            print(f"[SENTINEL] Failed to clear credentials: {e}")
    session = get_session(body.profile_id)
    session.cookie       = None
    session.account_info = None
    return {"cleared": True}

# ── MONITORING ────────────────────────────────────────────────────────────────

@app.post("/api/monitoring/start")
async def api_start(body: MonitorBody):
    session = get_session(body.profile_id)
    if not session.cookie:
        raise HTTPException(400, "No credentials. Connect extension first.")
    if session.monitoring:
        return {"status": "already_running"}
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
    for k, v in data.items():
        set_cfg(pid, k, v)
    # If saveCookies toggled off, wipe saved credentials
    if "saveCookies" in data and not data["saveCookies"] and PG_URL:
        try:
            conn = get_pg()
            cur  = conn.cursor()
            cur.execute("DELETE FROM saved_credentials WHERE profile_id=%s", (pid,))
            conn.commit()
            cur.close(); release_pg(conn)
        except Exception as e:
            print(f"[SENTINEL] Error clearing credentials on toggle off: {e}")
    return get_config(pid)

# ── MISC ──────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(memory_watchdog())
    sentinel_log("SENTINEL backend started", "INFO", "SYSTEM")

@app.get("/api/health")
def health():
    return {"ok": True}

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

@app.get("/", response_class=HTMLResponse)
def serve_root():
    p = STATIC_DIR / "index.html"
    return HTMLResponse(
        p.read_text() if p.exists() else "<h1>Frontend missing</h1>", 200
    )

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
