# SENTINEL - Roblox Audio Moderation Backend
# Start command: uvicorn main:app --host 0.0.0.0 --port $PORT

from __future__ import annotations
import asyncio, json, os, sqlite3, time, secrets, string, hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict

import httpx
import psycopg2
import psycopg2.extras
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
    """)
    conn.commit()
    conn.close()

init_db()

# ── POSTGRES (profiles + saved credentials) ───────────────────────────────────

PG_URL = os.environ.get("DATABASE_URL", "")

def get_pg():
    return psycopg2.connect(PG_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_pg():
    if not PG_URL:
        print("[SENTINEL] No DATABASE_URL set — Postgres features disabled")
        return
    try:
        conn = get_pg()
        cur = conn.cursor()
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
        cur.close()
        conn.close()
        print("[SENTINEL] Postgres initialized")
    except Exception as e:
        print(f"[SENTINEL] Postgres init error: {e}")

init_pg()

def hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.encode()).hexdigest()

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

# ── CONNECT CODES ─────────────────────────────────────────────────────────────

_connect_codes: dict = {}  # code -> {expiry, profile_id}

def generate_connect_code(profile_id: str) -> str:
    now = datetime.now()
    expired = [c for c, v in _connect_codes.items() if now > v["expiry"]]
    for c in expired:
        del _connect_codes[c]
    code = ''.join(secrets.choice(string.digits) for _ in range(4))
    _connect_codes[code] = {"expiry": now + timedelta(minutes=5), "profile_id": profile_id}
    return code

def validate_connect_code(code: str) -> Optional[str]:
    entry = _connect_codes.get(code)
    if not entry:
        return None
    if datetime.now() > entry["expiry"]:
        del _connect_codes[code]
        return None
    profile_id = entry["profile_id"]
    del _connect_codes[code]
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
    conn = get_db()
    rows = conn.execute(
        "SELECT key, value FROM config WHERE profile_id=?", (profile_id,)
    ).fetchall()
    conn.close()
    cfg = dict(DEFAULT_CFG)
    for row in rows:
        try:
            cfg[row["key"]] = json.loads(row["value"])
        except Exception:
            cfg[row["key"]] = row["value"]
    return cfg

def set_cfg(profile_id: str, key: str, value):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO config (profile_id, key, value) VALUES (?,?,?)",
        (profile_id, key, json.dumps(value))
    )
    conn.commit()
    conn.close()

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

async def get_csrf(cookie: str) -> str:
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post("https://auth.roblox.com/v2/logout",
                         cookies={".ROBLOSECURITY": cookie})
        return r.headers.get("x-csrf-token", "")

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
    for _ in range(5):
        params = {"assetType": asset_type, "sortOrder": "Desc", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        r = await rblx_get(
            f"https://develop.roblox.com/v1/groups/{group_id}/assets",
            cookie=cookie, params=params,
        )
        if r.status_code != 200:
            break
        d = r.json()
        for item in d.get("data", []):
            assets.append({
                "id":          str(item["id"]),
                "name":        item.get("name", "Unknown"),
                "creatorId":   str(item.get("creatorId", "")),
                "creatorName": item.get("creatorName", ""),
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
        if r.status_code in (200, 204):
            return True
        r2 = await c.post(
            f"https://www.roblox.com/item-configuration/v1/items/{asset_id}/archive",
            headers={"X-CSRF-TOKEN": csrf, "Content-Type": "application/json"},
            cookies={".ROBLOSECURITY": cookie},
            json={},
        )
        return r2.status_code in (200, 204)

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
    print(f"[SENTINEL] Monitor loop started for profile {profile_id}")

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

        conn   = get_db()
        groups = conn.execute(
            "SELECT id, name FROM groups WHERE profile_id=?", (profile_id,)
        ).fetchall()
        conn.close()

        for grp in groups:
            gid, gname = grp["id"], grp["name"]
            try:
                all_assets: list[dict] = []
                for asset_type in asset_filters:
                    type_assets = await fetch_group_assets(
                        gid, asset_type, cookie=session.cookie
                    )
                    all_assets.extend(type_assets)

                current    = {a["id"]: a for a in all_assets}
                current_ids = set(current)
                group_key  = f"{profile_id}:{gid}"

                if group_key not in session.known_assets:
                    session.known_assets[group_key] = current_ids
                    print(f"[SENTINEL] Profile {profile_id} Group {gid} baseline: {len(current_ids)} assets")
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
                    creator_name = a.get("creatorName", "Unknown")
                    asset_type   = a.get("assetType", "Unknown")

                    # Global whitelist check
                    if creator_id.lower() in whitelist_all or creator_name.lower() in whitelist_all:
                        print(f"[SENTINEL] Global whitelist skip: {creator_name}")
                        continue

                    # Per-type whitelist check
                    type_wl = {str(u).strip().lower() for u in cfg.get(f"whitelist_{asset_type}", [])}
                    if creator_id.lower() in type_wl or creator_name.lower() in type_wl:
                        print(f"[SENTINEL] Type whitelist skip: {creator_name} ({asset_type})")
                        continue

                    print(f"[SENTINEL] New {asset_type} {aid} ({a.get('name')}) by {creator_name}")

                    if delay_sec > 0:
                        await asyncio.sleep(delay_sec)
                        if not session.monitoring:
                            break

                    ok = await archive_asset(aid, cookie=session.cookie)

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
                        except Exception as e:
                            print(f"[SENTINEL] DM error: {e}")
                            dm_status = "failed"

                    conn = get_db()
                    conn.execute(
                        "INSERT OR IGNORE INTO history"
                        " (id, profile_id, username, display_name, user_id,"
                        "  audio_name, audio_id, asset_type, group_id, group_name,"
                        "  time, dm_status, archived)"
                        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)",
                        (
                            f"{aid}_{int(time.time())}",
                            profile_id,
                            creator_name, creator_name, creator_id,
                            a.get("name", "Unknown"), aid, asset_type,
                            gid, gname,
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            dm_status,
                        ),
                    )
                    conn.commit()
                    conn.close()
                    print(f"[SENTINEL] archived={ok} dm={dm_status}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[SENTINEL] Error in group {gid}: {e}")

        await asyncio.sleep(poll_sec)

    print(f"[SENTINEL] Monitor loop stopped for profile {profile_id}")

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
        cur.close(); conn.close()
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
        profile_id = secrets.token_hex(8)
        conn = get_pg()
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO profiles (id, name, pin_hash, avatar_url) VALUES (%s,%s,%s,%s)",
            (profile_id, body.name.strip(), hash_pin(body.pin), body.avatar_url)
        )
        conn.commit()
        cur.close(); conn.close()
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
            cur.close(); conn.close()
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

        cur.close(); conn.close()

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
            cur.close(); conn.close()
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

        cur.close(); conn.close()
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
            cur.close(); conn.close()
            raise HTTPException(401, "Invalid PIN")
        cur.execute("DELETE FROM profiles WHERE id=%s", (profile_id,))
        conn.commit()
        cur.close(); conn.close()
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
    profile_id = validate_connect_code(body.code)
    if not profile_id:
        raise HTTPException(400, "Invalid or expired code")
    info = await validate_cookie(body.cookie)
    session = get_session(profile_id)
    session.cookie       = body.cookie
    session.account_info = info

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
            cur.close(); conn.close()
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
            cur.close(); conn.close()
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
    return {"status": "started"}

@app.post("/api/monitoring/stop")
async def api_stop(body: MonitorBody):
    session = get_session(body.profile_id)
    session.monitoring = False
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
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, added_at FROM groups WHERE profile_id=? ORDER BY added_at DESC",
        (profile_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/api/groups")
async def api_add_group(body: GroupBody):
    gid = body.id.strip()
    if not gid.isdigit():
        raise HTTPException(400, "Group ID must be numeric")
    session = get_session(body.profile_id)
    name = body.name.strip() or await get_group_name(gid, cookie=session.cookie)
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO groups (id, profile_id, name, added_at) VALUES (?,?,?,?)",
        (gid, body.profile_id, name, time.time())
    )
    conn.commit()
    conn.close()
    return {"id": gid, "name": name, "added_at": time.time()}

@app.delete("/api/groups/{group_id}")
def api_remove_group(group_id: str, profile_id: str = ""):
    conn = get_db()
    conn.execute("DELETE FROM groups WHERE id=? AND profile_id=?", (group_id, profile_id))
    conn.commit()
    conn.close()
    session = get_session(profile_id)
    session.known_assets.pop(f"{profile_id}:{group_id}", None)
    return {"deleted": True}

# ── HISTORY ───────────────────────────────────────────────────────────────────

@app.get("/api/history")
def api_history(profile_id: str = "", limit: int = 200, search: str = ""):
    conn = get_db()
    if search:
        s = f"%{search}%"
        rows = conn.execute(
            "SELECT * FROM history WHERE profile_id=?"
            " AND (username LIKE ? OR audio_name LIKE ? OR audio_id LIKE ?)"
            " ORDER BY time DESC LIMIT ?",
            (profile_id, s, s, s, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM history WHERE profile_id=? ORDER BY time DESC LIMIT ?",
            (profile_id, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.delete("/api/history")
def api_clear_history(profile_id: str = ""):
    conn = get_db()
    conn.execute("DELETE FROM history WHERE profile_id=?", (profile_id,))
    conn.commit()
    conn.close()
    return {"cleared": True}

# ── STATS ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
def api_stats(profile_id: str = ""):
    conn = get_db()
    archived = conn.execute(
        "SELECT COUNT(*) FROM history WHERE profile_id=? AND archived=1", (profile_id,)
    ).fetchone()[0]
    dms = conn.execute(
        "SELECT COUNT(*) FROM history WHERE profile_id=? AND dm_status='sent'", (profile_id,)
    ).fetchone()[0]
    groups = conn.execute(
        "SELECT COUNT(*) FROM groups WHERE profile_id=?", (profile_id,)
    ).fetchone()[0]
    conn.close()
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
            cur.close(); conn.close()
        except Exception as e:
            print(f"[SENTINEL] Error clearing credentials on toggle off: {e}")
    return get_config(pid)

# ── MISC ──────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"ok": True}

@app.get("/api/asset-types")
def api_asset_types():
    return ALL_ASSET_TYPES

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
