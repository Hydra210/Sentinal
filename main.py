# SENTINEL - Roblox Asset Moderation Backend
# Start command: uvicorn main:app --host 0.0.0.0 --port $PORT

from __future__ import annotations
import asyncio, json, os, sqlite3, time, secrets, string, hashlib, base64
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import httpx

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PG = True
except ImportError:
    HAS_PG = False

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="SENTINEL API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ALL_ASSET_TYPES = [
    "Audio", "Image", "Decal", "Model", "Animation",
    "Plugin", "MeshPart", "Mesh", "Package", "Video",
]

# ── SQLITE ────────────────────────────────────────────────────────────────────

DB_PATH = os.environ.get("DB_PATH", "sentinel.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_sqlite():
    conn = get_db()
    conn.executescript(
        "CREATE TABLE IF NOT EXISTS groups "
        "(id TEXT PRIMARY KEY, name TEXT DEFAULT '', profile_id INTEGER DEFAULT 0, added_at REAL);"
        "CREATE TABLE IF NOT EXISTS history "
        "(id TEXT PRIMARY KEY, username TEXT DEFAULT '', display_name TEXT DEFAULT '',"
        "user_id TEXT DEFAULT '', audio_name TEXT DEFAULT '', audio_id TEXT DEFAULT '',"
        "asset_type TEXT DEFAULT 'Audio', group_id TEXT DEFAULT '', group_name TEXT DEFAULT '',"
        "profile_id INTEGER DEFAULT 0, time TEXT, dm_status TEXT DEFAULT 'n/a', archived INTEGER DEFAULT 1);"
        "CREATE TABLE IF NOT EXISTS local_config "
        "(profile_id INTEGER, key TEXT, value TEXT, PRIMARY KEY(profile_id, key));"
    )
    conn.commit()
    conn.close()

init_sqlite()

# ── POSTGRES ──────────────────────────────────────────────────────────────────

PG_URL = os.environ.get("DATABASE_URL")

def get_pg():
    if not HAS_PG or not PG_URL:
        raise HTTPException(503, "Postgres not configured. Add DATABASE_URL env var.")
    return psycopg2.connect(PG_URL, cursor_factory=RealDictCursor)

def init_pg():
    if not HAS_PG or not PG_URL:
        print("[SENTINEL] No DATABASE_URL set — profile persistence disabled")
        return
    try:
        conn = get_pg()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS profiles "
            "(id SERIAL PRIMARY KEY, name TEXT NOT NULL, pfp_url TEXT DEFAULT '',"
            " pin_hash TEXT DEFAULT '', created_at TIMESTAMP DEFAULT NOW())"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS profile_credentials "
            "(profile_id INTEGER PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,"
            " cookie_b64 TEXT DEFAULT '', updated_at TIMESTAMP DEFAULT NOW())"
        )
        conn.commit()
        cur.close()
        conn.close()
        print("[SENTINEL] Postgres initialized OK")
    except Exception as e:
        print(f"[SENTINEL] Postgres init error: {e}")

init_pg()

def hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.encode()).hexdigest()

def encode_cookie(cookie: str) -> str:
    return base64.b64encode(cookie.encode()).decode()

def decode_cookie(b64: str) -> str:
    return base64.b64decode(b64.encode()).decode()

# ── STATE ─────────────────────────────────────────────────────────────────────

class _State:
    cookie: Optional[str] = None
    mode: str = "cookie"
    monitoring: bool = False
    monitor_task: Optional[asyncio.Task] = None
    known_assets: dict = {}
    account_info: Optional[dict] = None
    active_profile_id: Optional[int] = None

state = _State()

# ── CONNECT CODES ─────────────────────────────────────────────────────────────

_connect_codes: dict = {}

def generate_connect_code() -> str:
    now = datetime.now()
    for c, exp in list(_connect_codes.items()):
        if now > exp:
            del _connect_codes[c]
    code = ''.join(secrets.choice(string.digits) for _ in range(4))
    _connect_codes[code] = now + timedelta(minutes=5)
    return code

def validate_connect_code(code: str) -> bool:
    expiry = _connect_codes.get(code)
    if not expiry:
        return False
    if datetime.now() > expiry:
        del _connect_codes[code]
        return False
    del _connect_codes[code]
    return True

# ── CONFIG ────────────────────────────────────────────────────────────────────

DEFAULT_CFG = {
    "pollingInterval": 60,
    "archiveDelay": 0,
    "notifyEnabled": True,
    "allowFastPoll": False,
    "archiveExisting": False,
    "assetTypes": ALL_ASSET_TYPES,
    "whitelist": [],
    "dmTemplate": (
        "Hi [USER_NAME], your asset [AUDIO_NAME] was removed from [GROUP_NAME] "
        "because we only accept uploads through approved channels.\n\n"
        "To share your audio: upload it to your own account, then go to the Creator Hub,"
        " find your asset, open Permissions, and add [ALT_ACCOUNT] as a collaborator.\n\n"
        "If you believe this was a mistake, please contact group staff."
    ),
    "altAccount": "",
}

def get_config(profile_id: int = 0) -> dict:
    conn = get_db()
    rows = conn.execute(
        "SELECT key, value FROM local_config WHERE profile_id=?", (profile_id,)
    ).fetchall()
    conn.close()
    cfg = dict(DEFAULT_CFG)
    for row in rows:
        try:
            cfg[row["key"]] = json.loads(row["value"])
        except Exception:
            cfg[row["key"]] = row["value"]
    return cfg

def set_cfg(key: str, value, profile_id: int = 0):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO local_config (profile_id, key, value) VALUES (?,?,?)",
        (profile_id, key, json.dumps(value))
    )
    conn.commit()
    conn.close()

# ── ROBLOX HELPERS ────────────────────────────────────────────────────────────

ROBLOX_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

async def rblx_get(url, *, cookie=None, params=None):
    headers = {"User-Agent": ROBLOX_UA}
    cookies = {}
    if cookie:
        cookies[".ROBLOSECURITY"] = cookie
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
        return await c.get(url, headers=headers, cookies=cookies, params=params)

async def get_csrf(cookie: str) -> str:
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(
            "https://auth.roblox.com/v2/logout",
            cookies={".ROBLOSECURITY": cookie},
            headers={"User-Agent": ROBLOX_UA, "Content-Type": "application/json"},
        )
        token = r.headers.get("x-csrf-token", "")
        print(f"[SENTINEL] CSRF status={r.status_code} token={'OK' if token else 'EMPTY'}")
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
        "userId": uid,
        "username": d["name"],
        "displayName": d["displayName"],
        "avatarUrl": avatar_url,
    }

async def get_group_name(group_id: str, cookie=None) -> str:
    try:
        r = await rblx_get(f"https://groups.roblox.com/v1/groups/{group_id}", cookie=cookie)
        if r.status_code == 200:
            return r.json().get("name", f"Group {group_id}")
    except Exception:
        pass
    return f"Group {group_id}"

async def fetch_group_assets_of_type(group_id: str, asset_type: str, cookie: str) -> list:
    assets = []
    cursor = None
    for _ in range(20):
        params = {"groupId": group_id, "assetType": asset_type, "sortOrder": "Desc", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        r = await rblx_get(
            "https://itemconfiguration.roblox.com/v1/creations/get-assets",
            cookie=cookie, params=params,
        )
        if r.status_code != 200:
            print(f"[SENTINEL] fetch {asset_type} status={r.status_code}")
            break
        d = r.json()
        raw = d.get("data", [])
        ids = [str(item.get("assetId", "")) for item in raw if item.get("assetId")]
        creator_map = {}
        if ids:
            try:
                det = await rblx_get(
                    "https://economy.roblox.com/v2/assets",
                    cookie=cookie,
                    params={"assetIds": ",".join(ids)},
                )
                if det.status_code == 200:
                    for d2 in det.json().get("data", []):
                        aid2 = str(d2.get("id", ""))
                        cr = d2.get("creator", {})
                        creator_map[aid2] = {
                            "creatorId": str(cr.get("targetId", "")),
                            "creatorName": cr.get("name", ""),
                        }
            except Exception as e:
                print(f"[SENTINEL] creator lookup error: {e}")
        for item in raw:
            aid = str(item.get("assetId", ""))
            info = creator_map.get(aid, {"creatorId": "", "creatorName": ""})
            assets.append({
                "id": aid,
                "name": item.get("name", "Unknown"),
                "assetType": asset_type,
                "creatorId": info["creatorId"],
                "creatorName": info["creatorName"],
            })
        cursor = d.get("nextPageCursor")
        if not cursor:
            break
    return assets

async def fetch_group_assets(group_id: str, cookie: str, asset_types: list) -> list:
    all_assets = []
    for atype in asset_types:
        try:
            batch = await fetch_group_assets_of_type(group_id, atype, cookie)
            all_assets.extend(batch)
        except Exception as e:
            print(f"[SENTINEL] fetch error {atype}: {e}")
    print(f"[SENTINEL] Group {group_id} total={len(all_assets)} across {len(asset_types)} types")
    return all_assets

async def archive_asset(asset_id: str, cookie: str) -> bool:
    csrf = await get_csrf(cookie)
    if not csrf:
        print("[SENTINEL] archive FAILED - empty CSRF")
        return False
    hdrs = {
        "X-CSRF-TOKEN": csrf,
        "Content-Type": "application/json",
        "User-Agent": ROBLOX_UA,
        "Referer": "https://www.roblox.com/",
        "Origin": "https://www.roblox.com",
    }
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            f"https://itemconfiguration.roblox.com/v1/assets/{asset_id}/archive",
            headers=hdrs, cookies={".ROBLOSECURITY": cookie}, json={},
        )
        print(f"[SENTINEL] archive primary status={r.status_code} body={r.text[:300]}")
        if r.status_code in (200, 204):
            return True
        new_csrf = r.headers.get("x-csrf-token", "")
        if r.status_code == 403 and new_csrf:
            hdrs["X-CSRF-TOKEN"] = new_csrf
            r2 = await c.post(
                f"https://itemconfiguration.roblox.com/v1/assets/{asset_id}/archive",
                headers=hdrs, cookies={".ROBLOSECURITY": cookie}, json={},
            )
            print(f"[SENTINEL] archive retry status={r2.status_code} body={r2.text[:300]}")
            if r2.status_code in (200, 204):
                return True
        r3 = await c.post(
            f"https://develop.roblox.com/v1/assets/{asset_id}/archive",
            headers=hdrs, cookies={".ROBLOSECURITY": cookie}, json={},
        )
        print(f"[SENTINEL] archive fallback status={r3.status_code} body={r3.text[:300]}")
        return r3.status_code in (200, 204)

async def restore_asset(asset_id: str, cookie: str) -> bool:
    csrf = await get_csrf(cookie)
    if not csrf:
        return False
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            f"https://itemconfiguration.roblox.com/v1/assets/{asset_id}/restore",
            headers={"X-CSRF-TOKEN": csrf, "Content-Type": "application/json", "User-Agent": ROBLOX_UA},
            cookies={".ROBLOSECURITY": cookie}, json={},
        )
        return r.status_code in (200, 204)

async def send_dm(user_id: str, subject: str, body: str, cookie: str) -> bool:
    csrf = await get_csrf(cookie)
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            "https://privatemessages.roblox.com/v1/messages",
            headers={"X-CSRF-TOKEN": csrf, "Content-Type": "application/json", "User-Agent": ROBLOX_UA},
            cookies={".ROBLOSECURITY": cookie},
            json={"userId": int(user_id), "subject": subject, "body": body},
        )
        return r.status_code in (200, 204)

# ── MONITORING LOOP ───────────────────────────────────────────────────────────

async def monitor_loop():
    print("[SENTINEL] Monitor loop started")
    while state.monitoring:
        pid = state.active_profile_id or 0
        cfg = get_config(pid)
        poll_sec = max(1, int(cfg.get("pollingInterval", 60)))
        delay_sec = int(cfg.get("archiveDelay", 0))
        notify = bool(cfg.get("notifyEnabled", True))
        archive_existing = bool(cfg.get("archiveExisting", False))
        asset_types = cfg.get("assetTypes", ALL_ASSET_TYPES)
        whitelist = {str(u).strip().lower() for u in cfg.get("whitelist", [])}
        dm_tmpl = str(cfg.get("dmTemplate", DEFAULT_CFG["dmTemplate"]))
        alt = str(cfg.get("altAccount", ""))

        conn = get_db()
        groups = conn.execute(
            "SELECT id, name FROM groups WHERE profile_id=?", (pid,)
        ).fetchall()
        conn.close()

        for grp in groups:
            gid, gname = grp["id"], grp["name"]
            try:
                assets = await fetch_group_assets(gid, state.cookie, asset_types)
                current = {a["id"]: a for a in assets}
                current_ids = set(current)

                if gid not in state.known_assets:
                    if archive_existing:
                        state.known_assets[gid] = set()
                        print(f"[SENTINEL] archiveExisting ON — treating all {len(current_ids)} as new")
                    else:
                        state.known_assets[gid] = current_ids
                        print(f"[SENTINEL] Group {gid} baseline: {len(current_ids)} assets")
                        continue

                new_ids = current_ids - state.known_assets[gid]
                state.known_assets[gid] = current_ids

                for aid in new_ids:
                    a = current.get(aid, {})
                    creator_id = a.get("creatorId", "")
                    creator_name = a.get("creatorName", "Unknown")
                    atype = a.get("assetType", "Asset")

                    if creator_id.lower() in whitelist or creator_name.lower() in whitelist:
                        print(f"[SENTINEL] Whitelist skip: {creator_name}")
                        continue

                    print(f"[SENTINEL] New {atype} {aid} ({a.get('name')}) by {creator_name}")

                    if delay_sec > 0:
                        await asyncio.sleep(delay_sec)
                        if not state.monitoring:
                            break

                    ok = await archive_asset(aid, cookie=state.cookie)

                    dm_status = "n/a"
                    if notify and state.cookie and creator_id:
                        try:
                            msg = (dm_tmpl
                                .replace("[USER_NAME]", creator_name)
                                .replace("[AUDIO_NAME]", a.get("name", ""))
                                .replace("[ALT_ACCOUNT]", alt)
                                .replace("[GROUP_NAME]", gname))
                            sent = await send_dm(creator_id, "Asset Policy Notice", msg, state.cookie)
                            dm_status = "sent" if sent else "failed"
                        except Exception as e:
                            print(f"[SENTINEL] DM error: {e}")
                            dm_status = "failed"

                    conn = get_db()
                    conn.execute(
                        "INSERT OR IGNORE INTO history"
                        " (id, username, display_name, user_id, audio_name, audio_id,"
                        " asset_type, group_id, group_name, profile_id, time, dm_status, archived)"
                        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)",
                        (
                            f"{aid}_{int(time.time())}",
                            creator_name, creator_name, creator_id,
                            a.get("name", "Unknown"), aid, atype,
                            gid, gname, pid,
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

    print("[SENTINEL] Monitor loop stopped")

# ── MODELS ────────────────────────────────────────────────────────────────────

class ConnectCodeBody(BaseModel):
    code: str
    cookie: str

class GroupBody(BaseModel):
    id: str
    name: str = ""

class ConfigBody(BaseModel):
    pollingInterval: Optional[int] = None
    archiveDelay: Optional[int] = None
    notifyEnabled: Optional[bool] = None
    allowFastPoll: Optional[bool] = None
    archiveExisting: Optional[bool] = None
    assetTypes: Optional[List[str]] = None
    whitelist: Optional[List[str]] = None
    dmTemplate: Optional[str] = None
    altAccount: Optional[str] = None

class ProfileCreateBody(BaseModel):
    name: str
    pfp_url: str = ""
    pin: str = ""

class ProfileAuthBody(BaseModel):
    profile_id: int
    pin: str = ""

class SaveCredBody(BaseModel):
    profile_id: int

# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"ok": True, "monitoring": state.monitoring}

@app.get("/api/status")
def api_status():
    return {
        "monitoring": state.monitoring,
        "mode": state.mode,
        "account": state.account_info,
        "hasCredential": bool(state.cookie),
        "activeProfileId": state.active_profile_id,
        "allAssetTypes": ALL_ASSET_TYPES,
    }

@app.get("/api/profiles")
def api_list_profiles():
    try:
        conn = get_pg()
        cur = conn.cursor()
        cur.execute("SELECT id, name, pfp_url, (pin_hash != '') as has_pin FROM profiles ORDER BY id")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        print(f"[SENTINEL] profiles list error: {e}")
        return []

@app.post("/api/profiles")
def api_create_profile(body: ProfileCreateBody):
    try:
        conn = get_pg()
        cur = conn.cursor()
        pin_hash = hash_pin(body.pin) if body.pin else ""
        cur.execute(
            "INSERT INTO profiles (name, pfp_url, pin_hash) VALUES (%s, %s, %s) RETURNING id, name, pfp_url",
            (body.name.strip(), body.pfp_url.strip(), pin_hash)
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.post("/api/profiles/auth")
def api_auth_profile(body: ProfileAuthBody):
    try:
        conn = get_pg()
        cur = conn.cursor()
        cur.execute("SELECT id, name, pfp_url, pin_hash FROM profiles WHERE id=%s", (body.profile_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(404, "Profile not found")
        if row["pin_hash"] and hash_pin(body.pin) != row["pin_hash"]:
            raise HTTPException(401, "Incorrect PIN")
        # Load saved credential if any
        try:
            conn2 = get_pg()
            cur2 = conn2.cursor()
            cur2.execute("SELECT cookie_b64 FROM profile_credentials WHERE profile_id=%s", (body.profile_id,))
            cred = cur2.fetchone()
            cur2.close()
            conn2.close()
            if cred and cred["cookie_b64"]:
                state.cookie = decode_cookie(cred["cookie_b64"])
                state.mode = "cookie"
                print(f"[SENTINEL] Loaded saved cred for profile {body.profile_id}")
        except Exception:
            pass
        state.active_profile_id = body.profile_id
        return {"ok": True, "id": row["id"], "name": row["name"], "pfp_url": row["pfp_url"]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.delete("/api/profiles/{profile_id}")
def api_delete_profile(profile_id: int):
    try:
        conn = get_pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM profiles WHERE id=%s", (profile_id,))
        conn.commit()
        cur.close()
        conn.close()
        return {"deleted": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.post("/api/profiles/save-credential")
def api_save_credential(body: SaveCredBody):
    if not state.cookie:
        raise HTTPException(400, "No active cookie to save")
    try:
        conn = get_pg()
        cur = conn.cursor()
        encoded = encode_cookie(state.cookie)
        cur.execute(
            "INSERT INTO profile_credentials (profile_id, cookie_b64, updated_at)"
            " VALUES (%s, %s, NOW())"
            " ON CONFLICT (profile_id) DO UPDATE SET cookie_b64=%s, updated_at=NOW()",
            (body.profile_id, encoded, encoded)
        )
        conn.commit()
        cur.close()
        conn.close()
        return {"saved": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.delete("/api/profiles/{profile_id}/credential")
def api_delete_credential(profile_id: int):
    try:
        conn = get_pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM profile_credentials WHERE profile_id=%s", (profile_id,))
        conn.commit()
        cur.close()
        conn.close()
        return {"deleted": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

@app.post("/api/connect-code/generate")
def api_generate_code():
    return {"code": generate_connect_code(), "expiresIn": 300}

@app.post("/api/connect-code/redeem")
async def api_redeem_code(body: ConnectCodeBody):
    if not validate_connect_code(body.code):
        raise HTTPException(400, "Invalid or expired code")
    info = await validate_cookie(body.cookie)
    state.cookie = body.cookie
    state.mode = "cookie"
    state.account_info = info
    return info

@app.post("/api/monitoring/start")
async def api_start():
    if not state.cookie:
        raise HTTPException(400, "No credentials — complete setup first.")
    if state.monitoring:
        return {"status": "already_running"}
    state.monitoring = True
    state.monitor_task = asyncio.create_task(monitor_loop())
    return {"status": "started"}

@app.post("/api/monitoring/stop")
async def api_stop():
    state.monitoring = False
    if state.monitor_task:
        state.monitor_task.cancel()
        try:
            await state.monitor_task
        except (asyncio.CancelledError, Exception):
            pass
        state.monitor_task = None
    return {"status": "stopped"}

@app.get("/api/groups")
def api_list_groups():
    pid = state.active_profile_id or 0
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, added_at FROM groups WHERE profile_id=? ORDER BY added_at DESC", (pid,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/api/groups")
async def api_add_group(body: GroupBody):
    pid = state.active_profile_id or 0
    gid = body.id.strip()
    if not gid.isdigit():
        raise HTTPException(400, "Group ID must be numeric")
    name = body.name.strip() or await get_group_name(gid, cookie=state.cookie)
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO groups (id, name, profile_id, added_at) VALUES (?,?,?,?)",
        (gid, name, pid, time.time())
    )
    conn.commit()
    conn.close()
    return {"id": gid, "name": name}

@app.delete("/api/groups/{group_id}")
def api_remove_group(group_id: str):
    pid = state.active_profile_id or 0
    conn = get_db()
    conn.execute("DELETE FROM groups WHERE id=? AND profile_id=?", (group_id, pid))
    conn.commit()
    conn.close()
    state.known_assets.pop(group_id, None)
    return {"deleted": True}

@app.get("/api/history")
def api_history(limit: int = 200, search: str = ""):
    pid = state.active_profile_id or 0
    conn = get_db()
    if search:
        s = f"%{search}%"
        rows = conn.execute(
            "SELECT * FROM history WHERE profile_id=?"
            " AND (username LIKE ? OR audio_name LIKE ? OR audio_id LIKE ?)"
            " ORDER BY time DESC LIMIT ?",
            (pid, s, s, s, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM history WHERE profile_id=? ORDER BY time DESC LIMIT ?",
            (pid, limit),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/api/history/{entry_id}/restore")
async def api_restore(entry_id: str):
    conn = get_db()
    row = conn.execute("SELECT audio_id FROM history WHERE id=?", (entry_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Entry not found")
    ok = await restore_asset(row["audio_id"], cookie=state.cookie)
    if ok:
        conn = get_db()
        conn.execute("UPDATE history SET archived=0 WHERE id=?", (entry_id,))
        conn.commit()
        conn.close()
    return {"restored": ok}

@app.delete("/api/history")
def api_clear_history():
    pid = state.active_profile_id or 0
    conn = get_db()
    conn.execute("DELETE FROM history WHERE profile_id=?", (pid,))
    conn.commit()
    conn.close()
    return {"cleared": True}

@app.get("/api/stats")
def api_stats():
    pid = state.active_profile_id or 0
    conn = get_db()
    archived = conn.execute("SELECT COUNT(*) FROM history WHERE archived=1 AND profile_id=?", (pid,)).fetchone()[0]
    dms = conn.execute("SELECT COUNT(*) FROM history WHERE dm_status='sent' AND profile_id=?", (pid,)).fetchone()[0]
    groups = conn.execute("SELECT COUNT(*) FROM groups WHERE profile_id=?", (pid,)).fetchone()[0]
    conn.close()
    wl = len(get_config(pid).get("whitelist", []))
    return {"archived": archived, "dms": dms, "groups": groups, "whitelisted": wl}

@app.get("/api/config")
def api_get_config():
    return get_config(state.active_profile_id or 0)

@app.post("/api/config")
def api_update_config(body: ConfigBody):
    pid = state.active_profile_id or 0
    for k, v in body.model_dump(exclude_none=True).items():
        set_cfg(k, v, pid)
    return get_config(pid)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def serve_root():
    p = STATIC_DIR / "index.html"
    return HTMLResponse(p.read_text() if p.exists() else "<h1>Frontend missing</h1>", 200)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
