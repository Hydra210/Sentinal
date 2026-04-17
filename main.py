# SENTINEL - Roblox Audio Moderation Backend
# Start command: uvicorn main:app --host 0.0.0.0 --port $PORT

from __future__ import annotations
import asyncio, json, os, sqlite3, time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import httpx
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

# ── DATABASE ──────────────────────────────────────────────────────────────────

DB_PATH = os.environ.get("DB_PATH", "sentinel.db")

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    sql = (
        "CREATE TABLE IF NOT EXISTS groups ("
        "id TEXT PRIMARY KEY, name TEXT DEFAULT '', added_at REAL);"
        "CREATE TABLE IF NOT EXISTS history ("
        "id TEXT PRIMARY KEY, username TEXT DEFAULT '', display_name TEXT DEFAULT '',"
        "user_id TEXT DEFAULT '', audio_name TEXT DEFAULT '', audio_id TEXT DEFAULT '',"
        "group_id TEXT DEFAULT '', group_name TEXT DEFAULT '', time TEXT,"
        "dm_status TEXT DEFAULT 'n/a', archived INTEGER DEFAULT 1);"
        "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT);"
    )
    conn.executescript(sql)
    conn.commit()
    conn.close()

init_db()

# ── APP STATE (in-memory — credentials are never written to disk) ─────────────

class _State:
    cookie:       Optional[str]  = None
    api_key:      Optional[str]  = None
    mode:         str            = "cookie"
    monitoring:   bool           = False
    monitor_task: Optional[asyncio.Task] = None
    known_assets: dict           = {}   # groupId -> set[str]
    account_info: Optional[dict] = None

state = _State()

# ── CONFIG ────────────────────────────────────────────────────────────────────

DEFAULT_CFG = {
    "pollingInterval": 60,
    "archiveDelay":    0,
    "notifyEnabled":   True,
    "whitelist":       [],
    "dmTemplate": (
        "Hi [USER_NAME], your audio [AUDIO_NAME] was removed from [GROUP_NAME] "
        "because we only accept uploads through approved channels.\n\n"
        "To share your audio with us: upload it to your own account, then go to the "
        "Creator Hub, find your asset, open Permissions, and add [ALT_ACCOUNT] as a collaborator.\n\n"
        "If you believe this was a mistake, please contact group staff."
    ),
    "altAccount": "",
}

def get_config() -> dict:
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM config").fetchall()
    conn.close()
    cfg = dict(DEFAULT_CFG)
    for row in rows:
        try:
            cfg[row["key"]] = json.loads(row["value"])
        except Exception:
            cfg[row["key"]] = row["value"]
    return cfg

def set_cfg(key: str, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                 (key, json.dumps(value)))
    conn.commit()
    conn.close()

# ── ROBLOX API HELPERS ────────────────────────────────────────────────────────

async def rblx_get(url: str, *, cookie=None, api_key=None, params=None) -> httpx.Response:
    headers: dict = {}
    cookies: dict = {}
    if cookie:
        cookies[".ROBLOSECURITY"] = cookie
    if api_key:
        headers["x-api-key"] = api_key
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
        return await c.get(url, headers=headers, cookies=cookies, params=params)

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

async def validate_apikey(api_key: str) -> dict:
    r = await rblx_get(
        "https://apis.roblox.com/assets/v1/assets",
        api_key=api_key,
        params={"maxPageSize": 1},
    )
    if r.status_code == 401:
        raise HTTPException(400, "Invalid API key")
    return {"valid": True}

async def get_group_name(group_id: str, cookie=None) -> str:
    try:
        r = await rblx_get(f"https://groups.roblox.com/v1/groups/{group_id}", cookie=cookie)
        if r.status_code == 200:
            return r.json().get("name", f"Group {group_id}")
    except Exception:
        pass
    return f"Group {group_id}"

async def fetch_group_audios(group_id: str, *, cookie=None, api_key=None) -> list[dict]:
    """Returns list of {id, name, creatorId, creatorName}"""
    assets: list[dict] = []

    if cookie:
        cursor = None
        for _ in range(5):  # max 500 assets (5x100)
            params = {"assetType": "Audio", "sortOrder": "Desc", "limit": 100}
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
                })
            cursor = d.get("nextPageCursor")
            if not cursor:
                break

    else:
        # API key mode -- use Open Cloud v1 assets API
        cursor = None
        for _ in range(5):
            params = {
                "groupId": group_id,
                "assetType": "Audio",
                "limit": "100",
            }
            if cursor:
                params["cursor"] = cursor
            r = await rblx_get(
                f"https://apis.roblox.com/toolbox-service/v1/groups/{group_id}/assets",
                api_key=api_key,
                params=params,
            )
            print(f"[SENTINEL] opencloud fetch group={group_id} status={r.status_code} body={r.text[:400]}")
            if r.status_code != 200:
                # Try alternate endpoint
                r = await rblx_get(
                    f"https://apis.roblox.com/assets/v1/assets?groupId={group_id}&assetType=Audio&limit=100",
                    api_key=api_key,
                )
                print(f"[SENTINEL] opencloud alt fetch status={r.status_code} body={r.text[:400]}")
                if r.status_code != 200:
                    break
            d = r.json()
            print(f"[SENTINEL] response keys: {list(d.keys())}")
            for item in d.get("assets", d.get("data", [])):
                creator = item.get("creationContext", {}).get("creator", {})
                assets.append({
                    "id":          str(item.get("assetId", item.get("id", ""))),
                    "name":        item.get("displayName", item.get("name", "Unknown")),
                    "creatorId":   str(creator.get("userId", item.get("creatorId", ""))),
                    "creatorName": str(creator.get("userId", item.get("creatorName", ""))),
                })
            cursor = d.get("nextPageToken", d.get("nextPageCursor"))
            if not cursor:
                break

    return assets
async def archive_asset(asset_id: str, *, cookie=None, api_key=None) -> bool:
    if cookie:
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

    if api_key:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                f"https://apis.roblox.com/cloud/v2/assets/{asset_id}:archive",
                headers={"x-api-key": api_key, "Content-Type": "application/json"},
                json={},
            )
            print(f"[SENTINEL] archive v2 status={r.status_code} body={r.text[:200]}")
            if r.status_code in (200, 204):
                return True
            r2 = await c.patch(
                f"https://apis.roblox.com/assets/v1/assets/{asset_id}",
                headers={"x-api-key": api_key, "Content-Type": "application/json"},
                json={"assetType": "Audio", "archived": True},
            )
            print(f"[SENTINEL] archive v1 status={r2.status_code} body={r2.text[:200]}")
            return r2.status_code in (200, 204)

    return False

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

async def monitor_loop():
    print("[SENTINEL] Monitor loop started")
    while state.monitoring:
        cfg       = get_config()
        poll_sec  = int(cfg.get("pollingInterval", 60))
        delay_sec = int(cfg.get("archiveDelay", 0))
        notify    = bool(cfg.get("notifyEnabled", True))
        whitelist = {str(u).strip().lower() for u in cfg.get("whitelist", [])}
        dm_tmpl   = str(cfg.get("dmTemplate", DEFAULT_CFG["dmTemplate"]))
        alt       = str(cfg.get("altAccount", ""))

        conn   = get_db()
        groups = conn.execute("SELECT id, name FROM groups").fetchall()
        conn.close()

        for grp in groups:
            gid, gname = grp["id"], grp["name"]
            try:
                assets = await fetch_group_audios(gid, cookie=state.cookie, api_key=state.api_key)
                print(f"[SENTINEL] Group {gid} fetched {len(assets)} total assets")
                current = {a["id"]: a for a in assets}
                current_ids = set(current)

                if gid not in state.known_assets:
                    state.known_assets[gid] = current_ids
                    print(f"[SENTINEL] Group {gid} baseline: {len(current_ids)} assets")
                    continue

                new_ids = current_ids - state.known_assets[gid]
                state.known_assets[gid] = current_ids

                for aid in new_ids:
                    a = current.get(aid, {})
                    creator_id   = a.get("creatorId", "")
                    creator_name = a.get("creatorName", "Unknown")

                    if creator_id.lower() in whitelist or creator_name.lower() in whitelist:
                        print(f"[SENTINEL] Whitelist skip: {creator_name}")
                        continue

                    print(f"[SENTINEL] New asset {aid} ({a.get('name')}) by {creator_name}")

                    if delay_sec > 0:
                        await asyncio.sleep(delay_sec)
                        if not state.monitoring:
                            break

                    ok = await archive_asset(aid, cookie=state.cookie, api_key=state.api_key)

                    dm_status = "n/a"
                    if notify and state.cookie and creator_id:
                        try:
                            msg = (dm_tmpl
                                   .replace("[USER_NAME]",  creator_name)
                                   .replace("[AUDIO_NAME]", a.get("name", ""))
                                   .replace("[ALT_ACCOUNT]", alt)
                                   .replace("[GROUP_NAME]", gname))
                            sent = await send_dm(creator_id, "Audio Policy Notice", msg, state.cookie)
                            dm_status = "sent" if sent else "failed"
                        except Exception as e:
                            print(f"[SENTINEL] DM error: {e}")
                            dm_status = "failed"

                    conn = get_db()
                    conn.execute(
                        "INSERT OR IGNORE INTO history"
                        " (id, username, display_name, user_id, audio_name, audio_id,"
                        " group_id, group_name, time, dm_status, archived)"
                        " VALUES (?,?,?,?,?,?,?,?,?,?,1)",
                        (
                            f"{aid}_{int(time.time())}",
                            creator_name, creator_name, creator_id,
                            a.get("name", "Unknown"), aid,
                            gid, gname,
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            dm_status,
                        ),
                    )
                    conn.commit()
                    conn.close()
                    print(f"[SENTINEL] archived={ok}  dm={dm_status}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[SENTINEL] Error in group {gid}: {e}")

        await asyncio.sleep(poll_sec)

    print("[SENTINEL] Monitor loop stopped")

# ── PYDANTIC MODELS ───────────────────────────────────────────────────────────

class CookieBody(BaseModel):
    cookie: str

class ApiKeyBody(BaseModel):
    apiKey: str

class GroupBody(BaseModel):
    id:   str
    name: str = ""

class ConfigBody(BaseModel):
    pollingInterval: Optional[int]       = None
    archiveDelay:    Optional[int]       = None
    notifyEnabled:   Optional[bool]      = None
    whitelist:       Optional[List[str]] = None
    dmTemplate:      Optional[str]       = None
    altAccount:      Optional[str]       = None

# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"ok": True, "monitoring": state.monitoring}

@app.post("/api/validate-cookie")
async def api_validate_cookie(body: CookieBody):
    info = await validate_cookie(body.cookie)
    state.cookie       = body.cookie
    state.api_key      = None
    state.mode         = "cookie"
    state.account_info = info
    return info

@app.post("/api/validate-apikey")
async def api_validate_apikey(body: ApiKeyBody):
    res = await validate_apikey(body.apiKey)
    state.api_key      = body.apiKey
    state.cookie       = None
    state.mode         = "api"
    state.account_info = {
        "userId": "", "username": "API Key Mode",
        "displayName": "Open Cloud", "avatarUrl": None,
    }
    return res

@app.get("/api/status")
def api_status():
    return {
        "monitoring":    state.monitoring,
        "mode":          state.mode,
        "account":       state.account_info,
        "hasCredential": bool(state.cookie or state.api_key),
    }

@app.post("/api/monitoring/start")
async def api_start():
    if not (state.cookie or state.api_key):
        raise HTTPException(400, "No credentials. Complete setup first.")
    if state.monitoring:
        return {"status": "already_running"}
    state.monitoring   = True
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
    conn = get_db()
    rows = conn.execute("SELECT id, name, added_at FROM groups ORDER BY added_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/api/groups")
async def api_add_group(body: GroupBody):
    gid = body.id.strip()
    if not gid.isdigit():
        raise HTTPException(400, "Group ID must be numeric")
    name = body.name.strip()
    if not name:
        name = await get_group_name(gid, cookie=state.cookie)
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO groups (id, name, added_at) VALUES (?,?,?)",
                 (gid, name, time.time()))
    conn.commit()
    conn.close()
    return {"id": gid, "name": name, "added_at": time.time()}

@app.delete("/api/groups/{group_id}")
def api_remove_group(group_id: str):
    conn = get_db()
    conn.execute("DELETE FROM groups WHERE id=?", (group_id,))
    conn.commit()
    conn.close()
    state.known_assets.pop(group_id, None)
    return {"deleted": True}

@app.get("/api/history")
def api_history(limit: int = 200, search: str = ""):
    conn = get_db()
    if search:
        s = f"%{search}%"
        rows = conn.execute(
            "SELECT * FROM history"
            " WHERE username LIKE ? OR audio_name LIKE ? OR audio_id LIKE ?"
            " ORDER BY time DESC LIMIT ?",
            (s, s, s, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM history ORDER BY time DESC LIMIT ?", (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.delete("/api/history")
def api_clear_history():
    conn = get_db()
    conn.execute("DELETE FROM history")
    conn.commit()
    conn.close()
    return {"cleared": True}

@app.get("/api/stats")
def api_stats():
    conn = get_db()
    archived = conn.execute("SELECT COUNT(*) FROM history WHERE archived=1").fetchone()[0]
    dms      = conn.execute("SELECT COUNT(*) FROM history WHERE dm_status='sent'").fetchone()[0]
    groups   = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
    conn.close()
    wl = len(get_config().get("whitelist", []))
    return {"archived": archived, "dms": dms, "groups": groups, "whitelisted": wl}

@app.get("/api/config")
def api_get_config():
    return get_config()

@app.post("/api/config")
def api_update_config(body: ConfigBody):
    for k, v in body.model_dump(exclude_none=True).items():
        set_cfg(k, v)
    return get_config()

# ── SERVE FRONTEND ────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

@app.get("/", response_class=HTMLResponse)
def serve_root():
    p = STATIC_DIR / "index.html"
    return HTMLResponse(p.read_text() if p.exists() else "<h1>Frontend missing — static/index.html not found</h1>", 200)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
