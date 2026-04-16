# SENTINEL — Roblox Audio Moderation System

## Project Structure
```
sentinel/
├── main.py          ← FastAPI backend (all API logic + Roblox calls)
├── requirements.txt ← Python dependencies
├── render.yaml      ← One-click Render deployment config
└── static/
    └── index.html   ← Frontend dashboard (served by the backend)
```

---

## Deploy to Render (Recommended)

1. **Push to GitHub** — Create a new repo and push this whole folder.

2. **Connect to Render** — Go to [render.com](https://render.com) → New → Web Service → connect your repo.

3. **Or use render.yaml** — Render will auto-detect `render.yaml` and configure everything (Python, build command, start command, persistent disk).

4. **Important: Use Starter plan ($7/mo)** — The free tier spins down after 15 min of inactivity, which would kill your monitoring loop. The Starter plan keeps it always-on.

5. Your app will be live at `https://sentinel-xxxx.onrender.com`

---

## Run Locally (Testing)

```bash
# Install deps
pip install -r requirements.txt

# Start server
uvicorn main:app --host 0.0.0.0 --port 8000

# Open browser
open http://localhost:8000
```

---

## How It Works

### Cookie Mode (Recommended)
- Uses your `.ROBLOSECURITY` cookie to authenticate
- Supports auto-DM to uploaders
- Full feature access

### Open Cloud API Mode
- Uses a Roblox Open Cloud API key
- More stable/doesn't expire
- DMs not supported

### Monitoring Loop
1. On startup of monitoring, records a **baseline** of existing audio assets per group
2. Every `pollingInterval` seconds, fetches current assets
3. Compares against baseline — **new asset IDs** are flagged
4. If uploader is **not** on the whitelist, applies `archiveDelay`, then archives the asset
5. If cookie mode + notifications enabled, sends a **DM** to the uploader
6. Logs everything to history in SQLite

---

## Cookie Notes
- Credentials are stored **in-memory on the server only** — never written to disk
- If the server restarts, you'll need to re-enter your cookie (just re-run setup)
- Always use a dedicated bot/mod account, never your main

---

## DM Template Placeholders
| Placeholder     | Replaced with            |
|----------------|--------------------------|
| `[USER_NAME]`  | Uploader's Roblox username |
| `[AUDIO_NAME]` | Name of the archived asset |
| `[GROUP_NAME]` | Group name                 |
| `[ALT_ACCOUNT]`| Your configured alt account |

---

## Rate Limiting Tips
- Default poll interval is 60s — don't go below 30s
- Archive delay of 0 = immediate deletion on detection
- The backend uses exponential-safe async sleeping, so it won't hammer Roblox

---

## Roblox API Endpoints Used
| Action              | Endpoint                                                     |
|--------------------|--------------------------------------------------------------|
| Validate cookie     | `GET users.roblox.com/v1/users/authenticated`               |
| Get avatar          | `GET thumbnails.roblox.com/v1/users/avatar-headshot`        |
| List group audios   | `GET develop.roblox.com/v1/groups/{id}/assets`              |
| Archive asset       | `POST develop.roblox.com/v1/assets/{id}/archive`            |
| Get CSRF token      | `POST auth.roblox.com/v2/logout`                            |
| Send DM             | `POST privatemessages.roblox.com/v1/messages`               |
| Get group name      | `GET groups.roblox.com/v1/groups/{id}`                      |
