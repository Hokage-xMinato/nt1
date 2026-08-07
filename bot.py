"""
bot.py – NextToppers + MissionJeet Telegram Bot
Combined single-file bot with MySQL persistence, auto-posting, broadcast,
force-all-update, daily logs, midnight DB backup, and beautiful UI.

CHANGES vs previous version:
- Courses are loaded from DB (batches table), NOT from the platform API.
- "batches" table now has a `name` VARCHAR column (nullable → shows "Untitled").
- Direct API calls to course.nexttoppers.com for all-content + content-details
  (same base URL for both NT and MJ, with different app_id/user_id/auth headers).
- Add-course flow: owner sends "addcourse nt 107" or uses ➕ Add Course button.
- Delete-course button on the course detail screen.
- Rename-course button on the course detail screen.
- build_course_keyboard now uses DB rows, not API data.
"""

import os
import io
import csv
import json
import asyncio
import logging
import threading
import base64
import datetime
from urllib.parse import quote

import aiohttp
import mysql.connector
from mysql.connector import pooling
import tempfile
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ═══════════════════════════════════════════════════════════════
#  CONFIG  –  Sensitive values come from environment variables
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
DB_PASSWORD = os.environ["DB_PASSWORD"]

# ── Non-sensitive / tweak here ─────────────────────────────────
CHECK_INTERVAL_HOURS = 24
FLASK_PORT = int(os.getenv("PORT", "8080"))
WATERMARK = "\n\n<i>Provided by @smartrz</i>"

PLAYER_BASE = "https://smarterz.netlify.app/ntplayer"
PLAYER_SECRET = "smarterzpro"

# ── Direct API base (course.nexttoppers.com) ───────────────────
COURSE_API_BASE = "https://course.nexttoppers.com/course"

# ── Per-platform credentials for direct API calls ─────────────
# app_id / user_id are static; bearer tokens are fetched dynamically
# from the auth API at the start of every update session.
NT_APP_ID  = os.getenv("NT_APP_ID",  "1770981347")
NT_USER_ID = os.getenv("NT_USER_ID", "682065")

MJ_APP_ID  = os.getenv("MJ_APP_ID",  "1772100600")
MJ_USER_ID = os.getenv("MJ_USER_ID", "3186295")

# ── Auth token API ─────────────────────────────────────────────
AUTH_API_URL = "https://nt-bearer.vercel.app/api"

# ── Delta Study cookie API ──────────────────────────────────────
DELTA_COOKIE_API  = "https://deltacookie.vercel.app/api/cookie"
DELTA_API_BASE    = "https://apiserver.deltastudy.site/api/nexttoppers"
DELTA_MJ_API_BASE = "https://apiserver.deltastudy.site/api/missionjeet"

# In-memory delta cookie cache  { "cookie": "delta_cf_verified=…", "expires_at": datetime }
_delta_cookie_cache: dict = {}

# Maps auth-API response keys → platform keys
# e.g. "nexttoppers-107" → nt,  "missionjeet-151" → mj
_AUTH_KEY_PLATFORM = {
    "nexttoppers": "nt",
    "missionjeet": "mj",
}

# In-memory bearer token cache  { "nt": "Bearer eyJ...", "mj": "Bearer eyJ..." }
_bearer_cache: dict[str, str] = {}

PLATFORM_CREDS = {
    "nt": {
        "app_id":  NT_APP_ID,
        "user_id": NT_USER_ID,
        "origin":  "https://nexttoppers.com",
        "referer": "https://nexttoppers.com/",
    },
    "mj": {
        "app_id":  MJ_APP_ID,
        "user_id": MJ_USER_ID,
        "origin":  "https://missionjeet.in",
        "referer": "https://missionjeet.in/",
    },
}

# ─── Platform metadata (label / emoji only — no API URLs needed) ──
PLATFORMS = {
    "nt": {"label": "NextToppers", "emoji": "📘"},
    "mj": {"label": "MissionJeet", "emoji": "🎯"},
}

# ═══════════════════════════════════════════════════════════════
#  DATABASE  –  Aiven MySQL
# ═══════════════════════════════════════════════════════════════
_DB_HOST = "mysql-15f5ad4f-himanshu-dd3b.e.aivencloud.com"
_DB_PORT = 21771
_DB_USER = "avnadmin"
_DB_DATABASE = "defaultdb"
_CA_CERT_INLINE = os.getenv("AIVEN_CA_CERT", "").strip()

_pool: pooling.MySQLConnectionPool | None = None


def _build_ssl_args() -> dict:
    if _CA_CERT_INLINE:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".pem", delete=False, prefix="aiven_ca_"
        )
        tmp.write(_CA_CERT_INLINE.replace("\\n", "\n"))
        tmp.flush()
        tmp.close()
        return {
            "ssl_ca": tmp.name,
            "ssl_verify_cert": True,
            "ssl_verify_identity": False,
        }
    return {"ssl_disabled": False, "ssl_verify_cert": False}


_CONNECT_ARGS = {
    "host": _DB_HOST,
    "port": _DB_PORT,
    "user": _DB_USER,
    "password": DB_PASSWORD,
    "database": _DB_DATABASE,
    "connection_timeout": 15,
    "autocommit": False,
    **_build_ssl_args(),
}


def _get_pool() -> pooling.MySQLConnectionPool:
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="ntbot", pool_size=5, **_CONNECT_ARGS
        )
    return _pool


def _conn():
    return _get_pool().get_connection()


# ── Schema ──────────────────────────────────────────────────────
def db_init():
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            platform   VARCHAR(10)  NOT NULL,
            course_id  INT          NOT NULL,
            name       VARCHAR(255) DEFAULT NULL,
            channel_id BIGINT       DEFAULT NULL,
            status     VARCHAR(10)  NOT NULL DEFAULT 'active',
            PRIMARY KEY (platform, course_id)
        )
    """)
    # Add name column if upgrading from old schema
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME   = 'batches'
          AND COLUMN_NAME  = 'name'
    """)
    (col_exists,) = cur.fetchone()
    if not col_exists:
        cur.execute("ALTER TABLE batches ADD COLUMN name VARCHAR(255) DEFAULT NULL AFTER course_id")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_files (
            id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            platform   VARCHAR(10) NOT NULL,
            course_id  INT         NOT NULL,
            content_id INT         NOT NULL,
            sent_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_sent (platform, course_id, content_id)
        )
    """)
    con.commit()
    cur.close()
    con.close()
    log.info("DB schema ready.")


def db_ping():
    try:
        con = _conn()
        cur = con.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        con.close()
        log.info("DB ping OK.")
    except Exception as e:
        log.warning(f"DB ping failed: {e}")
        global _pool
        _pool = None


def db_get_batch(platform, course_id):
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM batches WHERE platform=%s AND course_id=%s",
        (platform, course_id),
    )
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def db_get_all_active_batches():
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM batches WHERE status='active' AND channel_id IS NOT NULL"
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def db_get_all_batches():
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute("SELECT * FROM batches ORDER BY platform, course_id")
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def db_get_batches_for_platform(platform):
    """Return all batches for a given platform, ordered by course_id."""
    con = _conn()
    cur = con.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM batches WHERE platform=%s ORDER BY course_id",
        (platform,),
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def db_upsert_batch(
    platform, course_id, name=None, channel_id=None, remove_channel=False, status=None
):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT IGNORE INTO batches (platform, course_id) VALUES (%s, %s)",
        (platform, course_id),
    )
    if name is not None:
        cur.execute(
            "UPDATE batches SET name=%s WHERE platform=%s AND course_id=%s",
            (name, platform, course_id),
        )
    if remove_channel:
        cur.execute(
            "UPDATE batches SET channel_id=NULL WHERE platform=%s AND course_id=%s",
            (platform, course_id),
        )
    elif channel_id is not None:
        cur.execute(
            "UPDATE batches SET channel_id=%s WHERE platform=%s AND course_id=%s",
            (channel_id, platform, course_id),
        )
    if status is not None:
        cur.execute(
            "UPDATE batches SET status=%s WHERE platform=%s AND course_id=%s",
            (status, platform, course_id),
        )
    con.commit()
    cur.close()
    con.close()


def db_set_status(platform, course_id, status):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO batches (platform, course_id, status) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE status=VALUES(status)",
        (platform, course_id, status),
    )
    con.commit()
    cur.close()
    con.close()


def db_delete_batch(platform, course_id):
    """Remove a course from the batches table entirely."""
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "DELETE FROM batches WHERE platform=%s AND course_id=%s",
        (platform, course_id),
    )
    cur.execute(
        "DELETE FROM sent_files WHERE platform=%s AND course_id=%s",
        (platform, course_id),
    )
    con.commit()
    cur.close()
    con.close()


def db_rename_batch(platform, course_id, name: str):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "UPDATE batches SET name=%s WHERE platform=%s AND course_id=%s",
        (name, platform, course_id),
    )
    con.commit()
    cur.close()
    con.close()


def db_is_sent(platform, course_id, content_id):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM sent_files WHERE platform=%s AND course_id=%s AND content_id=%s",
        (platform, course_id, content_id),
    )
    found = cur.fetchone() is not None
    cur.close()
    con.close()
    return found


def db_mark_sent(platform, course_id, content_id):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "INSERT IGNORE INTO sent_files (platform, course_id, content_id) VALUES (%s,%s,%s)",
        (platform, course_id, content_id),
    )
    con.commit()
    cur.close()
    con.close()


def db_reset_sent(platform, course_id):
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "DELETE FROM sent_files WHERE platform=%s AND course_id=%s",
        (platform, course_id),
    )
    con.commit()
    cur.close()
    con.close()


def db_dump_csv() -> bytes:
    """Export both tables as a combined CSV for backup."""
    buf = io.StringIO()
    writer = csv.writer(buf)

    con = _conn()
    cur = con.cursor()

    writer.writerow(["=== TABLE: batches ==="])
    cur.execute("SELECT * FROM batches")
    writer.writerow([d[0] for d in cur.description])
    writer.writerows(cur.fetchall())

    writer.writerow([])
    writer.writerow(["=== TABLE: sent_files ==="])
    cur.execute("SELECT * FROM sent_files")
    writer.writerow([d[0] for d in cur.description])
    writer.writerows(cur.fetchall())

    cur.close()
    con.close()
    return buf.getvalue().encode()


# ═══════════════════════════════════════════════════════════════
#  FLASK KEEP-ALIVE
# ═══════════════════════════════════════════════════════════════
flask_app = Flask(__name__)


@flask_app.route("/")
def health():
    return "OK – Bot is running hehehe", 200


@flask_app.route("/ping")
def ping():
    return "pong", 200


def start_flask():
    def _run():
        from werkzeug.serving import make_server
        server = make_server("0.0.0.0", FLASK_PORT, flask_app)
        log.info(f"Flask keep-alive on port {FLASK_PORT}")
        server.serve_forever()
    threading.Thread(target=_run, daemon=True).start()


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s"
)
log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════
def owner_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *a, **kw):
        if (
            update.effective_chat.type == "private"
            and update.effective_user.id != OWNER_ID
        ):
            await update.effective_message.reply_text("⛔ Access denied.")
            return
        return await func(update, ctx, *a, **kw)
    return wrapper


async def fetch_auth_tokens() -> bool:
    """
    Fetch fresh bearer tokens from the auth API and populate _bearer_cache.
    Returns True on success, False on failure (old cache preserved on failure).
    Maps response keys like "nexttoppers-107" -> platform "nt",
    "missionjeet-151" -> platform "mj".
    """
    global _bearer_cache
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(AUTH_API_URL, timeout=aiohttp.ClientTimeout(total=15)) as r:
                r.raise_for_status()
                data = await r.json(content_type=None)

        new_cache: dict[str, str] = {}
        for key, token in data.items():
            prefix = key.split("-")[0].lower()
            plat = _AUTH_KEY_PLATFORM.get(prefix)
            if plat:
                raw = token.strip()
                new_cache[plat] = raw if raw.startswith("Bearer ") else f"Bearer {raw}"

        if not new_cache:
            log.warning("fetch_auth_tokens: response had no recognisable platform keys — keeping old cache")
            return False

        _bearer_cache.update(new_cache)
        log.info(f"fetch_auth_tokens: updated tokens for platforms: {list(new_cache.keys())}")
        return True
    except Exception as e:
        log.error(f"fetch_auth_tokens: failed to fetch tokens: {e}")
        return False


def _get_bearer(platform: str) -> str:
    """Return cached bearer token; raises RuntimeError if cache is empty."""
    token = _bearer_cache.get(platform, "")
    if not token:
        raise RuntimeError(
            f"No bearer token in cache for platform '{platform}'. "
            "fetch_auth_tokens() must be called before starting an update session."
        )
    return token


def _direct_headers(platform: str) -> dict:
    """Build the HTTP headers for direct course.nexttoppers.com API calls."""
    creds = PLATFORM_CREDS[platform]
    bearer = _get_bearer(platform)
    return {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "app_id": creds["app_id"],
        "authorization": bearer,
        "content-type": "application/json",
        "origin": creds["origin"],
        "platform": "3",
        "referer": creds["referer"],
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "user_id": creds["user_id"],
        "version": "1",
    }


# ═══════════════════════════════════════════════════════════════
#  DELTA STUDY  –  Cookie Management
# ═══════════════════════════════════════════════════════════════
async def fetch_delta_cookie(session: aiohttp.ClientSession | None = None) -> str | None:
    """
    Fetch a fresh delta_cf_verified cookie from the cookie API and cache it.
    Returns the raw cookie string (e.g. 'delta_cf_verified=…') or None on failure.
    Uses a passed session if provided, otherwise creates a temporary one.
    """
    global _delta_cookie_cache
    _own_session = session is None
    try:
        if _own_session:
            session = aiohttp.ClientSession()
        async with session.get(
            DELTA_COOKIE_API, timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            r.raise_for_status()
            data = await r.json(content_type=None)

        # Response: {"cookie": ["delta_cf_verified=…; Path=/; …"], "expiresAt": "…", …}
        cookie_list = data.get("cookie") or []
        if not cookie_list:
            log.warning("fetch_delta_cookie: API returned no cookie values")
            return None

        # Extract just the cookie name=value part (everything before the first ";")
        raw_cookie = cookie_list[0].split(";")[0].strip()

        expires_at_str = data.get("expiresAt")
        expires_at = None
        if expires_at_str:
            try:
                expires_at = datetime.datetime.fromisoformat(
                    expires_at_str.replace("Z", "+00:00")
                )
            except Exception:
                pass

        _delta_cookie_cache = {"cookie": raw_cookie, "expires_at": expires_at}
        log.info(f"fetch_delta_cookie: refreshed → {raw_cookie[:40]}… expires={expires_at}")
        return raw_cookie

    except Exception as e:
        log.error(f"fetch_delta_cookie: failed: {e}")
        return None
    finally:
        if _own_session and session is not None:
            await session.close()


async def _get_delta_cookie(session: aiohttp.ClientSession | None = None) -> str:
    """
    Return a valid delta cookie, fetching one if the cache is empty or stale.
    Raises RuntimeError if unable to obtain a cookie.
    """
    cached = _delta_cookie_cache.get("cookie", "")
    expires_at = _delta_cookie_cache.get("expires_at")

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    is_expired = (
        not cached
        or (expires_at is not None and expires_at <= now_utc + datetime.timedelta(seconds=60))
    )

    if is_expired:
        cookie = await fetch_delta_cookie(session)
        if not cookie:
            raise RuntimeError("Could not obtain a delta_cf_verified cookie")
        return cookie

    return cached


def _delta_headers(cookie: str) -> dict:
    """Build HTTP headers for apiserver.deltastudy.site requests."""
    return {
        "accept": "application/json, text/plain, */*",
        "cookie": cookie,
        "origin": "https://deltastudy.site",
        "referer": "https://deltastudy.site/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
    }


async def _delta_get(
    session: aiohttp.ClientSession, path: str, params: dict
) -> dict | None:
    """
    GET https://apiserver.deltastudy.site/api/nexttoppers/<path> with delta cookie.
    Auto-refreshes the cookie on 403 and retries once.
    Returns the parsed JSON dict, or None on failure.
    """
    url = f"{DELTA_API_BASE}/{path}"
    for attempt in range(1, 4):
        try:
            cookie = await _get_delta_cookie(session)
            headers = _delta_headers(cookie)
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status == 403:
                    log.warning(
                        f"_delta_get {path} attempt {attempt}/3: 403 — refreshing cookie"
                    )
                    # Force refresh by clearing cache
                    _delta_cookie_cache.clear()
                    if attempt < 3:
                        await asyncio.sleep(1)
                        continue
                    log.error(f"_delta_get {path}: still 403 after cookie refresh")
                    return None
                if r.status != 200:
                    log.warning(f"_delta_get {path} attempt {attempt}/3: HTTP {r.status}")
                    if attempt < 3:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return None
                data = await r.json(content_type=None)
                return data
        except Exception as e:
            log.warning(f"_delta_get {path} attempt {attempt}/3: {e}")
            if attempt < 3:
                await asyncio.sleep(2 ** attempt)
    return None


# ═══════════════════════════════════════════════════════════════
#  DELTA STUDY  –  Content & Video Detail Fetchers
# ═══════════════════════════════════════════════════════════════
async def fetch_delta_content_details(
    session: aiohttp.ClientSession,
    content_id: int,
    course_id: int,
) -> dict | None:
    """
    Fetch PDF / video content details from deltastudy.site.
    Endpoint: GET /api/nexttoppers/content-details?content_id=X&courseid=Y
    Returns the 'data' dict from the response, or None.
    """
    resp = await _delta_get(
        session,
        "content-details",
        params={"content_id": content_id, "courseid": course_id},
    )
    if resp is None:
        return None
    if not resp.get("success"):
        log.warning(
            f"fetch_delta_content_details: content_id={content_id} "
            f"success=false msg={resp.get('message','')}"
        )
        return None
    return resp.get("data")


async def fetch_delta_video_details(
    session: aiohttp.ClientSession,
    vdc_id: str,
) -> dict | None:
    """
    Fetch video details from deltastudy.site.
    Endpoint: GET /api/nexttoppers/video-details?videoid=<vdc_id>
    Returns the full 'data' dict (which contains file_url, duration,
    thumbnail, etc.), or None on failure.
    """
    resp = await _delta_get(
        session,
        "video-details",
        params={"videoid": vdc_id},
    )
    if resp is None:
        return None
    if not resp.get("success"):
        log.warning(
            f"fetch_delta_video_details: vdc_id={vdc_id} "
            f"success=false msg={resp.get('message','')}"
        )
        return None
    data = resp.get("data") or {}
    return data if data else None


async def _delta_get_mj(
    session: aiohttp.ClientSession, path: str, params: dict
) -> dict | None:
    """
    GET https://apiserver.deltastudy.site/api/missionjeet/<path> with delta cookie.
    Identical retry / cookie-refresh logic to _delta_get but uses DELTA_MJ_API_BASE.
    """
    url = f"{DELTA_MJ_API_BASE}/{path}"
    for attempt in range(1, 4):
        try:
            cookie = await _get_delta_cookie(session)
            headers = _delta_headers(cookie)
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status == 403:
                    log.warning(
                        f"_delta_get_mj {path} attempt {attempt}/3: 403 — refreshing cookie"
                    )
                    _delta_cookie_cache.clear()
                    if attempt < 3:
                        await asyncio.sleep(1)
                        continue
                    log.error(f"_delta_get_mj {path}: still 403 after cookie refresh")
                    return None
                if r.status != 200:
                    log.warning(f"_delta_get_mj {path} attempt {attempt}/3: HTTP {r.status}")
                    if attempt < 3:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return None
                data = await r.json(content_type=None)
                return data
        except Exception as e:
            log.warning(f"_delta_get_mj {path} attempt {attempt}/3: {e}")
            if attempt < 3:
                await asyncio.sleep(2 ** attempt)
    return None


# ─── MissionJeet-specific Delta fetchers ───────────────────────
async def fetch_mj_content_details(
    session: aiohttp.ClientSession,
    content_id: int,
    course_id: int,
) -> dict | None:
    """
    Fetch content details for MissionJeet from deltastudy.site.
    Endpoint: GET /api/missionjeet/content-details?content_id=X&course_id=Y
    Returns the 'data' dict from the response, or None.
    """
    resp = await _delta_get_mj(
        session,
        "content-details",
        params={"content_id": content_id, "course_id": course_id},
    )
    if resp is None:
        return None
    if not resp.get("success"):
        log.warning(
            f"fetch_mj_content_details: content_id={content_id} "
            f"success=false msg={resp.get('message','')}"
        )
        return None
    return resp.get("data")


async def fetch_mj_video_details(
    session: aiohttp.ClientSession,
    content_id: int,
    course_id: int,
    folder_id: int,
) -> dict | None:
    """
    Fetch video details for MissionJeet from deltastudy.site.
    Endpoint: GET /api/missionjeet/video-details?content_id=X&course_id=Y&folder_id=Z
    Returns the full 'data' dict (file_url, duration, thumbnail, etc.), or None.
    """
    resp = await _delta_get_mj(
        session,
        "video-details",
        params={"content_id": content_id, "course_id": course_id, "folder_id": folder_id},
    )
    if resp is None:
        return None
    if not resp.get("success"):
        log.warning(
            f"fetch_mj_video_details: content_id={content_id} course_id={course_id} "
            f"folder_id={folder_id} success=false msg={resp.get('message','')}"
        )
        return None
    data = resp.get("data") or {}
    return data if data else None


async def _direct_post(session: aiohttp.ClientSession, platform: str, path: str, body: dict) -> dict:
    """POST to course.nexttoppers.com with proper headers and retry logic.
    On 401/403 automatically refreshes the auth token once and retries."""
    url = f"{COURSE_API_BASE}{path}"
    last_exc = None
    for attempt in range(1, 4):
        try:
            headers = _direct_headers(platform)
            async with session.post(
                url, json=body, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                if r.status in (401, 403):
                    log.warning(f"[{platform}] POST {path} got {r.status} — refreshing auth token")
                    await fetch_auth_tokens()
                    if attempt < 3:
                        await asyncio.sleep(1)
                        continue
                    r.raise_for_status()
                r.raise_for_status()
                data = await r.json(content_type=None)
            if isinstance(data, dict) and data.get("success") is False:
                # Also refresh token if API says unauthorised
                msg = data.get("message", "")
                if any(w in msg.lower() for w in ("unauthori", "token", "expired")):
                    log.warning(f"[{platform}] POST {path} auth error in body — refreshing token")
                    await fetch_auth_tokens()
                log.warning(f"[{platform}] POST {path} success=false (attempt {attempt}/3)")
                if attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f"API success=false after 3 attempts: {url}")
            return data
        except RuntimeError:
            raise
        except Exception as e:
            last_exc = e
            log.warning(f"[{platform}] POST {path} attempt {attempt}/3: {e}")
            if attempt < 3:
                await asyncio.sleep(2 ** attempt)
    raise last_exc


async def _direct_get(session: aiohttp.ClientSession, platform: str, path: str, params: dict = None) -> dict:
    """GET from course.nexttoppers.com with proper headers and retry logic.
    On 401/403 automatically refreshes the auth token once and retries."""
    from urllib.parse import urlencode
    from multidict import CIMultiDict
    base_url = f"{COURSE_API_BASE}{path}"
    url = f"{base_url}?{urlencode(params)}" if params else base_url
    last_exc = None
    for attempt in range(1, 4):
        try:
            headers = CIMultiDict(_direct_headers(platform).items())
            async with session.get(
                url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                if r.status in (401, 403):
                    log.warning(f"[{platform}] GET {path} got {r.status} — refreshing auth token")
                    await fetch_auth_tokens()
                    if attempt < 3:
                        await asyncio.sleep(1)
                        continue
                    r.raise_for_status()
                r.raise_for_status()
                data = await r.json(content_type=None)
            if not isinstance(data, dict):
                raise RuntimeError(f"Unexpected response type {type(data)} from {url}")
            if data.get("success") is False:
                msg = data.get("message", "")
                if any(w in msg.lower() for w in ("unauthori", "token", "expired")):
                    log.warning(f"[{platform}] GET {path} auth error in body — refreshing token")
                    await fetch_auth_tokens()
                log.warning(f"[{platform}] GET {path} success=false rc={data.get('responseCode')} (attempt {attempt}/3): {msg}")
                if attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise RuntimeError(f"API success=false after 3 attempts: {url} — {msg}")
            return data
        except RuntimeError:
            raise
        except Exception as e:
            last_exc = e
            log.warning(f"[{platform}] GET {path} attempt {attempt}/3: {e}")
            if attempt < 3:
                await asyncio.sleep(2 ** attempt)
    raise last_exc


def encode_token(url: str) -> str:
    key = PLAYER_SECRET
    xored = bytes([ord(url[i]) ^ ord(key[i % len(key)]) for i in range(len(url))])
    return base64.b64encode(xored).decode()


def make_player_url(file_url: str) -> str:
    return f"{PLAYER_BASE}?token={quote(encode_token(file_url))}"


def pcfg(platform: str) -> dict:
    return PLATFORMS[platform]


def course_display_name(batch: dict) -> str:
    """Return the course name (from DB) or 'Untitled' if not set."""
    return (batch.get("name") or "").strip() or "Untitled"


def back_btn(platform: str) -> list:
    return [
        [InlineKeyboardButton("« Back to Courses", callback_data=f"back:{platform}")]
    ]


# ═══════════════════════════════════════════════════════════════
#  BEAUTIFUL UI BUILDER  –  now DB-driven (no API call)
# ═══════════════════════════════════════════════════════════════
def build_course_keyboard(batches: list, platform: str, cfg: dict) -> InlineKeyboardMarkup:
    """
    Build the course list keyboard from DB rows.
    Connected (has channel_id) shown first, then not-connected.
    """
    connected = []
    not_connected = []

    for b in batches:
        if b.get("channel_id"):
            connected.append(b)
        else:
            not_connected.append(b)

    keyboard = []

    if connected:
        keyboard.append(
            [InlineKeyboardButton("━━━━  ✅  CONNECTED  ━━━━", callback_data="noop")]
        )
        for b in connected:
            status = (b.get("status") or "active")
            s_icon = "▶️" if status == "active" else "⏸"
            name = course_display_name(b)
            label = f"{s_icon} {cfg['emoji']} {name[:40]}"
            keyboard.append(
                [InlineKeyboardButton(label, callback_data=f"course:{platform}:{b['course_id']}")]
            )

    if not_connected:
        keyboard.append(
            [InlineKeyboardButton("━━━  ○  NOT CONNECTED  ━━━", callback_data="noop")]
        )
        for b in not_connected:
            name = course_display_name(b)
            label = f"➕ {cfg['emoji']} {name[:40]}"
            keyboard.append(
                [InlineKeyboardButton(label, callback_data=f"course:{platform}:{b['course_id']}")]
            )

    keyboard.append(
        [InlineKeyboardButton("➕ Add Course", callback_data=f"addcourse:{platform}")]
    )
    keyboard.append(
        [
            InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
            InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton("📡 Broadcast All", callback_data="broadcast_prompt"),
            InlineKeyboardButton("⚡ Force All", callback_data="forceall_confirm"),
        ]
    )
    keyboard.append(
        [InlineKeyboardButton("🗄️ Backup Now", callback_data="backup_now")]
    )
    return InlineKeyboardMarkup(keyboard)


# ═══════════════════════════════════════════════════════════════
#  COURSE LIST  –  reads from DB, no API call
# ═══════════════════════════════════════════════════════════════
async def show_courses(edit_fn, platform: str):
    cfg = pcfg(platform)
    batches = db_get_batches_for_platform(platform)

    total_connected = sum(1 for b in batches if b.get("channel_id"))

    if not batches:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Course", callback_data=f"addcourse:{platform}")],
            [
                InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
                InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
            ],
        ])
        await edit_fn(
            f"{'─' * 28}\n"
            f"{cfg['emoji']}  *{cfg['label']} — Courses*\n"
            f"{'─' * 28}\n"
            f"No courses in DB yet. Tap ➕ Add Course to add one.",
            reply_markup=kb,
            parse_mode="Markdown",
        )
        return

    kb = build_course_keyboard(batches, platform, cfg)
    await edit_fn(
        f"{'─' * 28}\n"
        f"{cfg['emoji']}  *{cfg['label']} — Courses*\n"
        f"{'─' * 28}\n"
        f"📊  Total: `{len(batches)}`  |  ✅ Connected: `{total_connected}`  |  ○ Pending: `{len(batches) - total_connected}`\n"
        f"{'─' * 28}\n\n"
        f"Tap a course to manage it:",
        reply_markup=kb,
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════════
@owner_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [
            InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
            InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
        ],
        [
            InlineKeyboardButton("📡 Broadcast All", callback_data="broadcast_prompt"),
            InlineKeyboardButton("⚡ Force All", callback_data="forceall_confirm"),
        ],
        [InlineKeyboardButton("🗄️ Backup Now", callback_data="backup_now")],
    ]
    await update.message.reply_text(
        "╔══════════════════════════╗\n"
        "║   🤖  *Content Manager*   ║\n"
        "╚══════════════════════════╝\n\n"
        "Welcome back, Admin!\n"
        "Choose a platform to manage courses,\n"
        "or use the actions below.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


@owner_only
async def cmd_batches(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    platform = args[0].lower() if args and args[0].lower() in PLATFORMS else None
    if not platform:
        kb = [
            [
                InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
                InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
            ]
        ]
        await update.message.reply_text(
            "Which platform?", reply_markup=InlineKeyboardMarkup(kb)
        )
        return
    msg = await update.message.reply_text("⏳ Loading courses from DB…")
    await show_courses(msg.edit_text, platform)


@owner_only
async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/backup – send DB backup immediately."""
    msg = await update.message.reply_text("🗄️ Generating backup…")
    await _do_db_backup(ctx.application)
    await msg.delete()


@owner_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/broadcast <message> – broadcast to all connected channels."""
    text = " ".join(ctx.args) if ctx.args else ""
    if not text:
        await update.message.reply_text("Usage: /broadcast <your message>")
        return
    reply = await update.message.reply_text("📡 Broadcasting…")
    await _do_broadcast(reply.edit_text, ctx.application, text)


@owner_only
async def cmd_forceall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/forceall – force update every connected batch."""
    msg = await update.message.reply_text(
        "⚡ Starting force-update for all connected batches…"
    )
    await _do_forceall(msg.edit_text, ctx.application)


async def cb_backup_now(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🗄️ Sending backup…", show_alert=False)
    if query.from_user.id != OWNER_ID:
        return
    await _do_db_backup(ctx.application)
    await query.answer("✅ Backup sent to your DM!", show_alert=True)


async def _do_db_backup(app: Application):
    try:
        csv_bytes = db_dump_csv()
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
        filename = f"db_backup_{now}.csv"
        bio = io.BytesIO(csv_bytes)
        bio.name = filename
        await app.bot.send_document(
            chat_id=OWNER_ID,
            document=bio,
            filename=filename,
            caption=(
                f"🗄️ <b>Database Backup</b>\n"
                f"🕛 {now} UTC\n\n"
                f"Contains: <code>batches</code> + <code>sent_files</code> tables."
            ),
            parse_mode="HTML",
        )
        log.info("DB backup sent.")
    except Exception as e:
        log.error(f"DB backup failed: {e}")
        try:
            await app.bot.send_message(chat_id=OWNER_ID, text=f"❌ DB backup failed: {e}")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  BROADCAST LOGIC
# ═══════════════════════════════════════════════════════════════
async def _do_broadcast(reply_fn, app: Application, text: str):
    batches = db_get_all_active_batches()
    if not batches:
        await reply_fn("⚠️ No active connected batches to broadcast to.")
        return

    full_text = text + WATERMARK
    success, fail = 0, 0
    fail_details = []

    for batch in batches:
        try:
            msg = await app.bot.send_message(
                chat_id=batch["channel_id"], text=full_text, parse_mode="HTML",
            )
            await app.bot.pin_chat_message(
                chat_id=batch["channel_id"],
                message_id=msg.message_id,
                disable_notification=True,
            )
            success += 1
        except Exception as e:
            fail += 1
            fail_details.append(
                f"  • `{batch['channel_id']}` ({batch['platform']} / {batch['course_id']}): {e}"
            )

    report = (
        f"📡 *Broadcast Complete*\n"
        f"{'─' * 28}\n"
        f"✅ Sent & Pinned : `{success}`\n"
        f"❌ Failed        : `{fail}`\n"
    )
    if fail_details:
        report += "\n*Failures:*\n" + "\n".join(fail_details)
    await reply_fn(report, parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════════
#  FORCE-ALL LOGIC
# ═══════════════════════════════════════════════════════════════
async def _do_forceall(status_fn, app: Application):
    batches = db_get_all_active_batches()
    if not batches:
        await status_fn("⚠️ No active connected batches found.")
        return

    # ── Fetch fresh auth tokens for this update session ──────────
    await status_fn(
        f"⚡ *Force Update – All Batches*\n"
        f"{'─' * 28}\n"
        f"🔑 Fetching auth tokens…",
        parse_mode="Markdown",
    )
    await fetch_auth_tokens()

    await status_fn(
        f"⚡ *Force Update – All Batches*\n"
        f"{'─' * 28}\n"
        f"🔍 Found `{len(batches)}` active batch(es)…\nThis may take a while.",
        parse_mode="Markdown",
    )

    results = []
    async with aiohttp.ClientSession() as session:
        for batch_idx, batch in enumerate(batches, 1):
            platform = batch["platform"]
            course_id = batch["course_id"]
            channel_id = batch["channel_id"]
            cfg = pcfg(platform)
            posted = skipped = errors = 0

            try:
                files = await fetch_files_recursive(session, platform, course_id)
                total_files = len(files)
                log.info(f"[{platform}] forceall: found {total_files} file(s) for course {course_id}")

                processed = 0
                last_update = asyncio.get_event_loop().time()

                for f in files:
                    content_id = f["entity_id"]
                    processed += 1

                    # ── Telegram progress update every 10 s ──────
                    now = asyncio.get_event_loop().time()
                    if now - last_update >= 10:
                        bar = _progress_bar(processed, total_files)
                        try:
                            await status_fn(
                                f"⚡ *Force Update – All Batches*\n"
                                f"{'─' * 28}\n"
                                f"Batch `{batch_idx}/{len(batches)}` — {cfg['emoji']} Course `{course_id}`\n"
                                f"{bar}\n"
                                f"📤 posted={posted}  ⏭ skipped={skipped}  ❌ errors={errors}",
                                parse_mode="Markdown",
                            )
                        except Exception:
                            pass
                        last_update = now

                    if db_is_sent(platform, course_id, content_id):
                        skipped += 1
                        continue

                    detail = await get_content_detail(
                        session, platform, content_id, course_id,
                        inline_data=f.get("data"),
                        folder_id=int(f.get("parent_id") or 0),
                    )
                    if detail is None:
                        log.warning(
                            f"[{platform}] No detail for content_id={content_id} (course={course_id}), will retry."
                        )
                        skipped += 1
                        continue

                    file_url = (detail.get("file_url") or "").strip()
                    if not file_url:
                        log.info(f"[{platform}] content_id={content_id} has no file_url (locked?), skipping for retry.")
                        skipped += 1
                        continue

                    try:
                        ok = await post_content(app, platform, channel_id, detail, f["title"])
                        if ok:
                            db_mark_sent(platform, course_id, content_id)
                            posted += 1
                            await asyncio.sleep(1.2)
                        else:
                            skipped += 1
                    except Exception as post_err:
                        log.error(f"[{platform}] forceall post failed content_id={content_id}: {post_err}")
                        errors += 1

                results.append({
                    "platform": platform, "course_id": course_id,
                    "channel_id": channel_id, "posted": posted,
                    "skipped": skipped, "errors": errors, "ok": errors == 0,
                })
            except Exception as e:
                log.error(f"[{platform}] forceall scan error for course {course_id}: {e}")
                results.append({
                    "platform": platform, "course_id": course_id,
                    "channel_id": channel_id, "posted": posted,
                    "skipped": skipped, "errors": errors + 1, "ok": False, "err_msg": str(e),
                })

    total_posted = sum(r["posted"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    lines = [
        f"⚡ *Force Update – Summary*",
        f"{'─' * 28}",
        f"📦 Batches processed : `{len(results)}`",
        f"📤 Total posted      : `{total_posted}`",
        f"❌ Total errors      : `{total_errors}`",
        f"{'─' * 28}",
    ]
    for r in results:
        icon = "✅" if r["ok"] else "❌"
        cfg = pcfg(r["platform"])
        lines.append(
            f"{icon} {cfg['emoji']} Course `{r['course_id']}` → `{r['channel_id']}`\n"
            f"   📤 {r['posted']} posted  |  ⏭ {r['skipped']} skipped"
            + (f"\n   ⚠️ {r.get('err_msg', '')}" if not r["ok"] else "")
        )
    await status_fn("\n".join(lines), parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════════
#  CALLBACKS
# ═══════════════════════════════════════════════════════════════
async def cb_noop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()


async def cb_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        await query.answer("⛔ Access denied.", show_alert=True)
        return
    platform = query.data.split(":")[1]
    await show_courses(query.edit_message_text, platform)


async def cb_back(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    platform = query.data.split(":")[1]
    await show_courses(query.edit_message_text, platform)


async def cb_course(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        await query.answer("⛔ Access denied.", show_alert=True)
        return

    _, platform, cid = query.data.split(":")
    course_id = int(cid)
    cfg = pcfg(platform)
    batch = db_get_batch(platform, course_id)

    channel_id = batch["channel_id"] if batch else None
    status = (batch["status"] if batch else "active") or "active"
    name = course_display_name(batch) if batch else "Untitled"
    s_icon = "▶️" if status == "active" else "⏸"
    chan_txt = f"`{channel_id}`" if channel_id else "─ _not set_ ─"
    conn_badge = "✅ Connected" if channel_id else "○  Not Connected"

    text = (
        f"{'─' * 30}\n"
        f"{cfg['emoji']}  *{cfg['label']}*\n"
        f"📛  Name     :  *{name}*\n"
        f"Course ID :  `{course_id}`\n"
        f"{'─' * 30}\n"
        f"📡  Channel  :  {chan_txt}\n"
        f"🔗  Status   :  {conn_badge}\n"
        f"▶️  Mode     :  {s_icon} {status}\n"
        f"{'─' * 30}"
    )

    kb = [
        [
            InlineKeyboardButton("➕ Set Channel", callback_data=f"setchan:{platform}:{course_id}"),
            InlineKeyboardButton("➖ Remove Channel", callback_data=f"rmchan:{platform}:{course_id}"),
        ],
        [
            InlineKeyboardButton("⏸ Pause", callback_data=f"pause:{platform}:{course_id}"),
            InlineKeyboardButton("▶️ Resume", callback_data=f"resume:{platform}:{course_id}"),
        ],
        [
            InlineKeyboardButton("✏️ Rename Course", callback_data=f"rename:{platform}:{course_id}"),
        ],
        [
            InlineKeyboardButton("🔄 Restart (reset sent)", callback_data=f"rst_confirm:{platform}:{course_id}"),
        ],
        [
            InlineKeyboardButton("⚡ Force Update Now", callback_data=f"forceupdate:{platform}:{course_id}"),
        ],
        [
            InlineKeyboardButton("🗑️ Delete Course", callback_data=f"del_confirm:{platform}:{course_id}"),
        ],
    ] + back_btn(platform)

    await query.edit_message_text(
        text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown"
    )


async def cb_setchan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    ctx.user_data["awaiting_channel"] = (platform, int(cid))
    await query.edit_message_text(
        f"📩  Send the *channel ID* (e.g. `-1001234567890`)\nto link to course `{cid}`:",
        parse_mode="Markdown",
    )


async def cb_rmchan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_upsert_batch(platform, int(cid), remove_channel=True)
    await query.edit_message_text(
        f"✅ Channel removed from course `{cid}`.\nUse /start to go back.",
        parse_mode="Markdown",
    )


async def cb_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_set_status(platform, int(cid), "paused")
    await query.edit_message_text(
        f"⏸ Course `{cid}` paused.\nUse /start to go back.", parse_mode="Markdown"
    )


async def cb_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_set_status(platform, int(cid), "active")
    await query.edit_message_text(
        f"▶️ Course `{cid}` resumed.\nUse /start to go back.", parse_mode="Markdown"
    )


async def cb_rst_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    kb = [
        [
            InlineKeyboardButton("✅ Yes, reset", callback_data=f"restart:{platform}:{cid}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"course:{platform}:{cid}"),
        ]
    ]
    await query.edit_message_text(
        f"⚠️ *Restart course {cid}?*\n\n"
        "All sent-file records will be deleted.\n"
        "Content will re-post on the next check.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cb_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_reset_sent(platform, int(cid))
    await query.edit_message_text(
        f"🔄 Course `{cid}` reset.\nContent re-posts on next scheduled check.",
        parse_mode="Markdown",
    )


# ── Add Course ──────────────────────────────────────────────────
async def cb_addcourse(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Prompt owner to type the new course ID."""
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    platform = query.data.split(":")[1]
    ctx.user_data["awaiting_addcourse"] = platform
    await query.edit_message_text(
        f"➕ *Add Course — {PLATFORMS[platform]['label']}*\n\n"
        f"Send the *course ID* (number) to add to DB.\n"
        f"Optionally send `<id> <name>` to set a name immediately.\n\n"
        f"Example:  `107 Physics Batch 2025`",
        parse_mode="Markdown",
    )


# ── Rename Course ───────────────────────────────────────────────
async def cb_rename(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    ctx.user_data["awaiting_rename"] = (platform, int(cid))
    await query.edit_message_text(
        f"✏️ *Rename Course `{cid}`*\n\nSend the new name:",
        parse_mode="Markdown",
    )


# ── Delete Course ───────────────────────────────────────────────
async def cb_del_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    kb = [
        [
            InlineKeyboardButton("🗑️ Yes, delete", callback_data=f"delcourse:{platform}:{cid}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"course:{platform}:{cid}"),
        ]
    ]
    await query.edit_message_text(
        f"⚠️ *Delete course `{cid}` ({PLATFORMS[platform]['label']})?*\n\n"
        "This removes it from DB and deletes all sent-file records.\n"
        "The channel won't be affected.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cb_delcourse(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    _, platform, cid = query.data.split(":")
    db_delete_batch(platform, int(cid))
    await query.edit_message_text(
        f"🗑️ Course `{cid}` deleted from DB.\nUse /start to go back.",
        parse_mode="Markdown",
    )


async def cb_broadcast_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    ctx.user_data["awaiting_broadcast"] = True
    await query.edit_message_text(
        "📡 *Broadcast Message*\n\n"
        "Send the message text you want to broadcast to *all connected channels*.\n"
        "It will be pinned automatically.\n\n"
        "You can use HTML formatting.",
        parse_mode="Markdown",
    )


async def cb_forceall_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    kb = [
        [
            InlineKeyboardButton("⚡ Yes, force all", callback_data="forceall_go"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_action"),
        ]
    ]
    await query.edit_message_text(
        "⚡ *Force Update All Batches?*\n\n"
        "This will scan every connected course and post any new content.\n"
        "This may take several minutes.",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cb_forceall_go(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⚡ Starting…", show_alert=False)
    if query.from_user.id != OWNER_ID:
        return
    await _do_forceall(query.edit_message_text, ctx.application)


async def cb_cancel_action(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = [
        [
            InlineKeyboardButton("📘 NextToppers", callback_data="pick:nt"),
            InlineKeyboardButton("🎯 MissionJeet", callback_data="pick:mj"),
        ],
        [
            InlineKeyboardButton("📡 Broadcast All", callback_data="broadcast_prompt"),
            InlineKeyboardButton("⚡ Force All", callback_data="forceall_confirm"),
        ],
        [InlineKeyboardButton("🗄️ Backup Now", callback_data="backup_now")],
    ]
    await query.edit_message_text(
        "╔══════════════════════════╗\n"
        "║   🤖  *Content Manager*   ║\n"
        "╚══════════════════════════╝\n\n"
        "Action cancelled. Choose a platform:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cb_forceupdate(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⚡ Starting…", show_alert=False)
    if query.from_user.id != OWNER_ID:
        return

    _, platform, cid = query.data.split(":")
    course_id = int(cid)
    cfg = pcfg(platform)
    batch = db_get_batch(platform, course_id)

    if not batch or not batch.get("channel_id"):
        await query.edit_message_text(
            f"⚠️ Course {course_id} has no channel. Set one first."
        )
        return

    channel_id = batch["channel_id"]
    name = course_display_name(batch)

    # ── Fetch fresh auth tokens for this session ─────────────────
    await query.edit_message_text(
        f"{cfg['emoji']} *{cfg['label']} – {name}*\n\n"
        f"🔑 Fetching auth tokens…",
        parse_mode="Markdown",
    )
    await fetch_auth_tokens()

    await query.edit_message_text(
        f"{cfg['emoji']} *{cfg['label']} – {name}*\n\n"
        f"⚡ Force update running…\n📂 Fetching content list…",
        parse_mode="Markdown",
    )

    posted = skipped = errors = 0
    try:
        async with aiohttp.ClientSession() as session:
            files = await fetch_files_recursive(session, platform, course_id)
            total_files = len(files)
            log.info(f"[{platform}] forceupdate: found {total_files} file(s) for course {course_id}")

            processed = 0
            last_update = asyncio.get_event_loop().time()

            for f in files:
                content_id = f["entity_id"]
                processed += 1

                # ── Live Telegram progress bar every 10 s ────────
                now = asyncio.get_event_loop().time()
                if now - last_update >= 10:
                    bar = _progress_bar(processed, total_files)
                    try:
                        await query.edit_message_text(
                            f"{cfg['emoji']} *{cfg['label']} – {name}*\n"
                            f"{'─' * 30}\n"
                            f"⚡ Force update in progress…\n\n"
                            f"{bar}\n"
                            f"📤 posted={posted}  ⏭ skipped={skipped}  ❌ errors={errors}",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass
                    last_update = now

                if db_is_sent(platform, course_id, content_id):
                    skipped += 1
                    continue

                detail = await get_content_detail(
                    session, platform, content_id, course_id,
                    inline_data=f.get("data"),
                    folder_id=int(f.get("parent_id") or 0),
                )
                if detail is None:
                    log.warning(
                        f"[{platform}] No detail for content_id={content_id} (course={course_id}), will retry."
                    )
                    skipped += 1
                    continue

                file_url = (detail.get("file_url") or "").strip()
                if not file_url:
                    log.info(f"[{platform}] content_id={content_id} has no file_url (locked?), skipping for retry.")
                    skipped += 1
                    continue

                try:
                    ok = await post_content(ctx.application, platform, channel_id, detail, f["title"])
                    if ok:
                        db_mark_sent(platform, course_id, content_id)
                        posted += 1
                        await asyncio.sleep(1.2)
                    else:
                        # post_content already logged the specific skip reason
                        # (e.g. zero duration, missing file_url) — not a real error.
                        skipped += 1
                except Exception as post_err:
                    log.error(f"[{platform}] forceupdate post failed content_id={content_id}: {post_err}")
                    errors += 1

    except Exception as e:
        log.error(f"[{platform}] force update error {course_id}: {e}")
        errors += 1

    status_line = "✅ Done" if not errors else "⚠️ Finished with errors"
    kb = [
        [
            InlineKeyboardButton("➕ Set Channel", callback_data=f"setchan:{platform}:{course_id}"),
            InlineKeyboardButton("➖ Remove Channel", callback_data=f"rmchan:{platform}:{course_id}"),
        ],
        [
            InlineKeyboardButton("⏸ Pause", callback_data=f"pause:{platform}:{course_id}"),
            InlineKeyboardButton("▶️ Resume", callback_data=f"resume:{platform}:{course_id}"),
        ],
        [InlineKeyboardButton("✏️ Rename Course", callback_data=f"rename:{platform}:{course_id}")],
        [InlineKeyboardButton("🔄 Restart (reset sent)", callback_data=f"rst_confirm:{platform}:{course_id}")],
        [InlineKeyboardButton("⚡ Force Update Now", callback_data=f"forceupdate:{platform}:{course_id}")],
        [InlineKeyboardButton("🗑️ Delete Course", callback_data=f"del_confirm:{platform}:{course_id}")],
    ] + back_btn(platform)

    await query.edit_message_text(
        f"{'─' * 30}\n"
        f"{cfg['emoji']} *{cfg['label']} – {name} (Course {course_id})*\n"
        f"{'─' * 30}\n"
        f"📡 Channel : `{channel_id}`\n"
        f"{'─' * 30}\n\n"
        f"{status_line}\n"
        f"📤 Posted  : `{posted}`\n"
        f"⏭ Skipped : `{skipped}`\n"
        + (f"❌ Errors  : `{errors}`\n" if errors else ""),
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════════════════════
#  FREE-TEXT HANDLER
# ═══════════════════════════════════════════════════════════════
async def msg_text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle free-text: broadcast / channel ID / rename / add-course."""
    if update.effective_user.id != OWNER_ID:
        return

    # ── Awaiting broadcast message ──────────────────────────────
    if ctx.user_data.get("awaiting_broadcast"):
        ctx.user_data.pop("awaiting_broadcast")
        text = update.message.text.strip()
        reply = await update.message.reply_text("📡 Broadcasting…")
        await _do_broadcast(reply.edit_text, ctx.application, text)
        return

    # ── Awaiting channel ID ─────────────────────────────────────
    if ctx.user_data.get("awaiting_channel"):
        platform, course_id = ctx.user_data.pop("awaiting_channel")
        raw = update.message.text.strip()
        try:
            channel_id = int(raw)
        except ValueError:
            await update.message.reply_text("❌ Must be a number like `-1001234567890`.")
            return
        db_upsert_batch(platform, course_id, channel_id=channel_id)
        cfg = pcfg(platform)
        await update.message.reply_text(
            f"✅ Channel `{channel_id}` linked to *{cfg['label']}* course `{course_id}`.",
            parse_mode="Markdown",
        )
        return

    # ── Awaiting rename ─────────────────────────────────────────
    if ctx.user_data.get("awaiting_rename"):
        platform, course_id = ctx.user_data.pop("awaiting_rename")
        new_name = update.message.text.strip()
        if not new_name:
            await update.message.reply_text("❌ Name cannot be empty.")
            return
        db_rename_batch(platform, course_id, new_name)
        await update.message.reply_text(
            f"✅ Course `{course_id}` renamed to *{new_name}*.",
            parse_mode="Markdown",
        )
        return

    # ── Awaiting add-course ─────────────────────────────────────
    if ctx.user_data.get("awaiting_addcourse"):
        platform = ctx.user_data.pop("awaiting_addcourse")
        raw = update.message.text.strip()
        parts = raw.split(maxsplit=1)
        try:
            course_id = int(parts[0])
        except (ValueError, IndexError):
            await update.message.reply_text("❌ First word must be the course ID number.")
            return
        name = parts[1].strip() if len(parts) > 1 else None
        db_upsert_batch(platform, course_id, name=name)
        cfg = pcfg(platform)
        display = name or "Untitled"
        await update.message.reply_text(
            f"✅ Course `{course_id}` (*{display}*) added to *{cfg['label']}* DB.\n"
            f"Use /start → pick platform to manage it.",
            parse_mode="Markdown",
        )
        return


# ═══════════════════════════════════════════════════════════════
#  DIRECT API – ALL-CONTENT  (POST to course.nexttoppers.com)
# ═══════════════════════════════════════════════════════════════
async def fetch_all_content(
    session: aiohttp.ClientSession,
    platform: str,
    course_id: int,
    folder_id: int = 0,
) -> list:
    """
    POST /course/all-content with the correct body.
    Returns the list of items from data[], or [].
    """
    body = {
        "course_id": str(course_id),
        "folder_id": str(folder_id),
        "is_free": "",
        "keyword": "",
        "limit": "1000",
        "page": "1",
        "parent_course_id": "0",
    }
    try:
        resp = await _direct_post(session, platform, "/all-content", body)
        return resp.get("data") or []
    except Exception as e:
        log.warning(f"[{platform}] fetch_all_content course={course_id} folder={folder_id}: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
#  DIRECT API – CONTENT-DETAILS  (Delta Study API)
# ═══════════════════════════════════════════════════════════════
async def fetch_content_details_direct(
    session: aiohttp.ClientSession,
    platform: str,
    content_id: int,
    course_id: int,
    folder_id: int = 0,
) -> dict | None:
    """
    Fetch content details via deltastudy.site API.

    NT  → /api/nexttoppers/content-details?content_id=X&courseid=Y
          then /api/nexttoppers/video-details?videoid=<vdc_id>  if video.

    MJ  → /api/missionjeet/content-details?content_id=X&course_id=Y
          then /api/missionjeet/video-details?content_id=X&course_id=Y&folder_id=Z  if video.

    Returns the enriched data dict or None.
    """
    try:
        # ── Fetch content-details (platform-aware) ───────────────────
        if platform == "mj":
            detail = await fetch_mj_content_details(session, content_id, course_id)
        else:
            detail = await fetch_delta_content_details(session, content_id, course_id)

        if detail is None:
            log.warning(
                f"[{platform}] Delta content-details returned nothing for content_id={content_id}"
            )
            return None

        vdc_id = (detail.get("vdc_id") or "").strip()
        file_type = detail.get("file_type")

        if vdc_id:
            # ── Video: resolve real HLS URL via video-details (platform-aware) ──
            log.debug(
                f"[{platform}] content_id={content_id} has vdc_id={vdc_id!r} — "
                "fetching video-details"
            )
            if platform == "mj":
                video_data = await fetch_mj_video_details(
                    session, content_id, course_id, folder_id
                )
            else:
                video_data = await fetch_delta_video_details(session, vdc_id)

            if video_data:
                hls_url = (video_data.get("file_url") or "").strip()
                if hls_url:
                    detail["file_url"] = hls_url
                    log.debug(
                        f"[{platform}] content_id={content_id} video URL resolved: {hls_url[:60]}…"
                    )
                else:
                    log.warning(
                        f"[{platform}] content_id={content_id}: "
                        "video-details returned no file_url — will retry next run"
                    )
            else:
                log.warning(
                    f"[{platform}] content_id={content_id}: "
                    "video-details returned no data — will retry next run"
                )
        else:
            # ── PDF: file_url is already in the response ─────────────
            file_url = (detail.get("file_url") or "").strip()
            log.debug(
                f"[{platform}] content_id={content_id} PDF file_type={file_type} "
                f"file_url={'yes' if file_url else 'NO'}"
            )

        return detail

    except Exception as e:
        log.warning(f"[{platform}] content-details failed content_id={content_id}: {e}")
        return None


# ── LEGACY STUB kept so nothing else breaks ─────────────────────
# (was: fetch_content_details_direct using course.nexttoppers.com + decrypt.py)
# The body below is intentionally unreachable; real logic is above.
def _LEGACY_fetch_content_details_direct_UNREACHABLE(  # noqa: N802
    session, platform, content_id, course_id,
):  # pragma: no cover
    """
    Old implementation that hit course.nexttoppers.com/course/content-details
    and passed the encrypted response through decrypt.py.
    Kept here as a reference for the decrypt.py integration.
    """
    # (body omitted — superseded by Delta API above)
    pass


# ═══════════════════════════════════════════════════════════════
#  RECURSIVE FOLDER TRAVERSAL  –  uses direct API now
# ═══════════════════════════════════════════════════════════════
async def fetch_files_recursive(
    session, platform, course_id, folder_id=0, _depth=0
):
    """
    Recursively walk the course content tree via direct API calls.
    folder_id=0 for the root; recurse with the entity_id of each folder.
    """
    if _depth > 15:
        log.warning(f"[{platform}] Max recursion depth hit for course {course_id} folder {folder_id}")
        return []

    items = await fetch_all_content(session, platform, course_id, folder_id)
    log.debug(f"[{platform}] depth={_depth} folder={folder_id} → {len(items)} item(s)")

    result = []
    for item in items:
        itype = (item.get("type") or "").lower().strip()
        entity_id = item.get("entity_id")

        if itype == "folder":
            if not entity_id:
                log.warning(f"[{platform}] folder item has no entity_id, skipping: {item}")
                continue
            log.debug(f"[{platform}] descending folder entity_id={entity_id} depth={_depth}")
            sub = await fetch_files_recursive(
                session, platform, course_id, entity_id, _depth=_depth + 1
            )
            result.extend(sub)

        elif itype == "file":
            if not entity_id:
                log.warning(f"[{platform}] file item has no entity_id, skipping: {item}")
                continue
            result.append(item)

        else:
            # Unknown / missing type
            if not entity_id:
                log.debug(f"[{platform}] item has no type and no entity_id, skipping: {item}")
                continue

            inline_data = item.get("data") or {}
            has_file_url = bool((inline_data.get("file_url") or "").strip())

            if has_file_url:
                log.debug(f"[{platform}] unknown-type entity_id={entity_id} has inline file_url → file")
                result.append(item)
            else:
                log.debug(f"[{platform}] unknown-type entity_id={entity_id} → attempting folder recursion")
                sub = await fetch_files_recursive(
                    session, platform, course_id, entity_id, _depth=_depth + 1
                )
                if sub:
                    result.extend(sub)
                else:
                    log.debug(f"[{platform}] entity_id={entity_id} yielded no children → treating as file")
                    result.append(item)

    log.info(f"[{platform}] depth={_depth} folder={folder_id} → {len(result)} file(s) after recursion")
    return result


# ═══════════════════════════════════════════════════════════════
#  CONTENT DETAIL RESOLVER
# ═══════════════════════════════════════════════════════════════
async def get_content_detail(
    session, platform, content_id, course_id,
    inline_data: dict | None = None,
    folder_id: int = 0,
):
    """
    Return the content detail dict for a file item.

    Routing logic (based on content_type in the inline all-content data):

    • content_type == 1  (PDF / file):
        NT & MJ → call platform-appropriate content-details endpoint.
        file_url is already in the response.

    • content_type == 2  (Video):
        Skip content-details entirely and go straight to video-details.

        NT  → fetch_delta_video_details(vdc_id)
              vdc_id comes from inline_data["vdc_id"].

        MJ  → fetch_mj_video_details(content_id, course_id, folder_id)
              folder_id comes from the item's parent_id in the all-content list.

        Fields not returned by video-details are filled from inline_data so the
        returned dict has the same shape as a content-details response.

    • content_type unknown / inline_data missing:
        Fall back to the platform-appropriate content-details path (safe default).
    """
    inline = inline_data or {}
    content_type = inline.get("content_type")

    # ── content_type 2: Video — skip content-details, go straight to video-details ──
    if content_type == 2:
        if platform == "mj":
            # MJ: video-details takes content_id, course_id, folder_id (parent_id)
            log.debug(
                f"[mj] content_id={content_id} content_type=2 — "
                f"fetching MJ video-details directly (folder_id={folder_id})"
            )
            video_data = await fetch_mj_video_details(
                session, content_id, course_id, folder_id
            )
            if video_data is None:
                log.warning(
                    f"[mj] content_id={content_id}: MJ video-details failed — "
                    "falling back to MJ content-details"
                )
                return await fetch_content_details_direct(
                    session, platform, content_id, course_id, folder_id
                )
        else:
            # NT: video-details takes vdc_id
            vdc_id = (inline.get("vdc_id") or "").strip()
            if not vdc_id:
                log.warning(
                    f"[{platform}] content_id={content_id} content_type=2 but no vdc_id in "
                    "inline data — falling back to content-details"
                )
                return await fetch_content_details_direct(
                    session, platform, content_id, course_id, folder_id
                )
            log.debug(
                f"[{platform}] content_id={content_id} content_type=2 vdc_id={vdc_id!r} — "
                "fetching NT video-details directly (skipping content-details)"
            )
            video_data = await fetch_delta_video_details(session, vdc_id)
            if video_data is None:
                log.warning(
                    f"[{platform}] content_id={content_id} vdc_id={vdc_id!r}: "
                    "video-details failed — falling back to content-details"
                )
                return await fetch_content_details_direct(
                    session, platform, content_id, course_id, folder_id
                )

        hls_url = (video_data.get("file_url") or "").strip()
        if not hls_url:
            log.warning(
                f"[{platform}] content_id={content_id}: "
                "video-details returned no file_url — will retry next run"
            )
            # Return merged dict anyway; post_content will skip it (no file_url).

        vdc_id_val = inline.get("vdc_id", "") if platform != "mj" else ""

        # Build a detail dict in the same shape as content-details would return,
        # preferring video_data values but filling gaps from inline all-content data.
        detail = {
            "id":               inline.get("id", content_id),
            "vdc_id":           vdc_id_val,
            "file_type":        inline.get("file_type", 2),
            "content_type":     2,
            "file_url":         hls_url,
            "thumbnail":        video_data.get("thumbnail") or inline.get("thumbnail", ""),
            "duration":         video_data.get("duration") or inline.get("duration", 0),
            "is_drm":           inline.get("is_drm", 0),
            "is_live":          inline.get("is_live", 0),
            "is_download":      inline.get("is_download", 0),
            "download_urls":    inline.get("download_urls"),
            "video_type":       inline.get("video_type", 0),
            "in_app":           inline.get("in_app", 0),
            "is_share":         inline.get("is_share", 0),
            "has_pdf":          inline.get("has_pdf", "0"),
            "description":      inline.get("description"),
            "is_locked":        inline.get("is_locked", 0),
            "is_purchased":     inline.get("is_purchased", 0),
            "dynamic_link":     inline.get("dynamic_link", ""),
            "mark_as_complete": inline.get("mark_as_complete", 0),
            "rating":           inline.get("rating", "0.0"),
        }
        log.debug(
            f"[{platform}] content_id={content_id} video-details merged; "
            f"file_url={'yes' if hls_url else 'NO'}"
        )
        return detail

    # ── content_type 1 (PDF) or unknown: use platform-appropriate content-details ──
    if content_type == 1:
        log.debug(
            f"[{platform}] content_id={content_id} content_type=1 (PDF) — "
            "fetching content-details from Delta"
        )
    else:
        log.debug(
            f"[{platform}] content_id={content_id} content_type={content_type!r} (unknown) — "
            "falling back to content-details from Delta"
        )

    return await fetch_content_details_direct(
        session, platform, content_id, course_id, folder_id
    )


# ═══════════════════════════════════════════════════════════════
#  POST CONTENT TO CHANNEL
# ═══════════════════════════════════════════════════════════════
async def post_content(app: Application, platform, channel_id, detail, title):
    file_url = (detail.get("file_url") or "").strip()
    file_type = detail.get("file_type")
    thumb = (detail.get("thumbnail") or "").strip()
    duration = detail.get("duration")

    if not file_url:
        return False

    # Delta API: videos have a non-empty vdc_id; PDFs have file_type == 1.
    # Fall back to file_type == 2 / video_type for any legacy data.
    is_video = (
        bool((detail.get("vdc_id") or "").strip())
        or (file_type == 2)
        or bool(detail.get("video_type"))
    )

    # ── Skip videos with zero / missing duration (not yet processed by platform) ──
    if is_video:
        try:
            dur_secs = int(duration or 0)
        except (ValueError, TypeError):
            dur_secs = 0
        if dur_secs <= 0:
            log.info(
                f"[{platform}] Video content_id={detail.get('id')} has no duration ({duration!r}); "
                "skipping post and DB mark — will retry next run."
            )
            return False  # signals "not posted, do not mark sent, retry later"

    open_url = make_player_url(file_url) if is_video else file_url
    tag = "🎬 Video" if is_video else "📄 PDF"

    dur_txt = ""
    if is_video and duration:
        try:
            s = int(duration)
            if s > 0:
                dur_txt = f" • {s // 60}m {s % 60}s"
        except (ValueError, TypeError):
            pass

    cfg = pcfg(platform)
    caption = (
        f"{tag}{dur_txt}\n"
        f"<b>{title}</b>\n"
        f"<i>{cfg['label']}</i>"
        f"{WATERMARK}"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Open", url=open_url)]])

    try:
        if thumb and thumb.startswith("http"):
            await app.bot.send_photo(
                chat_id=channel_id,
                photo=thumb,
                caption=caption,
                reply_markup=kb,
                parse_mode="HTML",
            )
        else:
            await app.bot.send_message(
                chat_id=channel_id, text=caption, reply_markup=kb, parse_mode="HTML"
            )
    except Exception as e:
        log.error(f"[{platform}] post failed → channel {channel_id}: {e}")
        raise

    return True


def _progress_bar(done: int, total: int, width: int = 16) -> str:
    """Return a text progress bar like [████░░░░] 4/10."""
    if total == 0:
        return f"[{'░' * width}] 0/0"
    filled = int(width * done / total)
    bar = "█" * filled + "░" * (width - filled)
    pct = int(100 * done / total)
    return f"[{bar}] {done}/{total} ({pct}%)"


# ═══════════════════════════════════════════════════════════════
#  SCHEDULED DAILY CHECK + LOGS
# ═══════════════════════════════════════════════════════════════
async def check_and_post(app: Application):
    log.info("=== Daily content check started ===")

    # ── Fetch fresh auth tokens for this update session ──────────
    log.info("Fetching auth tokens for daily check session…")
    await fetch_auth_tokens()

    batches = db_get_all_active_batches()
    if not batches:
        log.info("No active batches.")
        return

    results = []
    async with aiohttp.ClientSession() as session:
        for batch in batches:
            platform = batch["platform"]
            course_id = batch["course_id"]
            channel_id = batch["channel_id"]
            cfg = pcfg(platform)
            posted = skipped = 0
            err_msgs = []

            log.info(f"[{platform}] Scanning course {course_id} → {channel_id}")
            try:
                files = await fetch_files_recursive(session, platform, course_id)
                total_files = len(files)
                log.info(f"[{platform}] daily check: found {total_files} file(s) for course {course_id}")

                # ── Progress tracking ────────────────────────────
                processed = 0
                last_progress_log = asyncio.get_event_loop().time()

                for f in files:
                    content_id = f["entity_id"]
                    processed += 1

                    # Log progress every 10 seconds
                    now = asyncio.get_event_loop().time()
                    if now - last_progress_log >= 10:
                        bar = _progress_bar(processed, total_files)
                        log.info(
                            f"[{platform}] daily check course {course_id} progress: "
                            f"{bar} | posted={posted} skipped={skipped}"
                        )
                        last_progress_log = now

                    if db_is_sent(platform, course_id, content_id):
                        skipped += 1
                        continue

                    detail = await get_content_detail(
                        session, platform, content_id, course_id,
                        inline_data=f.get("data"),
                        folder_id=int(f.get("parent_id") or 0),
                    )
                    if detail is None:
                        log.warning(
                            f"[{platform}] No detail for content_id={content_id}, will retry next run."
                        )
                        skipped += 1
                        continue

                    file_url = (detail.get("file_url") or "").strip()
                    if not file_url:
                        log.info(f"[{platform}] content_id={content_id} has no file_url (locked?), skipping for retry.")
                        skipped += 1
                        continue

                    try:
                        ok = await post_content(app, platform, channel_id, detail, f["title"])
                        if ok:
                            db_mark_sent(platform, course_id, content_id)
                            posted += 1
                            await asyncio.sleep(1.2)
                        else:
                            skipped += 1
                    except Exception as e:
                        err_msgs.append(str(e))

                log.info(
                    f"[{platform}] daily check course {course_id} done: "
                    f"{_progress_bar(processed, total_files)} | posted={posted} skipped={skipped}"
                )

            except Exception as e:
                err_msgs.append(f"Scan error: {e}")

            results.append({
                "platform": platform,
                "course_id": course_id,
                "channel_id": channel_id,
                "posted": posted,
                "skipped": skipped,
                "errors": err_msgs,
            })
            log.info(f"[{platform}] Course {course_id}: {posted} posted, {len(err_msgs)} errors.")

    # ── Send daily log to owner ──────────────────────────────────
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"📋 *Daily Update Log*",
        f"🕛 {now}",
        f"{'─' * 30}",
    ]
    for r in results:
        cfg = pcfg(r["platform"])
        icon = "✅" if not r["errors"] else "⚠️"
        lines.append(
            f"{icon} {cfg['emoji']} Course `{r['course_id']}` → `{r['channel_id']}`\n"
            f"   📤 {r['posted']} posted  |  ⏭ {r['skipped']} skipped"
        )
        for err in r["errors"]:
            lines.append(f"   ❌ `{err[:80]}`")

    total_posted = sum(r["posted"] for r in results)
    total_errors = sum(len(r["errors"]) for r in results)
    lines += [
        f"{'─' * 30}",
        f"📦 Batches : `{len(results)}`  |  📤 Posted : `{total_posted}`  |  ❌ Errors : `{total_errors}`",
    ]

    try:
        await app.bot.send_message(
            chat_id=OWNER_ID,
            text="\n".join(lines),
            parse_mode="Markdown",
        )
    except Exception as e:
        log.error(f"Failed to send daily log to owner: {e}")


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    start_flask()
    db_init()

    app = Application.builder().token(BOT_TOKEN).build()

    # ── Commands ────────────────────────────────────────────────
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("batches", cmd_batches))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("forceall", cmd_forceall))
    app.add_handler(CommandHandler("backup", cmd_backup))

    # ── Callbacks ───────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_noop,             pattern=r"^noop$"))
    app.add_handler(CallbackQueryHandler(cb_pick,             pattern=r"^pick:(nt|mj)$"))
    app.add_handler(CallbackQueryHandler(cb_back,             pattern=r"^back:(nt|mj)$"))
    app.add_handler(CallbackQueryHandler(cb_course,           pattern=r"^course:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_setchan,          pattern=r"^setchan:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_rmchan,           pattern=r"^rmchan:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_pause,            pattern=r"^pause:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_resume,           pattern=r"^resume:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_rst_confirm,      pattern=r"^rst_confirm:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_restart,          pattern=r"^restart:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_forceupdate,      pattern=r"^forceupdate:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_broadcast_prompt, pattern=r"^broadcast_prompt$"))
    app.add_handler(CallbackQueryHandler(cb_forceall_confirm, pattern=r"^forceall_confirm$"))
    app.add_handler(CallbackQueryHandler(cb_forceall_go,      pattern=r"^forceall_go$"))
    app.add_handler(CallbackQueryHandler(cb_cancel_action,    pattern=r"^cancel_action$"))
    app.add_handler(CallbackQueryHandler(cb_backup_now,       pattern=r"^backup_now$"))
    # New handlers
    app.add_handler(CallbackQueryHandler(cb_addcourse,        pattern=r"^addcourse:(nt|mj)$"))
    app.add_handler(CallbackQueryHandler(cb_rename,           pattern=r"^rename:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_del_confirm,      pattern=r"^del_confirm:(nt|mj):\d+$"))
    app.add_handler(CallbackQueryHandler(cb_delcourse,        pattern=r"^delcourse:(nt|mj):\d+$"))

    # ── Free-text handler ───────────────────────────────────────
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg_text_handler))

    # ── DB keep-alive ping every 2 hours ────────────────────────
    app.job_queue.run_repeating(
        lambda ctx: db_ping(),
        interval=7200,
        first=60,
    )

    # ── Daily content check at midnight UTC ─────────────────────
    app.job_queue.run_daily(
        lambda ctx: asyncio.create_task(check_and_post(app)),
        time=datetime.time(hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc),
    )

    # ── Nightly DB backup at 00:05 UTC ──────────────────────────
    app.job_queue.run_daily(
        lambda ctx: asyncio.create_task(_do_db_backup(app)),
        time=datetime.time(hour=0, minute=5, second=0, tzinfo=datetime.timezone.utc),
    )

    log.info("Bot polling started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
