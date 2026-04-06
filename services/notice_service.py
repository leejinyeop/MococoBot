# back/services/notice_service.py
import os, json, hashlib
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
import asyncio
import logging
import httpx

from database.connection import get_db
from services.delivery_utils import dispatch_embed, log_delivery as log_delivery_common, log_delivery_bulk as log_delivery_bulk_common
from utils.http_client import get_http_client

logger = logging.getLogger(__name__)

API_URL = "https://developer-lostark.game.onstove.com/news/notices"
API_KEYS = ["LOSTARK_API_SUB1_KEY", "LOSTARK_API_SUB2_KEY"]
DISPATCH_BATCH_SIZE = max(1, int(os.getenv("NOTICE_DISPATCH_BATCH_SIZE", "20")))
DELIVERY_LOG_BULK_CHUNK = max(25, int(os.getenv("NOTICE_DELIVERY_LOG_BULK_CHUNK", "200")))

def _auth_headers():
    for k in API_KEYS:
        v = os.getenv(k)
        if v:
            yield {"authorization": f"bearer {v}", "accept": "application/json"}

async def fetch_notice_json() -> Optional[list]:
    headers_list = list(_auth_headers())
    if not headers_list:
        logger.error("Lost Ark notice API keys not configured")
        return None

    last_err: Optional[Exception] = None

    client = await get_http_client()
    for headers in headers_list:
        for _ in range(3):
            try:
                r = await client.get(API_URL, headers=headers, timeout=10.0)
            except httpx.RequestError as e:
                last_err = e
                await asyncio.sleep(2)
                continue

            if 500 <= r.status_code < 600:
                last_err = httpx.HTTPStatusError(
                    f"{r.status_code} server error",
                    request=r.request,
                    response=r,
                )
                await asyncio.sleep(2)
                continue

            try:
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if 400 <= status < 500:
                    logger.error("Lost Ark notice API client error %s: %s", status, e)
                    return None
                last_err = e
                await asyncio.sleep(2)
                continue

            try:
                data = r.json()
            except ValueError as e:
                last_err = e
                await asyncio.sleep(2)
                continue

            if isinstance(data, list):
                return data
            return None

    if last_err:
        logger.warning("Lost Ark notice fetch failed after retries: %r", last_err)
    return None

def _link_key(link: str) -> str:
    return link.strip()

def _payload_fingerprint(payload: list) -> str:
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

async def _get_last_cache_fingerprint() -> Optional[str]:
    async with get_db() as db:
        row = await db.fetch_one("SELECT fingerprint FROM notice_cache ORDER BY id DESC LIMIT 1")
        if not row:
            return None
        return row.get("fingerprint")

async def save_notice_cache_if_changed(payload: list) -> bool:
    fp = _payload_fingerprint(payload)
    last_fp = await _get_last_cache_fingerprint()
    if last_fp == fp:
        return False
    async with get_db() as db:
        await db.execute(
            "INSERT INTO notice_cache (fetched_at, payload_json, fingerprint) VALUES (NOW(), ?, ?)",
            (json.dumps(payload, ensure_ascii=False), fp)
        )
        await db.commit()
    return True

async def save_notice_cache(payload: list) -> int:
    return 1 if await save_notice_cache_if_changed(payload) else 0

async def _load_last_state():
    async with get_db() as db:
        row = await db.fetch_one("SELECT last_seen_link, last_seen_date FROM notice_state WHERE id=1")
        if not row:
            return None, None
        return row.get("last_seen_link"), row.get("last_seen_date")

async def _save_last_state(last_link: str, last_date: datetime):
    async with get_db() as db:
        await db.execute(
            "INSERT INTO notice_state (id, last_seen_link, last_seen_date, updated_at) VALUES (1, ?, ?, NOW()) "
            "ON DUPLICATE KEY UPDATE last_seen_link=VALUES(last_seen_link), last_seen_date=VALUES(last_seen_date), updated_at=NOW()",
            (last_link, last_date)
        )
        await db.commit()

def _parse_iso(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")

def _sort_key(item: dict):
    dt = _parse_iso(item["Date"])
    return dt, _link_key(item["Link"])

def _build_embed(items: list[dict]) -> dict:
    if not items:
        return {"embeds": []}
    lines = []
    for e in items:
        title = e.get("Title") or ""
        link = e.get("Link") or ""
        dt = _parse_iso(e.get("Date"))
        ts = dt.strftime("%Y-%m-%d %H:%M")
        lines.append(f"[{title}]({link}) • {ts}")
    desc = "\n".join(lines)
    footer = {"text": "해당 알림은 /구독 명령어로 설정 가능해요."}
    return {
        "embeds": [{
            "title": "📢 새로운 공지사항이 도착했어요!",
            "description": desc,
            "footer": footer
        }]
    }

async def _select_new_items(payload: list) -> list:
    last_link, last_date = await _load_last_state()
    if last_link is None and last_date is None:
        return []
    new_items = []
    for e in payload:
        link = _link_key(e.get("Link", ""))
        dt = _parse_iso(e.get("Date"))
        if last_link and link == last_link:
            break
        if last_date:
            if dt > last_date:
                new_items.append(e)
        else:
            new_items.append(e)
    new_items.sort(key=_sort_key)
    return new_items

def _select_initial_head(payload: list) -> list:
    return payload[:1]


async def log_delivery_notice(user_id: int | None, guild_id: int | None, channel_id: int | None):
    await log_delivery_common("notice", user_id, guild_id, channel_id, logger)


async def log_delivery_notice_bulk(
    user_ids: List[int],
    channel_targets: List[Tuple[int, int]],
) -> None:
    await log_delivery_bulk_common("notice", user_ids, channel_targets, logger, DELIVERY_LOG_BULK_CHUNK)


async def run_notice_fetch_and_dispatch(initial: bool = False):
    try:
        payload = await fetch_notice_json()
    except Exception as e:
        logger.exception("Unexpected error while fetching Lost Ark notices: %r", e)
        return

    if not payload:
        return

    await save_notice_cache_if_changed(payload)

    items = _select_initial_head(payload) if initial else await _select_new_items(payload)

    if not items and payload:
        last_link, last_date = await _load_last_state()
        if last_link is None and last_date is None:
            last = max(payload, key=_sort_key)
            await _save_last_state(_link_key(last["Link"]), _parse_iso(last["Date"]))
            return

    if not items:
        if initial and payload:
            last = max(payload, key=_sort_key)
            await _save_last_state(_link_key(last["Link"]), _parse_iso(last["Date"]))
        return

    embed = _build_embed(items)["embeds"][0]

    async with get_db() as db:
        dm_rows = await db.execute("SELECT user_id FROM user_subscriptions WHERE type='notice' AND enabled=1") or []
        ch_rows = await db.execute("SELECT guild_id, channel_id FROM guild_channel_subscriptions WHERE type='notice' AND enabled=1") or []
        user_targets = [int(r["user_id"]) for r in dm_rows]
        channel_targets = [(int(r["guild_id"]), int(r["channel_id"])) for r in ch_rows]

    delivered_users, delivered_channels = await dispatch_embed(embed, user_targets, channel_targets, DISPATCH_BATCH_SIZE)

    await log_delivery_notice_bulk(delivered_users, delivered_channels)

    last = max(items, key=_sort_key)
    await _save_last_state(_link_key(last["Link"]), _parse_iso(last["Date"]))

async def purge_notice_cache(days: int = 14) -> int:
    async with get_db() as db:
        await db.execute(
            "DELETE FROM notice_cache WHERE fetched_at < DATE_SUB(NOW(), INTERVAL ? DAY)",
            (int(days),),
        )
        await db.commit()
        try:
            return int(getattr(db, "rowcount", 0))
        except Exception:
            return 0
