from __future__ import annotations

import os, json, hashlib, xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import asyncio
import httpx
import logging

from database.connection import get_db
from services.delivery_utils import dispatch_embed, log_delivery as log_delivery_common, log_delivery_bulk as log_delivery_bulk_common
from utils.http_client import get_http_client

DEFAULT_CHANNEL_ID = "UCL3gnarNIeI_M0cFxjNYdAA"
YOUTUBE_FEED = "https://www.youtube.com/feeds/videos.xml?channel_id={cid}"
DISPATCH_BATCH_SIZE = max(1, int(os.getenv("YOUTUBE_DISPATCH_BATCH_SIZE", "20")))
DELIVERY_LOG_BULK_CHUNK = max(25, int(os.getenv("YOUTUBE_DELIVERY_LOG_BULK_CHUNK", "200")))
logger = logging.getLogger(__name__)

def _channel_id() -> str:
    v = os.getenv("YT_LOSTARK_CHANNEL_ID", "").strip()
    return v or DEFAULT_CHANNEL_ID

def _json_safe_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    safe: List[Dict[str, Any]] = []
    for e in entries:
        d = dict(e)
        if isinstance(d.get("published"), datetime):
            d["published"] = d["published"].isoformat()
        safe.append(d)
    return safe

def _payload_fingerprint(entries: List[Dict[str, Any]]) -> str:
    s = json.dumps(_json_safe_entries(entries), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

async def _get_last_cache_fingerprint() -> Optional[str]:
    async with get_db() as db:
        row = await db.fetch_one("SELECT fingerprint FROM youtube_cache ORDER BY id DESC LIMIT 1")
        if not row:
            return None
        return row.get("fingerprint")

async def save_youtube_cache_if_changed(entries: List[Dict[str, Any]]) -> bool:
    fp = _payload_fingerprint(entries)
    if await _get_last_cache_fingerprint() == fp:
        return False
    async with get_db() as db:
        await db.execute(
            "INSERT INTO youtube_cache (fetched_at, payload_json, fingerprint) VALUES (NOW(), ?, ?)",
            (json.dumps(_json_safe_entries(entries), ensure_ascii=False), fp)
        )
        await db.commit()
    return True

async def _load_last_state():
    async with get_db() as db:
        row = await db.fetch_one("SELECT last_video_id, last_published FROM youtube_state WHERE id=1")
        if not row:
            return None, None
        return row.get("last_video_id"), row.get("last_published")

async def _save_last_state(vid: str, published: datetime):
    async with get_db() as db:
        await db.execute(
            "INSERT INTO youtube_state (id, last_video_id, last_published, updated_at) VALUES (1, ?, ?, NOW()) "
            "ON DUPLICATE KEY UPDATE last_video_id=VALUES(last_video_id), last_published=VALUES(last_published), updated_at=NOW()",
            (vid, published)
        )
        await db.commit()

def _parse_yt_time(s: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

def _parse_atom_feed(xml: str) -> List[Dict[str, Any]]:
    ns = {"a": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(xml)
    out: List[Dict[str, Any]] = []
    for e in root.findall("a:entry", ns):
        id_text = (e.findtext("a:id", default="", namespaces=ns) or "").strip()
        video_id = id_text.rsplit(":", 1)[-1] if ":" in id_text else id_text.rsplit("/", 1)[-1]
        title = (e.findtext("a:title", default="", namespaces=ns) or "").strip()
        link_el = e.find("a:link", ns)
        href = link_el.get("href") if link_el is not None else ""
        link = href or (f"https://www.youtube.com/watch?v={video_id}" if video_id else "")
        published_raw = (e.findtext("a:published", default="", namespaces=ns) or "").strip()
        if not published_raw:
            published_raw = (e.findtext("a:updated", default="", namespaces=ns) or "").strip()
        published = _parse_yt_time(published_raw) if published_raw else datetime.utcnow()
        out.append({"video_id": video_id, "title": title, "link": link, "published": published})
    out.sort(key=lambda x: (x["published"], x["video_id"]), reverse=True)
    return out

def _build_embed(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not items:
        return {"embeds": []}
    lines = []
    for e in items:
        ts = e["published"].strftime("%Y-%m-%d %H:%M")
        lines.append(f"[{e['title']}]({e['link']}) • {ts}")
    return {"embeds": [{"title": "<:youtube:1445575537157476494> 새 영상이 올라왔어요!", "description": "\n".join(lines), "footer": {"text": "해당 알림은 /구독 명령어로 설정 가능해요."}}]}


async def fetch_feed_entries() -> List[Dict[str, Any]]:
    feed_url = YOUTUBE_FEED.format(cid=_channel_id())
    headers = {
        "user-agent": "Mozilla/5.0",
        "accept": "application/atom+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
    }
    client = await get_http_client()
    r = await client.get(feed_url, timeout=20, headers=headers)
    r.raise_for_status()
    return _parse_atom_feed(r.text)

async def _select_new(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    last_vid, last_pub = await _load_last_state()
    if last_vid is None and last_pub is None:
        return []
    new_items = []
    for e in entries:
        if last_vid and e["video_id"] == last_vid:
            break
        if last_pub:
            if e["published"] > last_pub:
                new_items.append(e)
        else:
            new_items.append(e)
    new_items.sort(key=lambda x: (x["published"], x["video_id"]))
    return new_items

async def log_delivery(user_id: int | None, guild_id: int | None, channel_id: int | None):
    await log_delivery_common("youtube", user_id, guild_id, channel_id, logger)


async def log_delivery_bulk(
    user_ids: List[int],
    channel_targets: List[Tuple[int, int]],
) -> None:
    await log_delivery_bulk_common("youtube", user_ids, channel_targets, logger, DELIVERY_LOG_BULK_CHUNK)


async def run_youtube_fetch_and_dispatch(initial: bool = False):
    try:
        entries = await fetch_feed_entries()
    except httpx.HTTPStatusError as e:
        status = getattr(e.response, "status_code", None)
        if status in (404, 429):
            return
        return
    except httpx.HTTPError:
        return

    await save_youtube_cache_if_changed(entries)
    items = entries[:1] if initial else await _select_new(entries)
    if not items and entries:
        last_vid, last_pub = await _load_last_state()
        if last_vid is None and last_pub is None:
            last = entries[0]
            await _save_last_state(last["video_id"], last["published"])
            return
    if not items:
        if initial and entries:
            last = entries[0]
            await _save_last_state(last["video_id"], last["published"])
        return
    embed = _build_embed(items)["embeds"][0]
    async with get_db() as db:
        dm_rows = await db.execute("SELECT user_id FROM user_subscriptions WHERE type='youtube' AND enabled=1") or []
        ch_rows = await db.execute("SELECT guild_id, channel_id FROM guild_channel_subscriptions WHERE type='youtube' AND enabled=1") or []
        user_targets = [int(r["user_id"]) for r in dm_rows]
        channel_targets = [(int(r["guild_id"]), int(r["channel_id"])) for r in ch_rows]

    delivered_users, delivered_channels = await dispatch_embed(embed, user_targets, channel_targets, DISPATCH_BATCH_SIZE)

    await log_delivery_bulk(delivered_users, delivered_channels)

    last = max(items, key=lambda x: (x["published"], x["video_id"]))
    await _save_last_state(last["video_id"], last["published"])

async def purge_youtube_cache(days: int = 14) -> int:
    async with get_db() as db:
        await db.execute(
            "DELETE FROM youtube_cache WHERE fetched_at < DATE_SUB(NOW(), INTERVAL ? DAY)",
            (int(days),),
        )
        await db.commit()
        try:
            return int(getattr(db, "rowcount", 0))
        except Exception:
            return 0
