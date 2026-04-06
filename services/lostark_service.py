import os
import urllib.parse
import asyncio
import time
from typing import Any, Dict, Optional

from utils.http_client import get_http_client


LOSTARK_API_KEY = os.getenv("LOSTARK_API_KEY")
LOSTARK_API_BASE = "https://developer-lostark.game.onstove.com"
_CACHE_TTL_SEC = float(os.getenv("LOSTARK_CHAR_CACHE_TTL_SEC", "15"))
_CACHE_MAX_SIZE = int(os.getenv("LOSTARK_CHAR_CACHE_MAX", "1024"))
_char_cache: dict[str, tuple[float, Optional[Dict[str, Any]]]] = {}
_inflight: dict[str, asyncio.Task[Optional[Dict[str, Any]]]] = {}
_cache_lock = asyncio.Lock()


def _parse_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return default


async def _fetch_lostark_character(char_name: str) -> Optional[Dict[str, Any]]:
    try:
        client = await get_http_client()
        response = await client.get(
            f"{LOSTARK_API_BASE}/armories/characters/{urllib.parse.quote(char_name)}/profiles",
            headers={"Authorization": f"Bearer {LOSTARK_API_KEY}"},
            timeout=15.0,
        )

        if response.status_code != 200:
            return None

        data = response.json()

        return {
            "char_name": data.get("CharacterName", char_name),
            "class_name": data.get("CharacterClassName", ""),
            "server_name": data.get("ServerName", ""),
            "guild_name": data.get("GuildName", ""),
            "char_image": data.get("CharacterImage", ""),
            "item_lvl": _parse_float(data.get("ItemAvgLevel")),
            "combat_power": _parse_float(data.get("CombatPower")),
        }
    except Exception:
        return None


def _prune_cache(now_mono: float) -> None:
    expired = [k for k, (exp, _) in _char_cache.items() if exp <= now_mono]
    for k in expired:
        _char_cache.pop(k, None)

    if len(_char_cache) <= _CACHE_MAX_SIZE:
        return

    overflow = len(_char_cache) - _CACHE_MAX_SIZE
    for k, _ in sorted(_char_cache.items(), key=lambda kv: kv[1][0])[:overflow]:
        _char_cache.pop(k, None)


async def search_lostark_character(char_name: str) -> Optional[Dict[str, Any]]:
    key = (char_name or "").strip().lower()
    if not key:
        return None

    now_mono = time.monotonic()
    async with _cache_lock:
        _prune_cache(now_mono)
        cached = _char_cache.get(key)
        if cached and cached[0] > now_mono:
            data = cached[1]
            return dict(data) if isinstance(data, dict) else data

        inflight = _inflight.get(key)
        if inflight is None:
            inflight = asyncio.create_task(_fetch_lostark_character(char_name))
            _inflight[key] = inflight

    try:
        result = await inflight
    finally:
        async with _cache_lock:
            if _inflight.get(key) is inflight:
                _inflight.pop(key, None)

    async with _cache_lock:
        _char_cache[key] = (time.monotonic() + _CACHE_TTL_SEC, result)
        _prune_cache(time.monotonic())

    return dict(result) if isinstance(result, dict) else result
