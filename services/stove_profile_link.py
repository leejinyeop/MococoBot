import os
import re
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote, urlparse

import httpx


STOVE_PROFILE_API_URL_TMPL = "https://api.onstove.com/tm/v1/preferences/{member_no}"
UNOFFICIAL_STOVE_TO_LOSTARK_URL = "https://lostark.game.onstove.com/board/IscharacterList"
UNOFFICIAL_LOSTARK_MEMBER_URL = "https://lostark.game.onstove.com/Profile/Member"
LOSTARK_OPENAPI_BASE = "https://developer-lostark.game.onstove.com"
LOSTARK_UNAVAILABLE_DETAIL = "lostark_service_unavailable"


class StoveProfileError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _is_lostark_unavailable_status(status_code: int) -> bool:
    return int(status_code or 0) in {500, 502, 503, 504, 521, 522, 523, 524, 529}


def _raise_lostark_unavailable() -> None:
    raise StoveProfileError(LOSTARK_UNAVAILABLE_DETAIL, status_code=503)


@dataclass
class StoveProfileResolution:
    stove_profile_id: str
    stove_profile_bio: Optional[str]
    representative_character_name: str
    profile: Dict[str, Any]
    siblings: List[Dict[str, Any]]


def normalize_stove_profile_id(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        raise StoveProfileError("stove profile id is required", status_code=400)
    if raw.isdigit():
        return raw
    parsed = urlparse(raw)
    match = re.search(r"/(\d+)(?:/)?$", parsed.path or "")
    if match:
        return match.group(1)
    raise StoveProfileError("only stove_profile_id or profile.onstove.com url is supported", status_code=400)


def normalize_stove_profile_id_digits_only(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        raise StoveProfileError("stove profile id is required", status_code=400)
    if raw.isdigit():
        return raw

    parsed = urlparse(raw)
    host = (parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if parsed.scheme not in {"http", "https"} or host != "profile.onstove.com":
        raise StoveProfileError("invalid stove profile id or url", status_code=400)

    parts = [part for part in (parsed.path or "").split("/") if part]
    for part in reversed(parts):
        if part.isdigit():
            return part

    raise StoveProfileError("invalid stove profile id or url", status_code=400)


def _deep_find_first(obj: Any, keys: set[str]) -> Optional[str]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and isinstance(v, (str, int, float)) and str(v).strip():
                return str(v).strip()
        for v in obj.values():
            found = _deep_find_first(v, keys)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _deep_find_first(item, keys)
            if found:
                return found
    return None


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _extract_stove_intro(data: Any) -> Optional[str]:
    if not isinstance(data, dict):
        return None

    nested_data = data.get("data")
    if isinstance(nested_data, dict):
        direct = nested_data.get("introduce")
        if _is_non_empty_string(direct):
            return str(direct).strip()

        for key in ("introduction", "intro", "statusMessage", "status_message", "description"):
            value = nested_data.get(key)
            if _is_non_empty_string(value):
                return str(value).strip()

    direct = _deep_find_first(
        data,
        {
            "introduce",
            "introduction",
            "intro",
            "statusMessage",
            "status_message",
            "description",
            "profileMessage",
            "profile_message",
            "aboutMe",
            "about_me",
        },
    )
    if _is_non_empty_string(direct):
        return str(direct).strip()

    return None


def _stove_profile_headers(stove_profile_id: str) -> Dict[str, str]:
    profile_url = f"https://profile.onstove.com/ko/{stove_profile_id}"
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://profile.onstove.com",
        "Referer": profile_url,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "X-Lang": "ko",
        "X-Nation": "KR",
        "X-Timezone": "Asia/Seoul",
        "X-Utc-Offset": "540",
    }


async def _request_candidate_json(client: httpx.AsyncClient, method: str, url: str, **kwargs: Any) -> Optional[Any]:
    try:
        response = await client.request(method, url, **kwargs)
    except httpx.HTTPError:
        return None

    if response.status_code >= 400:
        return None

    try:
        return response.json()
    except ValueError:
        return None


async def _fetch_stove_profile_bio(client: httpx.AsyncClient, stove_profile_id: str) -> Optional[str]:
    api_url = STOVE_PROFILE_API_URL_TMPL.format(member_no=stove_profile_id)
    data = await _request_candidate_json(
        client,
        "GET",
        api_url,
        headers=_stove_profile_headers(stove_profile_id),
    )
    if not data:
        return None
    return _extract_stove_intro(data)


async def fetch_stove_status_message(
    value: str,
    retries: int = 0,
    retry_delay_sec: float = 0.0,
) -> Optional[str]:
    stove_profile_id = normalize_stove_profile_id(value)
    total_attempts = max(1, int(retries) + 1)

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for attempt in range(total_attempts):
            message = await _fetch_stove_profile_bio(client, stove_profile_id)
            if _is_non_empty_string(message):
                return str(message).strip()
            if attempt < (total_attempts - 1) and retry_delay_sec > 0:
                await asyncio.sleep(retry_delay_sec)
    return None


async def _resolve_encrypt_member_no(client: httpx.AsyncClient, stove_profile_id: str) -> str:
    candidates = [
        ("POST", UNOFFICIAL_STOVE_TO_LOSTARK_URL, {"data": {"memberNo": stove_profile_id}}),
        ("POST", UNOFFICIAL_STOVE_TO_LOSTARK_URL, {"json": {"memberNo": stove_profile_id}}),
        ("GET", UNOFFICIAL_STOVE_TO_LOSTARK_URL, {"params": {"memberNo": stove_profile_id}}),
    ]
    for method, url, kwargs in candidates:
        data = await _request_candidate_json(client, method, url, **kwargs)
        if not data:
            continue
        enc = _deep_find_first(data, {"encryptMemberNo", "encryptedMemberNo", "memberNoEnc", "memberNoEncrypt"})
        if enc:
            return enc
    raise StoveProfileError("failed to map stove_profile_id to member token", status_code=502)


def _extract_character_from_html(html: str) -> Optional[str]:
    patterns = [
        r'/Profile/Character/([^"\'?#]+)',
        r'charactername["\']?\s*[:=]\s*["\']([^"\']+)["\']',
        r'og:title["\']?\s+content=["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if not match:
            continue
        value = unquote(match.group(1)).strip()
        if value:
            return value
    return None


async def _resolve_representative_character(client: httpx.AsyncClient, encrypt_member_no: str) -> str:
    try:
        response = await client.get(UNOFFICIAL_LOSTARK_MEMBER_URL, params={"id": encrypt_member_no})
    except httpx.TimeoutException:
        _raise_lostark_unavailable()
    except httpx.HTTPError:
        _raise_lostark_unavailable()
    if _is_lostark_unavailable_status(response.status_code):
        _raise_lostark_unavailable()
    final_url = str(response.url)
    redirected = re.search(r"/Profile/Character/([^/?#]+)", final_url)
    if redirected:
        return unquote(redirected.group(1))
    from_html = _extract_character_from_html(response.text or "")
    if from_html:
        return from_html
    raise StoveProfileError("failed to resolve representative character", status_code=502)


def _lostark_api_token() -> str:
    token = (os.getenv("LOSTARK_API_KEY", "") or "").strip()
    if token:
        return token
    fallback = (os.getenv("LOSTARK_OPENAPI_TOKEN", "") or "").strip()
    if fallback:
        return fallback
    raise StoveProfileError("LOSTARK_API_KEY is not configured", status_code=500)


async def _call_lostark(client: httpx.AsyncClient, path: str) -> Any:
    try:
        response = await client.get(
            f"{LOSTARK_OPENAPI_BASE}{path}",
            headers={
                "accept": "application/json",
                "authorization": f"bearer {_lostark_api_token()}",
            },
        )
    except httpx.TimeoutException:
        _raise_lostark_unavailable()
    except httpx.HTTPError:
        _raise_lostark_unavailable()
    if response.status_code == 404:
        raise StoveProfileError("lost ark data not found", status_code=404)
    if _is_lostark_unavailable_status(response.status_code):
        _raise_lostark_unavailable()
    if response.status_code >= 400:
        raise StoveProfileError(
            f"lost ark open api request failed: {response.status_code}",
            status_code=502,
        )
    try:
        return response.json()
    except ValueError:
        _raise_lostark_unavailable()


def _to_item_level(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def build_top_characters(siblings: List[Dict[str, Any]], limit: Optional[int] = 0) -> List[Dict[str, Any]]:
    sorted_chars = sorted(
        siblings or [],
        key=lambda x: (-_to_item_level(x.get("ItemAvgLevel")), str(x.get("CharacterName") or "")),
    )
    out: List[Dict[str, Any]] = []
    if limit is None or int(limit) <= 0:
        selected = sorted_chars
    else:
        selected = sorted_chars[: int(limit)]
    for row in selected:
        out.append(
            {
                "character_name": row.get("CharacterName"),
                "character_class": row.get("CharacterClassName"),
                "server_name": row.get("ServerName"),
                "item_level": row.get("ItemAvgLevel"),
            }
        )
    return out


async def fetch_lostark_siblings_by_character_name(character_name: str) -> List[Dict[str, Any]]:
    name = str(character_name or "").strip()
    if not name:
        raise StoveProfileError("character_name is required", status_code=400)

    encoded_name = quote(name, safe="")
    async with httpx.AsyncClient(
        timeout=10.0,
        follow_redirects=True,
        headers={
            "user-agent": "Mozilla/5.0",
            "accept": "*/*",
            "referer": "https://lostark.game.onstove.com/",
        },
    ) as client:
        siblings = await _call_lostark(client, f"/characters/{encoded_name}/siblings")
        return siblings if isinstance(siblings, list) else []


async def resolve_stove_profile(value: str) -> StoveProfileResolution:
    stove_profile_id = normalize_stove_profile_id(value)
    async with httpx.AsyncClient(
        timeout=12.0,
        follow_redirects=True,
        headers={
            "user-agent": "Mozilla/5.0",
            "accept": "*/*",
            "referer": "https://lostark.game.onstove.com/",
        },
    ) as client:
        stove_profile_bio = await _fetch_stove_profile_bio(client, stove_profile_id)
        encrypt_member_no = await _resolve_encrypt_member_no(client, stove_profile_id)
        representative_character_name = await _resolve_representative_character(client, encrypt_member_no)
        encoded_name = quote(representative_character_name, safe="")
        profile = await _call_lostark(client, f"/armories/characters/{encoded_name}/profiles")
        siblings = await _call_lostark(client, f"/characters/{encoded_name}/siblings")
    return StoveProfileResolution(
        stove_profile_id=stove_profile_id,
        stove_profile_bio=stove_profile_bio,
        representative_character_name=representative_character_name,
        profile=profile if isinstance(profile, dict) else {},
        siblings=siblings if isinstance(siblings, list) else [],
    )