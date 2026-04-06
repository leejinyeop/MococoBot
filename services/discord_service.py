"""Discord API 통합 서비스"""
import httpx
import os
import re
import asyncio
import json as _json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
from database.connection import get_db
from dotenv import load_dotenv
from utils.http_client import get_http_client

load_dotenv()

_STARTER_DELETE_PATTERN = re.compile(r"^/channels/(?P<cid>\d+)/messages/(?P<mid>\d+)$")
logger = logging.getLogger(__name__)


def _build_http_error(resp: httpx.Response, *, stage: str, request_url: str, request_payload: dict | None) -> dict:
    body_text = None
    body_json = None
    try:
        body_json = resp.json()
    except Exception:
        try:
            t = resp.text
            body_text = t[:1000] + "...(truncated)" if len(t) > 1000 else t
        except Exception:
            body_text = None
    err = {
        "stage": stage,
        "status": resp.status_code,
        "reason": resp.reason_phrase,
        "request_url": request_url,
    }
    if request_payload is not None:
        try:
            import json as _json

            preview = _json.dumps(request_payload, ensure_ascii=False, default=str)
            err["request_preview"] = (
                preview[:800] + "...(truncated)" if len(preview) > 800 else preview
            )
        except Exception:
            pass
    if body_json is not None:
        err["response_json"] = {
            k: body_json.get(k) for k in ("message", "code", "errors") if k in body_json
        }
    elif body_text is not None:
        err["response_text"] = body_text
    if resp.status_code == 429:
        try:
            ra = body_json.get("retry_after") if isinstance(body_json, dict) else None
        except Exception:
            ra = None
        err["retry_after"] = ra or resp.headers.get("Retry-After")
    return {"_error": err}


class File:
    def __init__(self, fp, filename: str, content_type: str | None = None):
        self.fp = fp
        self.filename = filename
        self.content_type = content_type or "application/octet-stream"


class DiscordService:
    """Discord API 통합 서비스 클래스"""

    def __init__(self):
        self.bot_token = os.getenv("DISCORD_BOT_TOKEN")
        self.base_url = "https://discord.com/api/v10"
        self.client_id = os.getenv("DISCORD_CLIENT_ID")
        self.client_secret = os.getenv("DISCORD_CLIENT_SECRET")
        self._client: httpx.AsyncClient | None = None
        self._thumb_cache: dict = {}
        self._thumb_cache_ttl_sec = max(30, int(os.getenv("RAID_THUMB_CACHE_TTL_SEC", "900")))
        self._bot_headers = {
            "Authorization": f"Bot {self.bot_token}",
            "Content-Type": "application/json",
        }

    # --------------------------- 내부 유틸 ---------------------------
    async def _attach_raid_thumbnail(self, party_data: dict, embed: dict, thumb_url: str | None = None) -> dict:
        try:
            has_thumb = bool(
                isinstance(embed, dict) and embed.get("thumbnail", {}).get("url")
            )
            if not has_thumb:
                url = thumb_url or await self._resolve_raid_thumbnail_url(party_data)
                if url and isinstance(embed, dict):
                    embed["thumbnail"] = {"url": url}
        except Exception:
            pass
        return embed

    async def _resolve_raid_thumbnail_url(self, party_data: dict) -> str | None:
        try:
            raid_id = (
                party_data.get("raid_id") or (party_data.get("raid") or {}).get("id")
            )
            name = (
                party_data.get("raid_name") or (party_data.get("raid") or {}).get("name")
            )
            difficulty = (
                party_data.get("difficulty")
                or party_data.get("raid_difficulty")
                or (party_data.get("raid") or {}).get("difficulty")
            )

            key = (
                int(raid_id)
                if isinstance(raid_id, int)
                or (isinstance(raid_id, str) and raid_id.isdigit())
                else 0,
                str(name or ""),
                str(difficulty or ""),
            )
            cached = self._thumb_cache.get(key)
            if isinstance(cached, tuple) and len(cached) == 2:
                cached_url, expires_at = cached
                if float(expires_at) > asyncio.get_running_loop().time():
                    return cached_url
                self._thumb_cache.pop(key, None)

            async with get_db() as db:
                rows = None
                if raid_id:
                    rows = await db.execute(
                        "SELECT urls FROM raid WHERE id = ? LIMIT 1", (raid_id,)
                    )
                elif name and difficulty:
                    rows = await db.execute(
                        "SELECT urls FROM raid WHERE name = ? AND difficulty = ? LIMIT 1",
                        (name, difficulty),
                    )
                elif name:
                    rows = await db.execute(
                        "SELECT urls FROM raid WHERE name = ? LIMIT 1", (name,)
                    )

                url: str | None = None
                if rows and len(rows) > 0:
                    row0 = rows[0]
                    urls = (row0.get("urls") or "").strip() if isinstance(row0, dict) else None
                    if urls and urls.lower().startswith(("http://", "https://")):
                        url = urls

                if len(self._thumb_cache) >= 256:
                    try:
                        self._thumb_cache.pop(next(iter(self._thumb_cache)))
                    except Exception:
                        self._thumb_cache.clear()
                self._thumb_cache[key] = (
                    url,
                    asyncio.get_running_loop().time() + float(self._thumb_cache_ttl_sec),
                )
                return url
        except Exception:
            return None

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = await get_http_client()
        return self._client

    async def _make_bot_api_call(self, method: str, endpoint: str, **kwargs) -> httpx.Response:
        if method.upper() == "DELETE":
            m = _STARTER_DELETE_PATTERN.match(endpoint)
            if m and m.group("cid") == m.group("mid"):
                endpoint = f"/channels/{m.group('cid')}"

        url = f"{self.base_url}{endpoint}"
        client = await self._ensure_client()
        return await getattr(client, method.lower())(
            url,
            headers=self._bot_headers,
            **kwargs,
        )

    async def _make_bot_api_call_multipart(self, method: str, endpoint: str, **kwargs) -> httpx.Response:
        if method.upper() == "DELETE":
            m = _STARTER_DELETE_PATTERN.match(endpoint)
            if m and m.group("cid") == m.group("mid"):
                endpoint = f"/channels/{m.group('cid')}"

        url = f"{self.base_url}{endpoint}"
        client = await self._ensure_client()
        return await getattr(client, method.lower())(
            url,
            headers={"Authorization": f"Bot {self.bot_token}"},
            **kwargs,
        )

    async def _bot_api_call_with_retry(self, method: str, endpoint: str, *, max_retries: int = 3, retryable_status: tuple[int, ...] = (429, 500, 502, 503, 504), **kwargs) -> httpx.Response | None:
        resp: httpx.Response | None = None
        for attempt in range(max_retries):
            try:
                resp = await self._make_bot_api_call(method, endpoint, **kwargs)
            except httpx.RequestError:
                resp = None

            if resp is None:
                backoff = 2**attempt
                try:
                    await asyncio.sleep(backoff)
                except Exception:
                    pass
                continue

            status = resp.status_code
            if status not in retryable_status:
                return resp

            retry_after: float = 0.0
            if status == 429:
                try:
                    data = resp.json()
                    retry_after = float(data.get("retry_after", 0)) or 0.0
                except Exception:
                    retry_after = 0.0
                if retry_after == 0.0:
                    try:
                        retry_after = float(resp.headers.get("Retry-After", "0"))
                    except Exception:
                        retry_after = 0.0
            backoff = retry_after if retry_after > 0 else (2**attempt)
            try:
                await asyncio.sleep(backoff)
            except Exception:
                pass
        return resp

    async def _make_oauth_api_call(self, access_token: str, endpoint: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        try:
            client = await self._ensure_client()
            response = await client.get(
                f"{self.base_url}{endpoint}",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if response.status_code == 401:
                return {"error": "Access token invalid", "status_code": 401}, None
            elif response.status_code != 200:
                return {"error": "Discord API error", "status_code": 500}, None
            return None, response.json()
        except Exception as e:
            return {"error": str(e)}, None

    async def _ensure_forum_tag(self, forum_channel_id: str, tag_name: str) -> str | None:
        if not forum_channel_id or not tag_name:
            return None

        try:
            resp = await self._bot_api_call_with_retry(
                "GET",
                f"/channels/{forum_channel_id}",
            )
            if not resp or resp.status_code != 200:
                return None

            channel = resp.json()
            available_tags = channel.get("available_tags") or []

            for t in available_tags:
                if t.get("name") == tag_name and t.get("id"):
                    return str(t["id"])

            new_tags_payload = []
            for t in available_tags:
                tag_obj = {
                    "id": t.get("id"),
                    "name": t.get("name"),
                    "moderated": bool(t.get("moderated", False)),
                }
                if t.get("emoji_id") is not None:
                    tag_obj["emoji_id"] = t.get("emoji_id")
                if t.get("emoji_name") is not None:
                    tag_obj["emoji_name"] = t.get("emoji_name")
                new_tags_payload.append(tag_obj)

            new_tags_payload.append(
                {
                    "name": tag_name,
                    "moderated": False,
                    "emoji_name": "⚪",
                }
            )

            patch_resp = await self._bot_api_call_with_retry(
                "PATCH",
                f"/channels/{forum_channel_id}",
                json={"available_tags": new_tags_payload},
            )
            if not patch_resp or patch_resp.status_code != 200:
                return None

            updated_channel = patch_resp.json()
            updated_tags = updated_channel.get("available_tags") or []
            for t in updated_tags:
                if t.get("name") == tag_name and t.get("id"):
                    return str(t["id"])

            return None
        except Exception:
            return None

    # --------------------------- 포럼 글/스레드 ---------------------------
    async def create_forum_post(self, forum_channel_id: str, party_data: Dict, guild_id: str = None) -> Dict:
        try:
            embed = await self._create_party_embed(party_data)
            logger.debug("Forum post embed built (party_id=%s)", party_data.get("id"))
            mention_content = await self._create_mention_content(party_data, guild_id)

            payload: Dict[str, Any] = {
                "name": party_data["title"],
                "message": {
                    "content": mention_content,
                    "embeds": [embed],
                    "components": self._create_party_components(party_data["id"]),
                },
                "auto_archive_duration": 10080,
            }

            applied_tags: list[str] = list(party_data.get("applied_tags") or [])
            if party_data.get("raid_name"):
                boss_tag_id = await self._ensure_forum_tag(forum_channel_id, party_data.get("raid_name"))
                if boss_tag_id and boss_tag_id not in applied_tags:
                    applied_tags.append(boss_tag_id)

            if applied_tags:
                payload["applied_tags"] = applied_tags

            endpoint = f"/channels/{forum_channel_id}/threads"
            resp = await self._bot_api_call_with_retry("POST", endpoint, json=payload)

            if resp is None:
                return {
                    "_error": {
                        "stage": "http_call",
                        "detail": "no response object",
                        "request_url": f"{self.base_url}{endpoint}",
                    }
                }

            if resp.status_code not in (200, 201):
                return _build_http_error(
                    resp,
                    stage="http_post_forum",
                    request_url=f"{self.base_url}{endpoint}",
                    request_payload=payload,
                )

            try:
                data = resp.json()
            except Exception as e:
                return {
                    "_error": {
                        "stage": "decode_json",
                        "type": e.__class__.__name__,
                        "detail": str(e),
                        "request_url": f"{self.base_url}{endpoint}",
                    }
                }

            thread_id = isinstance(data, dict) and data.get("id")
            if not thread_id:
                preview = None
                try:
                    preview = _json.dumps(data, ensure_ascii=False)[:800]
                except Exception:
                    preview = str(data)[:800]
                return {
                    "_error": {
                        "stage": "response_shape",
                        "detail": "response missing 'id'",
                        "response_preview": preview,
                    }
                }

            try:
                manage_components = self._create_manage_components(party_data)
                manage_payload: Dict[str, Any] = {
                    "components": manage_components,
                }
                await self._bot_api_call_with_retry(
                    "POST",
                    f"/channels/{thread_id}/messages",
                    json=manage_payload,
                )
            except Exception:
                pass

            return {"id": str(thread_id)}

        except Exception as e:
            return {
                "_error": {
                    "stage": "exception",
                    "type": e.__class__.__name__,
                    "detail": str(e),
                }
            }


    async def _resolve_waitlist_enabled(self, party_data: Dict) -> bool:
        try:
            v = party_data.get("waitlist_use")
            if v is not None:
                return int(v or 0) == 1
        except Exception:
            pass

        try:
            gid = party_data.get("guild_id")
            if not gid:
                return False
            async with get_db() as db:
                row = await db.execute(
                    "SELECT waitlist_use FROM server WHERE guild_id = ? LIMIT 1",
                    (str(gid),),
                )
            if not row:
                return False
            return int(row[0].get("waitlist_use") or 0) == 1
        except Exception:
            return False

    async def update_forum_post(self, thread_id: str, party_data: Dict) -> bool:
        try:
            embed = await self._create_party_embed(party_data)

            dealers = party_data.get("participants", {}).get("dealers", [])
            supporters = party_data.get("participants", {}).get("supporters", [])
            dealer_max = party_data.get("dealer", 0) or 0
            supporter_max = party_data.get("supporter", 0) or 0

            is_dealer_closed = int(party_data.get("is_dealer_closed") or 0)
            is_supporter_closed = int(party_data.get("is_supporter_closed") or 0)
            both_closed = is_dealer_closed == 1 and is_supporter_closed == 1

            owner_display = party_data.get("owner") or party_data.get("owner_id") or "-"

            total_slots = (dealer_max or 0) + (supporter_max or 0)
            total_now = len(dealers) + len(supporters)
            is_full = total_slots > 0 and total_now >= total_slots

            dealer_full = dealer_max > 0 and len(dealers) >= dealer_max
            supporter_full = supporter_max > 0 and len(supporters) >= supporter_max

            dealer_has_slots = dealer_max > 0 and len(dealers) < dealer_max
            supporter_has_slots = supporter_max > 0 and len(supporters) < supporter_max

            waitlist_enabled = await self._resolve_waitlist_enabled(party_data)

            dealer_text = f"딜러 {len(dealers)}/{dealer_max}"
            supporter_text = f"서포터 {len(supporters)}/{supporter_max}"

            if dealer_full:
                dealer_text = f"~~{dealer_text}~~"
            elif dealer_has_slots:
                dealer_text = f"**{dealer_text}**"

            if supporter_full:
                supporter_text = f"~~{supporter_text}~~"
            elif supporter_has_slots:
                supporter_text = f"**{supporter_text}**"

            if both_closed or (is_full and not waitlist_enabled):
                icon = "⛔"
            elif is_full and waitlist_enabled:
                icon = "⏳"
            else:
                icon = "<:mococo_logo_symbol:1440231382222639165>"

            if waitlist_enabled and is_full and not both_closed:
                join_label = "대기열 신청"
            else:
                join_label = "참가신청"

            disabled_join = bool(both_closed or (is_full and not waitlist_enabled))
            show_waitlist_check = bool(waitlist_enabled and is_full and not both_closed)

            payload = {
                "content": f"{icon} {dealer_text} · {supporter_text} [<@{owner_display}>]",
                "embeds": [embed],
                "components": self._create_party_components(
                    party_data["id"],
                    disabled_join=disabled_join,
                    join_label=join_label,
                    show_waitlist_check=show_waitlist_check,
                ),
            }

            response = await self._bot_api_call_with_retry(
                "PATCH",
                f"/channels/{thread_id}/messages/{thread_id}",
                json=payload,
            )
            return bool(response and response.status_code == 200)
        except Exception:
            return False

    async def delete_forum_post(self, thread_id: str) -> bool:
        try:
            try:
                await self._bot_api_call_with_retry(
                    "PATCH",
                    f"/channels/{thread_id}",
                    json={"archived": False, "locked": False},
                )
            except Exception:
                pass
            resp = await self._bot_api_call_with_retry("DELETE", f"/channels/{thread_id}")
            return bool(resp and resp.status_code in (200, 202, 204))
        except Exception:
            return False

    async def safe_delete_forum_post(self, thread_id: str, reason: str = "") -> bool:
        try:
            resp = await self._bot_api_call_with_retry("DELETE", f"/channels/{thread_id}")
            if resp and resp.status_code in (200, 202, 204, 404):
                return True

            try:
                await self._bot_api_call_with_retry(
                    "PATCH",
                    f"/channels/{thread_id}",
                    json={"archived": False, "locked": False},
                )
            except Exception:
                pass

            resp2 = await self._bot_api_call_with_retry("DELETE", f"/channels/{thread_id}")
            if resp2 and resp2.status_code in (200, 202, 204, 404):
                return True

            return bool(resp2 and resp2.status_code in (200, 202, 204, 404))
        except Exception:
            return False

    async def send_to_thread(
        self,
        thread_id: str,
        content: str = None,
        embed: Dict = None,
    ) -> bool:
        payload: Dict[str, Any] = {}
        if content:
            payload["content"] = content
        if embed:
            payload["embeds"] = [embed]
        if not payload:
            return False

        resp = await self._bot_api_call_with_retry(
            "POST",
            f"/channels/{thread_id}/messages",
            json=payload,
        )
        if resp and resp.status_code in (200, 201):
            return True

        try:
            await self._bot_api_call_with_retry(
                "PATCH",
                f"/channels/{thread_id}",
                json={"archived": False, "locked": False},
            )
        except Exception:
            pass

        resp2 = await self._bot_api_call_with_retry(
            "POST",
            f"/channels/{thread_id}/messages",
            json=payload,
        )
        return bool(resp2 and resp2.status_code in (200, 201))

    # --------------------------- 일반 메세지/DM ---------------------------
    async def send_to_channel(
        self,
        channel_id: str,
        content: str | None = None,
        embed: Dict | None = None,
        *,
        allowed_mentions: Dict[str, Any] | None = None,
        file: File | None = None,
    ) -> bool:
        try:
            payload: Dict[str, Any] = {}
            if content:
                payload["content"] = content
            if embed:
                payload["embeds"] = [embed]
            if allowed_mentions is not None:
                payload["allowed_mentions"] = allowed_mentions

            if not payload and file is None:
                return False

            if file is None:
                resp = await self._bot_api_call_with_retry(
                    "POST",
                    f"/channels/{channel_id}/messages",
                    json=payload,
                )
                return bool(resp and resp.status_code in (200, 201))

            fp = file.fp
            try:
                fb = fp.getvalue()
            except Exception:
                try:
                    pos = fp.tell()
                except Exception:
                    pos = None
                fb = fp.read()
                if pos is not None:
                    try:
                        fp.seek(pos)
                    except Exception:
                        pass

            pj = _json.dumps(payload or {}, ensure_ascii=False, default=str)
            files = {
                "payload_json": (None, pj, "application/json"),
                "files[0]": (file.filename, fb, file.content_type),
            }

            resp = await self._make_bot_api_call_multipart(
                "POST",
                f"/channels/{channel_id}/messages",
                files=files,
            )
            return bool(resp and resp.status_code in (200, 201))
        except Exception:
            return False

    async def send_dm(
        self,
        user_id: str,
        content: str | None = None,
        embed: Dict | None = None,
        file: File | None = None,
    ) -> bool:
        try:
            resp = await self._bot_api_call_with_retry(
                "POST",
                "/users/@me/channels",
                json={"recipient_id": str(user_id)},
            )
            if not resp or resp.status_code not in (200, 201):
                return False
            channel_id = resp.json().get("id")
            if not channel_id:
                return False

            return await self.send_to_channel(
                str(channel_id),
                content=content,
                embed=embed,
                file=file,
            )
        except Exception:
            return False

    # --------------------------- OAuth/프로필 ---------------------------
    async def get_user_guilds(
        self,
        access_token: str,
    ) -> Tuple[Optional[Dict], Optional[List]]:
        return await self._make_oauth_api_call(access_token, "/users/@me/guilds")

    async def get_user_info(
        self,
        access_token: str,
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        return await self._make_oauth_api_call(access_token, "/users/@me")

    async def get_user_profile(
        self,
        user_id: str,
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        try:
            response = await self._bot_api_call_with_retry("GET", f"/users/{user_id}")
            if response and response.status_code == 200:
                return None, response.json()
            elif response and response.status_code == 404:
                return {"error": "User not found", "status_code": 404}, None
            else:
                return {"error": "Discord API error", "status_code": 500}, None
        except Exception:
            return {"error": "Failed to call Discord API", "status_code": 500}, None

    async def refresh_access_token(
        self,
        refresh_token: str,
        user_id: str,
    ) -> Optional[str]:
        try:
            client = await self._ensure_client()
            response = await client.post(
                f"{self.base_url}/oauth2/token",
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if response.status_code != 200:
                return None
            data = response.json()
            new_access_token = data.get("access_token")
            if not new_access_token:
                return None
            await self._save_refreshed_token(
                user_id,
                new_access_token,
                data.get("refresh_token", refresh_token),
                int(data.get("expires_in", 0) or 0),
            )
            return new_access_token
        except Exception:
            return None

    # --------------------------- 멘션/임베드/컴포넌트 ---------------------------
    async def _create_mention_content(self, party_data: Dict, guild_id: str = None) -> str:
        mentions: list[str] = []
        if guild_id:
            try:
                async with get_db() as db:
                    server_setting = await db.execute(
                        "SELECT mention_role_id FROM server WHERE guild_id = ?",
                        (guild_id,),
                    )
                    rid = (
                        server_setting[0]["mention_role_id"]
                        if server_setting
                        and server_setting[0].get("mention_role_id")
                        else None
                    )
                    mentions.append(f"<@&{rid}>") if rid else mentions.append("@here")
                raid_name = party_data.get("raid_name", "")
                roles = await self.get_guild_roles(guild_id)
                match = next((r for r in roles if r.get("name") == raid_name), None)
                if match:
                    mentions.append(f"<@&{match['id']}>")
            except Exception:
                logger.exception(
                    "Failed to compose mention content (guild_id=%s, party_id=%s)",
                    guild_id,
                    party_data.get("id") if isinstance(party_data, dict) else None,
                )

        return " ".join(mentions) if mentions else ""

    async def _create_party_embed(self, party_data: Dict, thumb_url: str = None) -> Dict:
        try:
            ps = party_data.get("participants") or {}
            dealers = ps.get("dealers") or []
            supporters = ps.get("supporters") or []
            dealer_max = party_data.get("dealer") or 0
            supporter_max = party_data.get("supporter") or 0

            is_dealer_closed = int(party_data.get("is_dealer_closed") or 0)
            is_supporter_closed = int(party_data.get("is_supporter_closed") or 0)

            ids = [
                p.get("user_id")
                for p in dealers + supporters
                if p.get("user_id") is not None
            ]
            ids = list(dict.fromkeys(ids)) if ids else []
            emojis = await self._get_user_emojis(ids) if ids else {}
            emo = emojis.get
            owner_display = (
                party_data.get("owner")
                or party_data.get("owner_id")
                or "알수없음"
            )

            party_id = party_data.get("id") or party_data.get("party_id")
            raw_is_active = party_data.get("is_active")
            is_public = False
            if raw_is_active is not None:
                try:
                    is_public = int(raw_is_active) == 1
                except Exception:
                    is_public = False

            header: List[str] = []
            if is_public and party_id:
                header.append(f"```https://mococobot.kr/party/{party_id}```")

            header.append(f"-# 공격대 생성자 : <@{owner_display}>")
            header.append("ㅤ")
            description = "\n".join(header)

            sum_lvl = 0.0
            sum_pow = 0.0
            cnt = 0

            def fmt_two_lines(m, tag_emoji):
                uid = m.get("user_id")
                uid_str = str(uid) if uid is not None else ""
                nick = m.get("name", "")
                ilvl = m.get("item_level") or 0
                cls = m.get("class_name") or "알 수 없음"
                cp = m.get("combat_power") or 0

                def _f(v, d=0.0):
                    try:
                        return float(v)
                    except Exception:
                        return d

                nonlocal sum_lvl, sum_pow, cnt
                sum_lvl += _f(ilvl)
                sum_pow += _f(cp)
                cnt += 1

                if uid_str.isdigit():
                    e = emo(uid_str, "")
                    mention = f"<@{uid_str}>"
                else:
                    e = ""
                    mention = "**[용병]**"

                line1 = f"{mention} {tag_emoji} {nick}"
                line2 = f"-# Lv. {ilvl} | {cls} | {cp}" + (f" {e}" if e else "")
                return line1, line2

            def chunk_lines(lines, limit=1024):
                chunks, buf, cur = [], [], 0
                for ln in lines:
                    add_len = len(ln) + 1
                    if cur + add_len > limit and buf:
                        chunks.append("\n".join(buf))
                        buf, cur = [ln], len(ln) + 1
                    else:
                        buf.append(ln)
                        cur += add_len
                if buf:
                    chunks.append("\n".join(buf))
                return chunks

            def build_chunks(members, tag_emoji, add_separator=False,empty_placeholder="== 없음 =="):
                lines = []
                for m in members:
                    l1, l2 = fmt_two_lines(m, tag_emoji)
                    lines.append(l1)
                    lines.append(l2)

                if not lines and empty_placeholder is not None:
                    lines.append(empty_placeholder)

                if add_separator and lines and members:
                    lines.append("───────────────────────────")
                return chunk_lines(lines, limit=1024)
            fields = []

            dealer_chunks = build_chunks(
                dealers,
                "<:dealer:1398129439656775770>",
                add_separator=bool(supporters),
            )
            dealer_label = f"딜러 **`({len(dealers)}/{dealer_max})`**"
            if is_dealer_closed:
                dealer_label += " ⛔ **(마감)**"
            for ch in dealer_chunks:
                fields.append(
                    {
                        "name": dealer_label,
                        "value": ch,
                        "inline": False,
                    }
                )

            supporter_chunks = build_chunks(
                supporters,
                "<:supporter:1398129451719462974>",
            )
            supporter_label = f"서포터 **`({len(supporters)}/{supporter_max})`**"
            if is_supporter_closed:
                supporter_label += " ⛔ **(마감)**"
            for ch in supporter_chunks:
                fields.append(
                    {
                        "name": supporter_label,
                        "value": ch,
                        "inline": False,
                    }
                )
            if len(fields) > 25:
                rest = fields[24:]
                merged = "\n\n".join(
                    f"**{f['name']}**\n{f['value']}" for f in rest
                )
                fields = fields[:24] + [
                    {
                        "name": "추가 명단",
                        "value": merged[:1024] if len(merged) > 1024 else merged,
                        "inline": False,
                    }
                ]

            footer_text = (
                f"\n공격대 평균 레벨: {sum_lvl / cnt:.1f}\n공격대 평균 전투력: {sum_pow / cnt:.1f}"
                if cnt
                else " "
            )

            embed = {
                "title": f"**{party_data.get('title', '공격대')}**",
                "description": description,
                "color": 0xFFFFFF,
                "footer": {"text": footer_text},
                "fields": fields,
            }
            embed = await self._attach_raid_thumbnail(party_data, embed, thumb_url)
            return embed

        except Exception:
            logger.exception("Failed to build party embed")
            return {
                "title": "**오류**",
                "description": "Embed 생성 중 오류가 발생했습니다.",
                "color": 0xFF0000,
            }

    async def _get_user_emojis(self, user_ids: List[str]) -> Dict[str, str]:
        if not user_ids:
            return {}
        try:
            async with get_db() as db:
                placeholders = ",".join("?" * len(user_ids))
                user_emojis = await db.execute(
                    f"""
                    SELECT user_id, emoji FROM user
                    WHERE user_id IN ({placeholders}) AND emoji IS NOT NULL AND emoji != ''
                """,
                    user_ids,
                )
                return {
                    str(row["user_id"]): row["emoji"] for row in (user_emojis or [])
                }
        except Exception:
            return {}

    async def get_guild_roles(self, guild_id: str) -> List[Dict]:
        try:
            response = await self._bot_api_call_with_retry(
                "GET",
                f"/guilds/{guild_id}/roles",
            )
            if response and response.status_code == 200:
                return response.json()
            else:
                return []
        except Exception:
            return []

    def _create_party_components(self, party_id: int, *, disabled_join: bool = False, join_label: str = "참가신청", show_waitlist_check: bool = False) -> List[Dict]:
        components = [
            {
                "type": 2,
                "style": 3,
                "label": join_label,
                "custom_id": f"party_join_{party_id}",
                "disabled": disabled_join,
            },
            {
                "type": 2,
                "style": 4,
                "label": "참가취소",
                "custom_id": f"party_leave_{party_id}",
            },
        ]

        if show_waitlist_check:
            components.insert(
                1,
                {
                    "type": 2,
                    "style": 2,
                    "label": "대기열 확인",
                    "custom_id": f"party_waitlist_{party_id}",
                },
            )

        return [{"type": 1, "components": components}]

    def _create_manage_components(self, party_data: Dict) -> List[Dict]:
        party_id = party_data.get("id") or party_data.get("party_id")
        return [
            {
                "type": 1,
                "components": [
                    {
                        "type": 2,
                        "style": 2,
                        "label": "인원 관리",
                        "custom_id": f"party_force_cancel_{party_id}",
                    },
                    {
                        "type": 2,
                        "style": 2,
                        "label": "인원 멘션",
                        "custom_id": f"party_mention_{party_id}",
                    },
                    {
                        "type": 2,
                        "style": 2,
                        "label": "일정 공개",
                        "custom_id": f"party_public_{party_id}",
                    },
                    {
                        "type": 2,
                        "style": 1,
                        "label": "이미지",
                        "custom_id": f"party_image_{party_id}",
                    },
                    {
                        "type": 2,
                        "style": 4,
                        "label": "일정 삭제",
                        "custom_id": f"party_delete_{party_id}",
                    },
                ],
            }
        ]

    async def _save_refreshed_token(
        self,
        user_id: str,
        access_token: str,
        refresh_token: str,
        expires_in: int,
    ):
        try:
            async with get_db() as db:
                expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                await db.execute(
                    "UPDATE discord_users SET access_token = ?, refresh_token = ?, expires_at = ? WHERE id = ?",
                    (access_token, refresh_token, expires_at, user_id),
                )
                await db.commit()
        except Exception:
            logger.exception("Failed to save discord token (user_id=%s)", user_id)

    async def list_bot_guild_ids_from_db(self) -> set[int]:
        try:
            async with get_db() as db:
                rows = await db.execute("SELECT guild_id FROM bot_guilds") or []
            return {int(r["guild_id"]) for r in rows}
        except Exception:
            return set()

    async def filter_user_guilds_with_bot(
        self,
        access_token: str,
    ) -> list[dict]:
        err, user_guilds = await self.get_user_guilds(access_token)
        if err:
            raise RuntimeError(f"discord oauth error: {err}")

        bot_ids = await self.list_bot_guild_ids_from_db()
        filtered = [g for g in (user_guilds or []) if int(g.get("id", 0)) in bot_ids]

        return [
            {
                "id": g.get("id"),
                "name": g.get("name"),
                "icon": g.get("icon"),
                "owner": g.get("owner", False),
                "permissions": str(g.get("permissions", "0")),
                "features": g.get("features", []),
            }
            for g in filtered
        ]

    async def send_quiz_message(self, channel_id: str, question: str, *, guild_id: int | str, quiz_id: int | str) -> str | None:
        embed = {
            "title": "오늘의 퀴즈!",
            "description": question,
            "color": 0x2B2D31,
        }
        custom_id = f"quiz_answer:{guild_id}:{quiz_id}"
        components = [
            {
                "type": 1,
                "components": [
                    {
                        "type": 2,
                        "style": 3,
                        "label": "정답 입력",
                        "custom_id": custom_id,
                    }
                ],
            }
        ]
        payload = {"embeds": [embed], "components": components}
        try:
            resp = await self._bot_api_call_with_retry(
                "POST",
                f"/channels/{channel_id}/messages",
                json=payload,
            )
            if resp and resp.status_code in (200, 201):
                data = resp.json()
                return str(data.get("id"))
            return None
        except Exception:
            return None
    
    async def fetch_channel_messages(self, channel_id: str, *, limit_total: int = 5000, batch_size: int = 100) -> tuple[bool, list[dict], dict | None]:
        batch_size = max(1, min(int(batch_size or 100), 100))
        limit_total = max(1, int(limit_total or 1))

        out: list[dict] = []
        before: str | None = None

        while len(out) < limit_total:
            params: dict[str, Any] = {"limit": min(batch_size, limit_total - len(out))}
            if before:
                params["before"] = before

            resp = await self._bot_api_call_with_retry(
                "GET",
                f"/channels/{channel_id}/messages",
                params=params,
            )
            if not resp:
                return False, out, {"stage": "GET_MESSAGES", "status": None}

            if resp.status_code != 200:
                return False, out, _build_http_error(
                    resp,
                    stage="GET_MESSAGES",
                    request_url=f"{self.base_url}/channels/{channel_id}/messages",
                    request_payload=params,
                )

            data = resp.json() or []
            if not isinstance(data, list) or not data:
                break

            out.extend(data)
            before = str(data[-1].get("id") or "")
            if not before:
                break

            if len(data) < params["limit"]:
                break

        return True, out, None


# 전역 인스턴스
discord_service = DiscordService()
discord_service.File = File
