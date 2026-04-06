import json
import hashlib
from zoneinfo import ZoneInfo
import os
import io
import asyncio
import re
import logging
from collections import OrderedDict
from datetime import datetime, timedelta, time as dt_time, date
from typing import List, Dict, Any, Tuple, Optional, Iterable

import httpx
from PIL import Image, ImageDraw, ImageFont, ImageFile

from utils.http_client import get_http_client

KST = ZoneInfo("Asia/Seoul")

CALENDAR_CACHE_KEY = "lostark.calendar.v1"
CALENDAR_CACHE_STALE_DAYS = 8

logger = logging.getLogger(__name__)
ImageFile.LOAD_TRUNCATED_IMAGES = True

try:
    from database.connection import get_db
    from services.discord_service import discord_service
except Exception:
    get_db = None
    discord_service = None

API_URL = "https://developer-lostark.game.onstove.com/gamecontents/calendar"
API_KEYS = ("LOSTARK_API_SUB1_KEY", "LOSTARK_API_SUB2_KEY")

BASE_DIR = os.path.normpath("C:/Users/Administrator/Desktop/TEST/new_back/render")
FONT_PATH = os.path.join(BASE_DIR, "fonts", "Pretendard-Regular.otf")
LOGO_PATH = os.path.join(BASE_DIR, "logo.png")

FIELD_BOSS_ICON_URL = "https://cdn-lostark.game.onstove.com/efui_iconatlas/achieve/achieve_14_142.png"
CHAOS_GATE_ICON_URL = "https://cdn-lostark.game.onstove.com/efui_iconatlas/achieve/achieve_13_11.png"

SAILING_MAX = 6
MAX_ICON_CACHE = max(32, int(os.getenv("CALENDAR_MAX_ICON_CACHE", "512")))
MAX_RESIZED_CACHE = max(64, int(os.getenv("CALENDAR_MAX_RESIZED_CACHE", "2048")))

IMG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://lostark.game.onstove.com/",
    "Accept": "image/png,image/apng,image/*;q=0.8",
}


def _resample_lanczos():
    try:
        return Image.Resampling.LANCZOS
    except Exception:
        return Image.LANCZOS


def _normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if not u:
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("http://"):
        return u.replace("http://", "https://", 1)
    return u


def _game_window(d: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(d, dt_time(6, 0, 0))
    end = start + timedelta(days=1)
    return start, end


def _in_window(t: datetime, start: datetime, end: datetime) -> bool:
    return start <= t < end

def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(KST).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _fmt_time_hhmm(t: datetime) -> str:
    return f"{t.hour:02d}:{t.minute:02d}"


def _time_key_hhmm(t: str) -> Tuple[int, int]:
    try:
        h, m = map(int, t.split(":"))
        return (h, m)
    except Exception:
        return (99, 99)


def _fmt_times_hhmm_from_str(times: Iterable[str]) -> List[str]:
    return sorted(set(times), key=_time_key_hhmm)


def _clean_sailing_name(name: str) -> str:
    s = re.sub(r"^\s*항해\s*협동\s*:\s*", "", name or "").strip()
    s = re.sub(r"\[대항해\][^)]*$", "", s).strip()
    return s


def _bucket_reward(name: str) -> Optional[str]:
    s = (name or "").replace("  ", " ").strip()
    if s == "골드" or "골드" in s:
        return "골드"
    if "카드 팩" in s or "카드팩" in s:
        return "카드 팩"
    if any(k in s for k in ("대양의 주화", "해적 주화", "주화 상자")) or s.endswith(" 주화"):
        return "대양의 주화"
    if s == "실링" or "실링" in s:
        return "실링"
    return None


def _auth_headers() -> Iterable[Dict[str, str]]:
    for k in API_KEYS:
        v = os.getenv(k)
        if v:
            yield {"authorization": f"bearer {v}", "accept": "application/json"}


async def fetch_calendar_json() -> Optional[Any]:
    if not any(os.getenv(k) for k in API_KEYS):
        return None

    client = await get_http_client()
    for headers in _auth_headers():
        try:
            r = await client.get(API_URL, headers=headers, timeout=httpx.Timeout(30.0))
            if r.status_code == 200:
                return r.json()
        except Exception:
            continue
    return None

def _now_kst_naive() -> datetime:
    return datetime.now(KST).replace(tzinfo=None)


def _extract_entries(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        entries = payload.get("Contents")
        return entries if isinstance(entries, list) else []
    if isinstance(payload, list):
        return [e for e in payload if isinstance(e, dict)]
    return []


def _infer_valid_range(payload: Any) -> Tuple[Optional[date], Optional[date]]:
    entries = _extract_entries(payload)
    min_d: Optional[date] = None
    max_d: Optional[date] = None
    for e in entries:
        for ts in (e.get("StartTimes") or []):
            dtv = _parse_dt(ts)
            if not dtv:
                continue
            d = dtv.date()
            if min_d is None or d < min_d:
                min_d = d
            if max_d is None or d > max_d:
                max_d = d
    return min_d, max_d


async def _db_get_cache_row(cache_key: str) -> Optional[Dict[str, Any]]:
    if get_db is None:
        return None
    try:
        async with get_db() as db:
            rows = await db.execute(
                "SELECT cache_key,payload,payload_sha256,fetched_at,expires_at,valid_from,valid_to "
                "FROM api_cache WHERE cache_key = ? LIMIT 1",
                (cache_key,),
            ) or []
            return rows[0] if rows else None
    except Exception:
        return None


async def _db_upsert_cache_row(
    *,
    cache_key: str,
    payload_str: str,
    payload_sha256: str,
    fetched_at: datetime,
    expires_at: Optional[datetime],
    valid_from: Optional[date],
    valid_to: Optional[date],
) -> None:
    if get_db is None:
        return
    try:
        async with get_db() as db:
            await db.execute(
                "INSERT INTO api_cache (cache_key,payload,payload_sha256,fetched_at,expires_at,valid_from,valid_to) "
                "VALUES (?,?,?,?,?,?,?) "
                "ON DUPLICATE KEY UPDATE "
                "payload=VALUES(payload), payload_sha256=VALUES(payload_sha256), fetched_at=VALUES(fetched_at), "
                "expires_at=VALUES(expires_at), valid_from=VALUES(valid_from), valid_to=VALUES(valid_to)",
                (cache_key, payload_str, payload_sha256, fetched_at, expires_at, valid_from, valid_to),
            )
            await db.commit()
    except Exception:
        return


def _cache_is_expired(row: Dict[str, Any]) -> bool:
    exp = row.get("expires_at")
    if not exp:
        return False
    try:
        return exp <= _now_kst_naive()
    except Exception:
        return False


def _cache_covers_date(row: Dict[str, Any], target_date: Optional[date]) -> bool:
    if not target_date:
        return True
    vf = row.get("valid_from")
    vt = row.get("valid_to")
    if not vf or not vt:
        return True
    try:
        return vf <= target_date <= vt
    except Exception:
        return True


async def refresh_calendar_cache() -> bool:
    payload = await fetch_calendar_json()
    if not payload:
        return False

    payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    payload_sha256 = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()

    fetched_at = _now_kst_naive()
    expires_at = fetched_at + timedelta(days=CALENDAR_CACHE_STALE_DAYS)
    valid_from, valid_to = _infer_valid_range(payload)

    await _db_upsert_cache_row(
        cache_key=CALENDAR_CACHE_KEY,
        payload_str=payload_str,
        payload_sha256=payload_sha256,
        fetched_at=fetched_at,
        expires_at=expires_at,
        valid_from=valid_from,
        valid_to=valid_to,
    )
    return True


async def get_calendar_payload_cached(
    *,
    target_date: Optional[date] = None,
    refresh_policy: str = "auto",
) -> Optional[Any]:
    if get_db is None:
        return await fetch_calendar_json() if refresh_policy != "never" else None

    row = await _db_get_cache_row(CALENDAR_CACHE_KEY)

    need_refresh = False
    if refresh_policy == "force":
        need_refresh = True
    elif row is None:
        need_refresh = refresh_policy != "never"
    else:
        if not _cache_covers_date(row, target_date):
            need_refresh = True
        elif refresh_policy == "auto" and _cache_is_expired(row):
            need_refresh = True

    if need_refresh:
        ok = await refresh_calendar_cache()
        if ok:
            row = await _db_get_cache_row(CALENDAR_CACHE_KEY)

    if not row:
        return None

    try:
        return json.loads(row.get("payload") or "null")
    except Exception:
        return None


async def get_calendar_for_date(target_date: date, *, refresh_policy: str = "auto") -> Optional[Dict[str, Any]]:
    payload = await get_calendar_payload_cached(target_date=target_date, refresh_policy=refresh_policy)
    if not payload:
        return None
    return parse_calendar_data(payload, target_date)


async def get_calendar_png_for_date(target_date: date, *, refresh_policy: str = "auto") -> Optional[Tuple[bytes, str]]:
    data = await get_calendar_for_date(target_date, refresh_policy=refresh_policy)
    if not data:
        return None
    drawer = CalendarDrawer()
    buf = await drawer.draw(target_date, data)
    file_bytes = buf.getvalue()
    filename = f"calendar_{target_date.strftime('%Y%m%d')}.png"
    return file_bytes, filename


def _get_game_date() -> date:
    now = datetime.now(KST)
    if now.hour < 6:
        return (now - timedelta(days=1)).date()
    return now.date()


def parse_calendar_data(payload: Any, game_date: date) -> Dict[str, Any]:
    start, end = _game_window(game_date)

    processed: Dict[str, Any] = {
        "presence": {
            "field_boss": False,
            "chaos_gate": False,
            "field_boss_icon": FIELD_BOSS_ICON_URL,
            "chaos_gate_icon": CHAOS_GATE_ICON_URL,
        },
        "islands": [],
        "sailing": [],
        "reward_buckets": {},
    }

    if isinstance(payload, dict):
        entries = payload.get("Contents")
        if not isinstance(entries, list):
            entries = []
    elif isinstance(payload, list):
        entries = payload
    else:
        entries = []

    islands_map: Dict[str, Dict[str, Any]] = {}
    rewards_map: Dict[str, Dict[str, Dict[str, Any]]] = {
        "골드": {},
        "카드 팩": {},
        "대양의 주화": {},
        "실링": {},
    }
    major_bucket_set = {"골드"}

    def _effective_times_for_item(it: Dict[str, Any], fallback_dt_times: List[datetime]) -> List[datetime]:
        raw = it.get("StartTimes")
        if isinstance(raw, list) and raw:
            out: List[datetime] = []
            for ts in raw:
                dtv = _parse_dt(ts)
                if dtv and _in_window(dtv, start, end):
                    out.append(dtv)
            return out
        return list(fallback_dt_times)

    for e in entries:
        if not isinstance(e, dict):
            continue

        cat = e.get("CategoryName") or ""
        name = e.get("ContentsName") or ""
        icon = _normalize_url(e.get("ContentsIcon"))

        raw_times = e.get("StartTimes") or []
        dt_times: List[datetime] = []
        for ts in raw_times:
            dtv = _parse_dt(ts)
            if dtv and _in_window(dtv, start, end):
                dt_times.append(dtv)

        if not dt_times:
            continue

        time_strs = _fmt_times_hhmm_from_str(_fmt_time_hhmm(t) for t in dt_times)

        if cat == "모험 섬" and name:
            rewards_for_card: List[Dict[str, Any]] = []
            is_major_flag = False

            for ri in (e.get("RewardItems") or []):
                if not isinstance(ri, dict):
                    continue
                for it in (ri.get("Items") or []):
                    if not isinstance(it, dict):
                        continue

                    it_name = it.get("Name") or ""
                    it_icon = _normalize_url(it.get("Icon"))
                    bucket = _bucket_reward(it_name)

                    eff_dt_times = _effective_times_for_item(it, dt_times)
                    if not eff_dt_times:
                        continue
                    eff_time_strs = _fmt_times_hhmm_from_str(_fmt_time_hhmm(t) for t in eff_dt_times)

                    if bucket:
                        if bucket in major_bucket_set:
                            is_major_flag = True

                        by_island = rewards_map.setdefault(bucket, {}).setdefault(
                            name, {"times": set(), "icon": None}
                        )
                        for t in eff_time_strs:
                            by_island["times"].add(t)
                        if not by_island["icon"] and it_icon:
                            by_island["icon"] = it_icon

                    if it_icon:
                        rewards_for_card.append({"name": it_name, "icon": it_icon, "grade": it.get("Grade")})

            if name in islands_map:
                prev = islands_map[name]
                prev["times"] = _fmt_times_hhmm_from_str(list(prev.get("times", [])) + list(time_strs))
                if is_major_flag:
                    prev["is_major"] = True

                existing_rewards = prev.get("rewards") or []
                existing_names = {r.get("name") for r in existing_rewards if isinstance(r, dict)}
                for new_r in rewards_for_card:
                    if new_r.get("name") not in existing_names:
                        existing_rewards.append(new_r)
                        existing_names.add(new_r.get("name"))
                prev["rewards"] = existing_rewards
                if not prev.get("icon") and icon:
                    prev["icon"] = icon
            else:
                islands_map[name] = {
                    "name": name,
                    "icon": icon,
                    "times": list(time_strs),
                    "rewards": rewards_for_card,
                    "is_major": is_major_flag,
                }

        elif cat == "필드보스":
            processed["presence"]["field_boss"] = True

        elif cat in ("카오스게이트", "카오스 게이트"):
            processed["presence"]["chaos_gate"] = True

        elif "항해" in cat:
            processed["sailing"].append({"name": _clean_sailing_name(name), "icon": icon, "times": list(time_strs)})

    processed["islands"] = sorted(islands_map.values(), key=lambda x: (not bool(x.get("is_major")), x.get("name") or ""))

    processed["sailing"] = sorted(
        processed["sailing"],
        key=lambda x: (_time_key_hhmm((x.get("times") or ["99:99"])[0]), x.get("name") or ""),
    )

    reward_out: Dict[str, List[Dict[str, Any]]] = {}
    for bucket, by_island in rewards_map.items():
        arr: List[Dict[str, Any]] = []
        for iname, obj in by_island.items():
            labels = sorted(obj.get("times") or [], key=_time_key_hhmm)
            if labels:
                arr.append({"name": iname, "times": labels, "icon": obj.get("icon")})
        arr.sort(key=lambda x: (_time_key_hhmm((x.get("times") or ["99:99"])[0]), x.get("name") or ""))
        if arr:
            reward_out[bucket] = arr

    processed["reward_buckets"] = reward_out
    return processed


class CalendarDrawer:
    def __init__(self):
        self.width = 1080
        self.padding = 40

        self.bg_color = (13, 14, 18, 255)
        self.card_bg = (26, 27, 32, 255)
        self.card_highlight_bg = (32, 33, 38, 255)
        self.stroke = (45, 46, 50, 255)
        self.gold_stroke = (180, 140, 60, 255)

        self.text_primary = (255, 255, 255, 255)
        self.text_secondary = (170, 175, 185, 255)
        self.text_tertiary = (100, 105, 115, 255)

        self.accent_gold = (255, 200, 80, 255)
        self.accent_blue = (100, 180, 255, 255)
        self.accent_green = (100, 220, 140, 255)
        self.accent_red = (255, 90, 90, 255)

        self.time_pill_bg = (40, 42, 48, 255)
        self.placeholder_badge_bg = (45, 46, 50, 255)

        try:
            self.font_header = ImageFont.truetype(FONT_PATH, 36)
            self.font_title = ImageFont.truetype(FONT_PATH, 22)
            self.font_date = ImageFont.truetype(FONT_PATH, 26)
            self.font_content = ImageFont.truetype(FONT_PATH, 24)
            self.font_small = ImageFont.truetype(FONT_PATH, 20)
            self.font_bold = ImageFont.truetype(FONT_PATH, 26)
            self.font_tiny = ImageFont.truetype(FONT_PATH, 18)
        except Exception:
            default = ImageFont.load_default()
            self.font_header = default
            self.font_title = default
            self.font_date = default
            self.font_content = default
            self.font_small = default
            self.font_bold = default
            self.font_tiny = default

        self.logo_img: Optional[Image.Image] = None
        if os.path.exists(LOGO_PATH):
            try:
                self.logo_img = Image.open(LOGO_PATH).convert("RGBA")
            except Exception:
                self.logo_img = None

        self._resample = _resample_lanczos()

        self.icon_cache: OrderedDict[str, Image.Image] = OrderedDict()
        self._resized_cache: OrderedDict[Tuple[str, Tuple[int, int], int], Image.Image] = OrderedDict()
        self._sem = asyncio.Semaphore(12)

    @staticmethod
    def _decode_icon_bytes(content: bytes) -> Optional[Image.Image]:
        try:
            img = Image.open(io.BytesIO(content)).convert("RGBA")
            img.load()
            return img
        except Exception:
            return None

    async def _fetch(self, client: httpx.AsyncClient, url: str) -> None:
        u = _normalize_url(url)
        if not u:
            return
        if u in self.icon_cache:
            self.icon_cache.move_to_end(u)
            return

        async with self._sem:
            try:
                r = await client.get(u, headers=IMG_HEADERS)
                if r.status_code != 200 or not r.content:
                    return
                img = await asyncio.to_thread(self._decode_icon_bytes, r.content)
                if img is None:
                    return
                self.icon_cache[u] = img
                self.icon_cache.move_to_end(u)
                while len(self.icon_cache) > MAX_ICON_CACHE:
                    self.icon_cache.popitem(last=False)
            except Exception:
                return

    async def _prepare_assets(self, data: Dict[str, Any]) -> None:
        urls = set()

        p = data.get("presence") or {}
        if (u := _normalize_url(p.get("field_boss_icon"))):
            urls.add(u)
        if (u := _normalize_url(p.get("chaos_gate_icon"))):
            urls.add(u)

        for island in (data.get("islands") or []):
            if (u := _normalize_url(island.get("icon"))):
                urls.add(u)
            for reward in (island.get("rewards") or []):
                if (u := _normalize_url(reward.get("icon"))):
                    urls.add(u)

        for bucket_arr in (data.get("reward_buckets") or {}).values():
            for x in bucket_arr or []:
                if (u := _normalize_url(x.get("icon"))):
                    urls.add(u)

        for sailing in (data.get("sailing") or []):
            if (u := _normalize_url(sailing.get("icon"))):
                urls.add(u)

        if not urls:
            return

        client = await get_http_client()
        await asyncio.gather(*[self._fetch(client, u) for u in urls], return_exceptions=True)

    def _get_resized_icon(self, url: Optional[str], size: Tuple[int, int], radius: int = 0) -> Optional["Image.Image"]:
        u = _normalize_url(url)
        if not u:
            return None
        base = self.icon_cache.get(u)
        if base is None:
            return None

        key = (u, size, radius)
        cached = self._resized_cache.get(key)
        if cached is not None:
            self._resized_cache.move_to_end(key)
            return cached

        try:
            src = base.copy()
            src.thumbnail(size, self._resample)
            bg = Image.new("RGBA", size, (0, 0, 0, 0))
            ox = (size[0] - src.size[0]) // 2
            oy = (size[1] - src.size[1]) // 2
            bg.paste(src, (ox, oy), src)

            img = bg
            if radius > 0:
                mask = Image.new("L", size, 0)
                md = ImageDraw.Draw(mask)
                md.rounded_rectangle((0, 0, size[0], size[1]), radius=radius, fill=255)
                out = Image.new("RGBA", size, (0, 0, 0, 0))
                out.paste(img, (0, 0), mask=mask)
                img = out

            self._resized_cache[key] = img
            self._resized_cache.move_to_end(key)
            while len(self._resized_cache) > MAX_RESIZED_CACHE:
                self._resized_cache.popitem(last=False)
            return img
        except Exception:
            return None

    def _paste(self, base: "Image.Image", icon: "Image.Image", xy: Tuple[int, int]) -> None:
        try:
            base.paste(icon, xy, icon)
        except Exception:
            return

    def _rounded_rect(
        self,
        d: ImageDraw.ImageDraw,
        box: Tuple[int, int, int, int],
        r: int,
        fill: Any,
        outline: Any = None,
        width: int = 1,
    ) -> None:
        if outline is None:
            d.rounded_rectangle(box, radius=r, fill=fill)
        else:
            d.rounded_rectangle(box, radius=r, fill=fill, outline=outline, width=width)

    def _text_width(self, d: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> float:
        try:
            return d.textlength(text, font=font)
        except Exception:
            try:
                return font.getlength(text)
            except Exception:
                return float(len(text) * 10)

    def _time_pill_w(self, d: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
        if not text:
            return 0
        pad_x = 8
        return int(self._text_width(d, text, font)) + pad_x * 2

    def _draw_time_pill_right(self, d: ImageDraw.ImageDraw, text: str, right_x: int, y: int, font: ImageFont.ImageFont) -> int:
        if not text:
            return 0
        pad_x = 8
        h = 22
        w = self._time_pill_w(d, text, font)
        x = right_x - w
        self._rounded_rect(d, (x, y, right_x, y + h), 8, self.time_pill_bg)
        d.text((x + pad_x, y + 1), text, font=font, fill=self.text_primary)
        return w

    def _draw_date_badge_right(self, d: ImageDraw.ImageDraw, text: str, right_x: int, y_center: int) -> int:
        if not text:
            return 0
        pad_x = 14
        h = 34
        w = int(self._text_width(d, text, self.font_date)) + pad_x * 2
        x1 = right_x - w
        y1 = y_center - (h // 2)
        self._rounded_rect(d, (x1, y1, right_x, y1 + h), 12, self.time_pill_bg)
        d.text((x1 + pad_x, y1 + 5), text, font=self.font_date, fill=self.text_primary)
        return w

    def _ellipsize(self, d: ImageDraw.ImageDraw, s: str, font: ImageFont.ImageFont, max_w: float) -> str:
        if self._text_width(d, s, font) <= max_w:
            return s
        ell = "..."
        lo, hi = 0, len(s)
        while lo < hi:
            mid = (lo + hi) // 2
            cand = s[:mid] + ell
            if self._text_width(d, cand, font) <= max_w:
                lo = mid + 1
            else:
                hi = mid
        return s[:max(0, lo - 1)] + ell

    def _draw_badge_placeholder(self, d: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, label: str) -> None:
        self._rounded_rect(d, (x, y, x + w, y + h), 16, self.placeholder_badge_bg)
        tw = int(self._text_width(d, label, self.font_bold))
        tx = x + (w - tw) // 2
        ty = y + (h - 22) // 2
        d.text((tx, ty), label, font=self.font_bold, fill=self.text_primary)

    async def draw(self, game_date: date, data: Dict[str, Any]) -> io.BytesIO:
        await self._prepare_assets(data)
        return await asyncio.to_thread(self._draw_sync, game_date, data)

    def _draw_sync(self, game_date: date, data: Dict[str, Any]) -> io.BytesIO:

        islands = data.get("islands") or []
        sailing = data.get("sailing") or []
        reward_buckets = data.get("reward_buckets") or {}
        rewards_list = [k for k in ("골드", "카드 팩", "대양의 주화", "실링") if reward_buckets.get(k)]

        header_h = 84

        sailing_items = sailing[:SAILING_MAX]
        sailing_cols = 3
        sailing_card_h = 50
        sailing_row_step = 60
        sailing_rows = (len(sailing_items) + sailing_cols - 1) // sailing_cols if sailing_items else 0

        h = self.padding + header_h + 24
        h += 140

        h += 40
        if not islands:
            h += 50
        else:
            island_rows = (len(islands) + 1) // 2
            h += island_rows * 160 + 20

        if rewards_list:
            h += 40
            for k in rewards_list:
                items = reward_buckets.get(k) or []
                if items:
                    h += 60 + 20
            h += 10

        h += 40
        if not sailing_rows:
            h += 50
        else:
            h += sailing_rows * sailing_row_step

        h += 120

        img = Image.new("RGBA", (self.width, max(800, h)), self.bg_color)
        d = ImageDraw.Draw(img)

        y = self.padding

        header_x1 = self.padding
        header_y1 = y
        header_x2 = self.width - self.padding
        header_y2 = y + header_h
        self._rounded_rect(d, (header_x1, header_y1, header_x2, header_y2), 20, self.card_bg, self.stroke, 1)

        title_txt = "로스트아크 일정"
        date_txt = f"{game_date.month}월 {game_date.day}일"

        d.text((header_x1 + 22, header_y1 + 22), title_txt, font=self.font_header, fill=self.text_primary)
        self._draw_date_badge_right(d, date_txt, header_x2 - 22, header_y1 + header_h // 2)

        y += header_h + 24

        content_w = (self.width - self.padding * 3) // 2
        p = data.get("presence") or {}

        self._rounded_rect(d, (self.padding, y, self.padding + content_w, y + 100), 18, self.card_bg, self.stroke, 1)
        fb_icon = self._get_resized_icon(p.get("field_boss_icon"), (60, 60), radius=16)
        if fb_icon:
            self._paste(img, fb_icon, (self.padding + 20, y + 20))
        else:
            self._draw_badge_placeholder(d, self.padding + 20, y + 20, 60, 60, "")
        d.text((self.padding + 100, y + 24), "필드보스", font=self.font_bold, fill=self.text_primary)
        fb_active = bool(p.get("field_boss"))
        fb_line = "매 시 정각 출현" if fb_active else "일정 없음"
        fb_fill = self.accent_green if fb_active else self.accent_red
        d.text((self.padding + 100, y + 56), fb_line, font=self.font_content, fill=fb_fill)

        rx = self.padding + content_w + self.padding
        self._rounded_rect(d, (rx, y, rx + content_w, y + 100), 18, self.card_bg, self.stroke, 1)
        cg_icon = self._get_resized_icon(p.get("chaos_gate_icon"), (60, 60), radius=16)
        if cg_icon:
            self._paste(img, cg_icon, (rx + 20, y + 20))
        else:
            self._draw_badge_placeholder(d, rx + 20, y + 20, 60, 60, "")
        d.text((rx + 100, y + 24), "카오스게이트", font=self.font_bold, fill=self.text_primary)
        cg_active = bool(p.get("chaos_gate"))
        cg_line = "매 시 정각 출현" if cg_active else "일정 없음"
        cg_fill = self.accent_blue if cg_active else self.accent_red
        d.text((rx + 100, y + 56), cg_line, font=self.font_content, fill=cg_fill)

        y += 140

        d.text((self.padding, y), "모험 섬", font=self.font_bold, fill=self.text_primary)
        y += 40

        if not islands:
            d.text((self.padding, y), "오늘 모험 섬 일정이 없습니다.", font=self.font_content, fill=self.text_tertiary)
            y += 50
        else:
            col_w = (self.width - self.padding * 3) // 2
            card_h = 140

            for idx, isl in enumerate(islands):
                row = idx // 2
                col = idx % 2
                cx = self.padding + col * (col_w + self.padding)
                cy = y + row * (card_h + 20)

                is_major = bool(isl.get("is_major"))
                bg = self.card_highlight_bg if is_major else self.card_bg
                stroke = self.gold_stroke if is_major else self.stroke
                self._rounded_rect(d, (cx, cy, cx + col_w, cy + card_h), 18, bg, stroke, 1)

                i_icon = self._get_resized_icon(isl.get("icon"), (50, 50), radius=12)
                if i_icon:
                    self._paste(img, i_icon, (cx + 20, cy + 20))

                nm_col = self.accent_gold if is_major else self.text_primary
                d.text((cx + 86, cy + 18), isl.get("name", ""), font=self.font_bold, fill=nm_col)

                tx = cx + 86
                ty = cy + 54
                for t_str in (isl.get("times") or [])[:5]:
                    w = self._time_pill_w(d, t_str, self.font_tiny)
                    if w <= 0:
                        continue
                    if tx + w > cx + col_w - 12:
                        break
                    self._rounded_rect(d, (tx, ty, tx + w, ty + 22), 8, self.time_pill_bg)
                    d.text((tx + 8, ty + 1), t_str, font=self.font_tiny, fill=self.text_primary)
                    tx += w + 8

                r_x = cx + 20
                r_y = cy + 88
                for rwd in (isl.get("rewards") or [])[:10]:
                    r_ic = self._get_resized_icon(rwd.get("icon"), (34, 34), radius=8)
                    if r_ic:
                        self._paste(img, r_ic, (r_x, r_y))
                        r_x += 42
                        if r_x > cx + col_w - 40:
                            break

            island_rows = (len(islands) + 1) // 2
            y += island_rows * 160 + 20

        if rewards_list:
            d.text((self.padding, y), "주요 보상", font=self.font_bold, fill=self.accent_gold)
            y += 40

            for k in rewards_list:
                items = reward_buckets.get(k) or []
                if not items:
                    continue

                bucket_h = 60

                ic_url = next((x.get("icon") for x in items if x.get("icon")), None)
                ic = self._get_resized_icon(ic_url, (24, 24), radius=6) if ic_url else None
                if ic:
                    self._paste(img, ic, (self.padding, y + 6))

                d.text((self.padding + 34, y + 6), k, font=self.font_bold, fill=self.text_primary)

                label_w = 34 + int(self._text_width(d, k, self.font_bold)) + 18
                x = self.padding + max(120, label_w)
                right_edge = self.width - self.padding

                pad_x = 14
                gap = 12
                chip_h = 40

                for idx, item in enumerate(items):
                    name_txt = item.get("name") or ""
                    max_w = right_edge - x
                    if max_w <= (pad_x * 2 + 20):
                        break

                    chip_w = int(self._text_width(d, name_txt, self.font_small)) + pad_x * 2

                    if chip_w > max_w:
                        remaining = len(items) - idx
                        more_txt = f"+{remaining}"
                        more_w = int(self._text_width(d, more_txt, self.font_small)) + pad_x * 2

                        if idx > 0 and more_w <= max_w:
                            self._rounded_rect(d, (x, y, x + more_w, y + chip_h), 10, self.time_pill_bg)
                            d.text((x + pad_x, y + 10), more_txt, font=self.font_small, fill=self.text_secondary)
                        else:
                            t_str = self._ellipsize(d, name_txt, self.font_small, max_w - pad_x * 2)
                            tw = int(self._text_width(d, t_str, self.font_small))
                            w2 = min(max_w, tw + pad_x * 2)
                            self._rounded_rect(d, (x, y, x + w2, y + chip_h), 10, self.time_pill_bg)
                            d.text((x + pad_x, y + 10), t_str, font=self.font_small, fill=self.text_secondary)
                        break

                    self._rounded_rect(d, (x, y, x + chip_w, y + chip_h), 10, self.time_pill_bg)
                    d.text((x + pad_x, y + 10), name_txt, font=self.font_small, fill=self.text_secondary)
                    x += chip_w + gap

                y += bucket_h + 20

            y += 10

        d.text((self.padding, y), "항해 협동", font=self.font_bold, fill=self.text_primary)
        y += 40

        if not sailing_items:
            d.text((self.padding, y), "오늘 항해 협동 일정이 없습니다.", font=self.font_content, fill=self.text_tertiary)
            y += 50
        else:
            gap = 18
            avail = self.width - self.padding * 2
            col_w = int((avail - gap * (sailing_cols - 1)) / sailing_cols)
            card_h = sailing_card_h

            for idx, it in enumerate(sailing_items):
                row = idx // sailing_cols
                col = idx % sailing_cols
                cx = self.padding + col * (col_w + gap)
                cy = y + row * sailing_row_step

                self._rounded_rect(d, (cx, cy, cx + col_w, cy + card_h), 14, self.card_bg)

                sic = self._get_resized_icon(it.get("icon"), (32, 32), radius=8)
                if sic:
                    self._paste(img, sic, (cx + 12, cy + 9))

                name_x = cx + 56
                right_edge = cx + col_w - 14

                times = (it.get("times") or [])[:6]
                time_total = 0
                for st in times:
                    time_total += self._time_pill_w(d, st, self.font_tiny) + 10
                if time_total > 0:
                    time_total -= 10

                name_max = max(0, (right_edge - time_total - 20) - name_x)
                name_txt = self._ellipsize(d, it.get("name") or "", self.font_content, name_max)
                d.text((name_x, cy + 13), name_txt, font=self.font_content, fill=self.text_primary)

                rx2 = right_edge
                for st in reversed(times):
                    w = self._time_pill_w(d, st, self.font_tiny)
                    self._draw_time_pill_right(d, st, rx2, cy + 13, self.font_tiny)
                    rx2 -= (w + 10)

            y += sailing_rows * sailing_row_step

        ft = "Image Created by Mococo"
        fw = self._text_width(d, ft, self.font_tiny)
        fy = y + 20
        d.text((self.width - self.padding - fw, fy), ft, font=self.font_tiny, fill=self.text_tertiary)

        if self.logo_img:
            target_h = 24
            ow, oh = self.logo_img.size
            if oh > 0:
                target_w = int(ow * (target_h / oh))
                try:
                    lr = self.logo_img.resize((target_w, target_h), resample=self._resample)
                    self._paste(img, lr, (self.padding, fy))
                except Exception:
                    pass

        y += 60
        img = img.crop((0, 0, self.width, max(800, y)))

        out = io.BytesIO()
        img.save(out, format="PNG")
        out.seek(0)
        return out


async def log_delivery(user_id, guild_id, channel_id, game_date) -> None:
    if get_db is None:
        return
    try:
        async with get_db() as db:
            if user_id:
                await db.execute(
                    "INSERT INTO delivery_log (user_id, type, game_date, sent_at) "
                    "VALUES (?, 'daily', ?, NOW(6)) "
                    "ON DUPLICATE KEY UPDATE sent_at=VALUES(sent_at)",
                    (int(user_id), game_date),
                )
            elif channel_id:
                await db.execute(
                    "INSERT INTO delivery_log (channel_id, guild_id, type, game_date, sent_at) "
                    "VALUES (?, ?, 'daily', ?, NOW(6)) "
                    "ON DUPLICATE KEY UPDATE sent_at=VALUES(sent_at)",
                    (int(channel_id), int(guild_id), game_date),
                )
            await db.commit()
    except Exception:
        return

def _refresh_cutoff_for_game_date(gd: date) -> datetime:
    delta = (gd.weekday() - 2) % 7
    wed = gd - timedelta(days=delta)
    return datetime.combine(wed, dt_time(10, 1, 0))


def _coerce_dt(v: Any) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v
    if isinstance(v, str) and v:
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(KST).replace(tzinfo=None)
        except Exception:
            try:
                return datetime.fromisoformat(v)
            except Exception:
                return None
    return None


async def get_today_rewards_values_from_cache(game_date: Optional[date] = None) -> Dict[str, Any]:
    game_date = game_date or _get_game_date()

    row = await _db_get_cache_row(CALENDAR_CACHE_KEY)
    if not row:
        return {
            "ok": False,
            "status": "no_cache",
            "game_date": game_date.isoformat(),
            "message": "현재 저장된 캘린더 캐시가 없습니다. 수요일 점검 이후(10:01 기준) 갱신이 완료되면 보상 정보를 제공할 수 있어요.",
            "rewards": {},
            "adventure_islands": [],
        }

    fetched_at = _coerce_dt(row.get("fetched_at"))
    valid_from = row.get("valid_from")
    valid_to = row.get("valid_to")

    cache_from = valid_from.isoformat() if isinstance(valid_from, date) else (str(valid_from) if valid_from else None)
    cache_to = valid_to.isoformat() if isinstance(valid_to, date) else (str(valid_to) if valid_to else None)

    cutoff = _refresh_cutoff_for_game_date(game_date)
    is_stale = (fetched_at is None) or (fetched_at < cutoff)
    out_of_range = ((valid_from is not None) and (game_date < valid_from)) or ((valid_to is not None) and (game_date > valid_to))

    if out_of_range:
        rng_txt = ""
        if valid_from and valid_to:
            rng_txt = f"{valid_from} ~ {valid_to}"
        elif valid_from:
            rng_txt = f"{valid_from} ~"
        elif valid_to:
            rng_txt = f"~ {valid_to}"
        msg = "조회 범위를 벗어났어요."
        if rng_txt:
            msg = f"{msg} (캐시: {rng_txt})"
        return {
            "ok": False,
            "status": "out_of_range",
            "game_date": game_date.isoformat(),
            "fetched_at": fetched_at.isoformat() if fetched_at else None,
            "cache_from": cache_from,
            "cache_to": cache_to,
            "message": msg,
            "rewards": {},
            "adventure_islands": [],
        }

    if is_stale:
        last_txt = fetched_at.strftime("%Y-%m-%d %H:%M") if fetched_at else "알 수 없음"
        rng_txt = ""
        if valid_from and valid_to:
            rng_txt = f" (캐시 범위: {valid_from} ~ {valid_to})"
        return {
            "ok": False,
            "status": "not_updated",
            "game_date": game_date.isoformat(),
            "fetched_at": fetched_at.isoformat() if fetched_at else None,
            "cache_from": cache_from,
            "cache_to": cache_to,
            "message": (
                "캘린더 데이터가 아직 최신으로 갱신되지 않았습니다. "
                "로스트아크 점검/업데이트 반영이 완료되면(수요일 10:01 기준) 자동으로 최신 일정/보상이 제공돼요. "
                f"마지막 캐시 갱신: {last_txt}{rng_txt}"
            ),
            "rewards": {},
            "adventure_islands": [],
        }

    payload_str = row.get("payload")
    try:
        payload = json.loads(payload_str) if payload_str else None
    except Exception:
        payload = None

    if not payload:
        return {
            "ok": False,
            "status": "bad_cache",
            "game_date": game_date.isoformat(),
            "fetched_at": fetched_at.isoformat() if fetched_at else None,
            "cache_from": cache_from,
            "cache_to": cache_to,
            "message": "저장된 캘린더 캐시를 해석할 수 없습니다. 점검 이후 갱신이 정상 반영되면 다시 확인해주세요.",
            "rewards": {},
            "adventure_islands": [],
        }

    parsed = parse_calendar_data(payload, game_date)
    buckets = parsed.get("reward_buckets") or {}

    rewards: Dict[str, List[Dict[str, Any]]] = {}
    for k, arr in buckets.items():
        if not arr:
            continue
        rewards[k] = [{"name": x.get("name"), "times": x.get("times") or []} for x in arr if isinstance(x, dict)]

    islands_out: List[Dict[str, Any]] = []
    for isl in (parsed.get("islands") or []):
        if not isinstance(isl, dict):
            continue
        islands_out.append(
            {
                "name": isl.get("name"),
                "icon": isl.get("icon"),
                "times": isl.get("times") or [],
                "is_major": bool(isl.get("is_major")),
                "rewards": [r.get("name") for r in (isl.get("rewards") or []) if isinstance(r, dict) and r.get("name")],
            }
        )

    return {
        "ok": True,
        "status": "ok",
        "game_date": game_date.isoformat(),
        "fetched_at": fetched_at.isoformat() if fetched_at else None,
        "cache_from": cache_from,
        "cache_to": cache_to,
        "presence": parsed.get("presence") or {},
        "rewards": rewards,
        "adventure_islands": islands_out,
    }

def _next_maintenance_date_kst(now: Optional[datetime] = None) -> date:
    n = now.astimezone(KST) if (now and now.tzinfo) else (now or datetime.now(KST))
    if n.weekday() == 2 and (n.hour, n.minute) < (10, 1):
        return n.date()
    days_ahead = (2 - n.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (n + timedelta(days=days_ahead)).date()


def _daypart_from_times(times: List[str]) -> str:
    best = ""
    for t in times or []:
        try:
            h, m = map(int, str(t).split(":"))
        except Exception:
            continue
        if h >= 18:
            return "저녁"
        if h >= 12:
            best = best or "오후"
        elif h >= 6:
            best = best or "오전"
        else:
            best = best or "새벽"
    return best


async def get_gold_islands_schedule_from_cache() -> Dict[str, Any]:
    game_date = _get_game_date()

    row = await _db_get_cache_row(CALENDAR_CACHE_KEY)
    if not row:
        return {
            "ok": False,
            "status": "no_cache",
            "game_date": game_date.isoformat(),
            "message": "현재 저장된 캘린더 캐시가 없습니다. 수요일 점검 이후(10:01 기준) 갱신이 완료되면 골드 보상 정보를 제공할 수 있어요.",
            "maintenance_date": _next_maintenance_date_kst().isoformat(),
            "schedule": [],
        }

    fetched_at = _coerce_dt(row.get("fetched_at"))
    valid_from = row.get("valid_from")
    valid_to = row.get("valid_to")

    cutoff = _refresh_cutoff_for_game_date(game_date)
    is_stale = (fetched_at is None) or (fetched_at < cutoff)
    out_of_range = (valid_to is not None) and (game_date > valid_to)

    maintenance_date = _next_maintenance_date_kst()
    if is_stale or out_of_range:
        last_txt = fetched_at.strftime("%Y-%m-%d %H:%M") if fetched_at else "알 수 없음"
        rng_txt = ""
        if valid_from and valid_to:
            rng_txt = f" (캐시 범위: {valid_from} ~ {valid_to})"
        return {
            "ok": False,
            "status": "not_updated",
            "game_date": game_date.isoformat(),
            "fetched_at": fetched_at.isoformat() if fetched_at else None,
            "maintenance_date": maintenance_date.isoformat(),
            "message": (
                "캘린더 데이터가 아직 최신으로 갱신되지 않았습니다. "
                "로스트아크 점검/업데이트 반영이 완료되면(수요일 10:01 기준) 자동으로 최신 일정/보상이 제공돼요. "
                f"마지막 캐시 갱신: {last_txt}{rng_txt}"
            ),
            "schedule": [],
        }

    payload_str = row.get("payload")
    try:
        payload = json.loads(payload_str) if payload_str else None
    except Exception:
        payload = None

    if not payload:
        return {
            "ok": False,
            "status": "bad_cache",
            "game_date": game_date.isoformat(),
            "fetched_at": fetched_at.isoformat() if fetched_at else None,
            "maintenance_date": maintenance_date.isoformat(),
            "message": "저장된 캘린더 캐시를 해석할 수 없습니다. 점검 이후 갱신이 정상 반영되면 다시 확인해주세요.",
            "schedule": [],
        }

    to_date = maintenance_date - timedelta(days=1)
    if valid_to is not None and to_date > valid_to:
        to_date = valid_to
    if to_date < game_date:
        to_date = game_date

    wd_map = ["월", "화", "수", "목", "금", "토", "일"]
    schedule: List[Dict[str, Any]] = []

    d = game_date
    while d <= to_date:
        parsed = parse_calendar_data(payload, d)

        island_icon_map: Dict[str, Optional[str]] = {}
        for isl in (parsed.get("islands") or []):
            if not isinstance(isl, dict):
                continue
            nm = isl.get("name")
            if not nm:
                continue
            island_icon_map[str(nm).strip()] = isl.get("icon")

        gold_arr = (parsed.get("reward_buckets") or {}).get("골드") or []
        for it in gold_arr:
            if not isinstance(it, dict):
                continue
            name = it.get("name")
            if not name:
                continue

            times = it.get("times") or []
            if not isinstance(times, list):
                times = []

            nm = str(name).strip()
            schedule.append(
                {
                    "date": d.isoformat(),
                    "weekday": wd_map[d.weekday()],
                    "label": _daypart_from_times([str(x) for x in times if x]),
                    "name": nm,
                    "icon": island_icon_map.get(nm),
                    "times": [str(x) for x in times if x],
                }
            )

        d = d + timedelta(days=1)

    return {
        "ok": True,
        "status": "ok",
        "game_date": game_date.isoformat(),
        "fetched_at": fetched_at.isoformat() if fetched_at else None,
        "maintenance_date": maintenance_date.isoformat(),
        "schedule": schedule,
    }


async def _dispatch_calendar(*, refresh_policy: str) -> None:
    if discord_service is None:
        return

    game_date = _get_game_date()
    payload = await get_calendar_payload_cached(target_date=game_date, refresh_policy=refresh_policy)
    if not payload:
        return

    parsed = parse_calendar_data(payload, game_date)
    drawer = CalendarDrawer()
    buf = await drawer.draw(game_date, parsed)

    file_bytes = buf.getvalue()
    filename = f"calendar_{game_date.strftime('%Y%m%d')}.png"

    try:
        async with get_db() as db:
            users_rows = await db.execute("SELECT user_id FROM user_subscriptions WHERE type='daily' AND enabled=1")
            users = [int(r["user_id"]) for r in (users_rows or [])]

            channels_rows = await db.execute(
                "SELECT guild_id, channel_id FROM guild_channel_subscriptions WHERE type='daily' AND enabled=1"
            )
            channels = [(int(r["guild_id"]), int(r["channel_id"])) for r in (channels_rows or [])]
    except Exception:
        return

    content = f"📅 **{game_date} 로스트아크 일정이에요.**"

    for uid in users:
        ok = await discord_service.send_dm(
            str(uid),
            content=content,
            file=discord_service.File(io.BytesIO(file_bytes), filename=filename),
        )
        if ok:
            await log_delivery(user_id=uid, guild_id=None, channel_id=None, game_date=game_date)

    for gid, cid in channels:
        ok = await discord_service.send_to_channel(
            str(cid),
            content=content,
            file=discord_service.File(io.BytesIO(file_bytes), filename=filename),
        )
        if ok:
            await log_delivery(user_id=None, guild_id=gid, channel_id=cid, game_date=game_date)
            
async def run_calendar_refresh_and_dispatch() -> None:
    if get_db is None or discord_service is None:
        return
    await refresh_calendar_cache()
    await _dispatch_calendar(refresh_policy="never")


async def run_calendar_dispatch_from_cache() -> None:
    if get_db is None or discord_service is None:
        return
    await _dispatch_calendar(refresh_policy="auto")


async def run_daily_fetch_and_dispatch() -> None:
    await run_calendar_dispatch_from_cache()

if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(run_daily_fetch_and_dispatch())
    except Exception:
        pass
