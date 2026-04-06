from __future__ import annotations

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED, JobExecutionEvent, JobEvent
from datetime import datetime, timezone, date
from zoneinfo import ZoneInfo
import asyncio, json
import random
import logging
import os
from typing import Dict, List, Any, Tuple, Optional, Callable, Awaitable

from database.connection import get_db
from services.discord_service import discord_service
from services.calendar_service import run_calendar_refresh_and_dispatch, run_calendar_dispatch_from_cache
from services.notice_service import run_notice_fetch_and_dispatch, purge_notice_cache
from services.youtube_service import run_youtube_fetch_and_dispatch, purge_youtube_cache
from utils.metrics import observe_scheduler_job, inc_scheduler_overlap

HOUR_RANGE: Tuple[int, int] = (3540, 3660)
MIN10_RANGE: Tuple[int, int] = (540, 660)
START_RANGE: Tuple[int, int] = (-60, 60)
EXPIRE_SEC: int = -300
KST = ZoneInfo("Asia/Seoul")
CACHE_TTL_SEC = 3 * 3600
PARTY_JOB_BUDGET_SEC = 55
QUIZ_JOB_BUDGET_SEC = 50
NOTIFY_GATHER_CHUNK = 30
QUIZ_DISPATCH_CHUNK = max(5, int(os.getenv("QUIZ_DISPATCH_CHUNK", "20")))
SENT_NOTIFICATIONS_CAP = 10000
SCHED_LAG_WARN_SEC = max(1.0, float(os.getenv("SCHED_LAG_WARN_SEC", "10")))
SCHED_LAG_CRIT_SEC = max(SCHED_LAG_WARN_SEC, float(os.getenv("SCHED_LAG_CRIT_SEC", "30")))
SCHED_DURATION_WARN_SEC = max(1.0, float(os.getenv("SCHED_DURATION_WARN_SEC", "40")))
SCHED_DURATION_CRIT_SEC = max(SCHED_DURATION_WARN_SEC, float(os.getenv("SCHED_DURATION_CRIT_SEC", "50")))
PARTY_DELETE_RETRY_DELAY_SEC = max(10.0, float(os.getenv("PARTY_DELETE_RETRY_DELAY_SEC", "60")))
PARTY_DELETE_RETRY_MAX_ATTEMPTS = max(1, int(os.getenv("PARTY_DELETE_RETRY_MAX_ATTEMPTS", "10")))
PARTY_AUDIT_MESSAGE_SNAPSHOT_LIMIT = max(100, int(os.getenv("PARTY_AUDIT_MESSAGE_SNAPSHOT_LIMIT", "1000")))
PARTY_AUDIT_MESSAGE_PERSIST_LIMIT = max(
    50, int(os.getenv("PARTY_AUDIT_MESSAGE_PERSIST_LIMIT", str(PARTY_AUDIT_MESSAGE_SNAPSHOT_LIMIT)))
)
logger = logging.getLogger(__name__)


def _is_real_user_id(uid: Any) -> bool:
    try:
        return str(uid).isdigit()
    except Exception:
        return False


class PartyScheduler:
    def __init__(self) -> None:
        self.scheduler = AsyncIOScheduler()
        self.sent_notifications: Dict[int, Dict[str, Any]] = {}
        self._job_locks: Dict[str, asyncio.Lock] = {}
        self._delete_retry_due: Dict[int, float] = {}
        self._delete_retry_attempts: Dict[int, int] = {}
        self.scheduler.add_listener(
            self._on_scheduler_event,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
        )

    def _ensure_notification_entry(self, party_id: int, now_ts: float) -> Dict[str, Any]:
        flags = self.sent_notifications.get(party_id)
        if flags is not None:
            return flags

        if len(self.sent_notifications) >= SENT_NOTIFICATIONS_CAP:
            overflow = len(self.sent_notifications) - SENT_NOTIFICATIONS_CAP + 1
            for pid, _ in sorted(
                self.sent_notifications.items(),
                key=lambda kv: float((kv[1] or {}).get("ts", 0.0)),
            )[:overflow]:
                self.sent_notifications.pop(pid, None)

        flags = {"hour": False, "min10": False, "start": False, "ts": now_ts}
        self.sent_notifications[party_id] = flags
        return flags

    def _get_job_lock(self, job_id: str) -> asyncio.Lock:
        lock = self._job_locks.get(job_id)
        if lock is None:
            lock = asyncio.Lock()
            self._job_locks[job_id] = lock
        return lock

    def _schedule_delete_retry(self, party_id: int) -> None:
        pid = int(party_id)
        attempt = int(self._delete_retry_attempts.get(pid, 0)) + 1
        self._delete_retry_attempts[pid] = attempt
        if attempt > PARTY_DELETE_RETRY_MAX_ATTEMPTS:
            logger.error(
                "party delete retry exhausted party_id=%s attempts=%s max=%s",
                pid,
                attempt - 1,
                PARTY_DELETE_RETRY_MAX_ATTEMPTS,
            )
            self._delete_retry_due.pop(pid, None)
            self._delete_retry_attempts.pop(pid, None)
            return
        self._delete_retry_due[pid] = asyncio.get_running_loop().time() + PARTY_DELETE_RETRY_DELAY_SEC

    def _clear_delete_retry(self, party_id: int) -> None:
        pid = int(party_id)
        self._delete_retry_due.pop(pid, None)
        self._delete_retry_attempts.pop(pid, None)

    def start(self) -> None:
        self.scheduler.add_job(
            self._run_check_parties,
            IntervalTrigger(minutes=1),
            id="party_check",
            coalesce=True,
            max_instances=1,
            misfire_grace_time=30,
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._run_check_quiz,
            CronTrigger(minute="*", timezone="Asia/Seoul"),
            id="quiz_dispatch",
            coalesce=True,
            max_instances=1,
            misfire_grace_time=30,
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._run_calendar_refresh_job,
            CronTrigger(day_of_week="wed", hour=10, minute=1, timezone="Asia/Seoul"),
            id="calendar_refresh_and_dispatch",
            coalesce=True,
            max_instances=1,
            misfire_grace_time=300,
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._run_calendar_cached_job,
            CronTrigger(day_of_week="mon,tue,thu,fri,sat,sun", hour=9, minute=0, timezone="Asia/Seoul"),
            id="calendar_dispatch_cached",
            coalesce=True,
            max_instances=1,
            misfire_grace_time=300,
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._run_notice_job,
            CronTrigger(minute="*", timezone="Asia/Seoul"),
            id="notice_minutely",
            kwargs={"initial": False},
            coalesce=True,
            max_instances=1,
            misfire_grace_time=30,
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._run_youtube_job,
            CronTrigger(minute="*", timezone="Asia/Seoul"),
            id="youtube_minutely",
            kwargs={"initial": False},
            coalesce=True,
            max_instances=1,
            misfire_grace_time=30,
            replace_existing=True,
        )
        self.scheduler.add_job(
            purge_youtube_cache,
            CronTrigger(hour=4, minute=5, timezone="Asia/Seoul"),
            id="youtube_cache_purge",
            kwargs={"days": 14},
            replace_existing=True,
        )
        self.scheduler.add_job(
            purge_notice_cache,
            CronTrigger(hour=4, minute=0, timezone="Asia/Seoul"),
            id="notice_cache_purge",
            kwargs={"days": 14},
            replace_existing=True,
        )
        self.scheduler.start()
        logger.info("Party scheduler started")

    def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown()

    def _on_scheduler_event(self, event: JobEvent) -> None:
        scheduled = getattr(event, "scheduled_run_time", None)
        lag_sec: Optional[float] = None
        if scheduled is not None:
            if getattr(scheduled, "tzinfo", None) is None:
                scheduled = scheduled.replace(tzinfo=timezone.utc)
            lag_sec = max(0.0, (datetime.now(timezone.utc) - scheduled).total_seconds())

        if event.code == EVENT_JOB_MISSED:
            logger.warning("scheduler job missed id=%s lag_sec=%.3f", event.job_id, lag_sec or 0.0)
            return

        if isinstance(event, JobExecutionEvent) and event.exception:
            logger.error("scheduler job error id=%s lag_sec=%.3f", event.job_id, lag_sec or 0.0)
            return

        if lag_sec is not None:
            if lag_sec >= SCHED_LAG_CRIT_SEC:
                logger.error(
                    "scheduler job lag critical id=%s lag_sec=%.3f warn=%.3f crit=%.3f",
                    event.job_id, lag_sec, SCHED_LAG_WARN_SEC, SCHED_LAG_CRIT_SEC
                )
            elif lag_sec >= SCHED_LAG_WARN_SEC:
                logger.warning(
                    "scheduler job lag warning id=%s lag_sec=%.3f warn=%.3f",
                    event.job_id, lag_sec, SCHED_LAG_WARN_SEC
                )
            else:
                logger.info("scheduler job lag id=%s lag_sec=%.3f", event.job_id, lag_sec)

    async def _run_with_duration_metric(self, job_id: str, fn: Callable[[], Awaitable[None]]) -> None:
        loop = asyncio.get_running_loop()
        lock = self._get_job_lock(job_id)
        if lock.locked():
            inc_scheduler_overlap(job_id)
            logger.warning("scheduler overlap skipped id=%s", job_id)
            return

        start = loop.time()
        status = "ok"
        try:
            async with lock:
                await fn()
        except Exception:
            status = "error"
            raise
        finally:
            elapsed = loop.time() - start
            observe_scheduler_job(job_id, status, elapsed)
            if elapsed >= SCHED_DURATION_CRIT_SEC:
                logger.error(
                    "scheduler job duration critical id=%s status=%s duration_sec=%.3f warn=%.3f crit=%.3f",
                    job_id, status, elapsed, SCHED_DURATION_WARN_SEC, SCHED_DURATION_CRIT_SEC
                )
            elif elapsed >= SCHED_DURATION_WARN_SEC:
                logger.warning(
                    "scheduler job duration warning id=%s status=%s duration_sec=%.3f warn=%.3f",
                    job_id, status, elapsed, SCHED_DURATION_WARN_SEC
                )
            else:
                logger.info("scheduler job duration id=%s status=%s duration_sec=%.3f", job_id, status, elapsed)

    async def _run_check_parties(self) -> None:
        await self._run_with_duration_metric("party_check", self.check_parties)

    async def _run_check_quiz(self) -> None:
        await self._run_with_duration_metric("quiz_dispatch", self.check_quiz)

    async def _run_notice_job(self, initial: bool = False) -> None:
        await self._run_with_duration_metric(
            "notice_minutely",
            lambda: run_notice_fetch_and_dispatch(initial=initial),
        )

    async def _run_calendar_refresh_job(self) -> None:
        await self._run_with_duration_metric("calendar_refresh_and_dispatch", run_calendar_refresh_and_dispatch)

    async def _run_calendar_cached_job(self) -> None:
        await self._run_with_duration_metric("calendar_dispatch_cached", run_calendar_dispatch_from_cache)

    async def _run_youtube_job(self, initial: bool = False) -> None:
        await self._run_with_duration_metric(
            "youtube_minutely",
            lambda: run_youtube_fetch_and_dispatch(initial=initial),
        )

    async def check_parties(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + PARTY_JOB_BUDGET_SEC
            parties, server_cfg_map, participants_map, live_party_ids = await self._fetch_db_info()

            tasks: List[Any] = []
            now_ts = datetime.now().timestamp()
            party_map: Dict[int, Dict[str, Any]] = {int(p["id"]): p for p in (parties or [])}
            queued_delete_ids: set[int] = set()

            for party in parties or []:
                party_id = party["id"]
                tdiff = int(party.get("tdiff") or 0)
                flags = self._ensure_notification_entry(party_id, now_ts)
                flags["ts"] = now_ts

                guild_id = str(party.get("guild_id") or "")
                cfg = server_cfg_map.get(guild_id) or {}
                mode_raw = cfg.get("alert_timer", 0)
                try:
                    alert_mode = int(mode_raw) & 0b11
                except (TypeError, ValueError):
                    alert_mode = 0

                enable_10m = (alert_mode & 0b01) != 0
                enable_1h = (alert_mode & 0b10) != 0
                alert_start = bool(cfg.get("alert_start", 0))
                chat_channel_id: Optional[str] = cfg.get("chat_channel_id")

                if enable_1h and HOUR_RANGE[0] <= tdiff <= HOUR_RANGE[1] and not flags["hour"]:
                    parts = participants_map.get(party_id, [])
                    tasks.append(self._send_notification_prefetched(party, parts, "1\uC2DC\uAC04", alert_start))
                    flags["hour"] = True
                elif enable_10m and MIN10_RANGE[0] <= tdiff <= MIN10_RANGE[1] and not flags["min10"]:
                    parts = participants_map.get(party_id, [])
                    tasks.append(self._send_notification_prefetched(party, parts, "10\uBD84", alert_start))
                    flags["min10"] = True
                elif START_RANGE[0] <= tdiff <= START_RANGE[1] and not flags["start"]:
                    parts = participants_map.get(party_id, [])
                    tasks.append(self._send_start_notification_prefetched(party, parts, chat_channel_id, alert_start))
                    flags["start"] = True
                elif tdiff <= EXPIRE_SEC:
                    if int(party_id) not in queued_delete_ids:
                        tasks.append(
                            self._delete_party_and_post(
                                party_id,
                                party.get("thread_manage_id"),
                                party=party,
                                participants=participants_map.get(party_id, []),
                            )
                        )
                        queued_delete_ids.add(int(party_id))
                    self.sent_notifications.pop(party_id, None)

            retry_party_ids = [pid for pid, due in list(self._delete_retry_due.items()) if due <= loop.time()]
            for pid in retry_party_ids:
                if int(pid) in queued_delete_ids:
                    continue
                p = party_map.get(int(pid))
                tasks.append(
                    self._delete_party_and_post(
                        int(pid),
                        (p or {}).get("thread_manage_id") if p else None,
                        party=p,
                        participants=participants_map.get(int(pid)) if p else None,
                    )
                )
                queued_delete_ids.add(int(pid))

            if not tasks:
                self._cleanup_notifications(live_party_ids or set())
                return

            for i in range(0, len(tasks), NOTIFY_GATHER_CHUNK):
                remaining = deadline - loop.time()
                if remaining <= 0:
                    logger.warning(
                        "party_check budget exceeded; remaining_tasks=%s",
                        len(tasks) - i,
                    )
                    break
                batch = tasks[i:i + NOTIFY_GATHER_CHUNK]
                await asyncio.wait_for(asyncio.gather(*batch, return_exceptions=True), timeout=remaining)
            self._cleanup_notifications(live_party_ids or set())

        except asyncio.TimeoutError:
            logger.warning("party_check timeout; cycle truncated")
        except Exception:
            logger.exception("Unhandled error in party_check")

    async def _fetch_db_info(self) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[int, List[Dict[str, Any]]], set]:
        async with get_db() as db:
            parties: List[Dict[str, Any]] = await db.execute(
                "SELECT p.id,p.title,p.message,p.start_date,p.owner,p.thread_manage_id,p.guild_id,"
                "r.dealer AS dealer_max,r.supporter AS supporter_max,r.name AS raid_name,r.difficulty,"
                "TIMESTAMPDIFF(SECOND, NOW(), p.start_date) AS tdiff "
                "FROM party p LEFT JOIN raid r ON p.raid_id = r.id "
                "WHERE p.start_date BETWEEN DATE_SUB(NOW(), INTERVAL 5 MINUTE) AND DATE_ADD(NOW(), INTERVAL 2 HOUR)"
            ) or []
            if not parties:
                return [], {}, {}, set()

            party_ids = [p["id"] for p in parties]
            live_party_ids = set(party_ids)
            guild_ids = list({str(p["guild_id"]) for p in parties if p.get("guild_id")})

            cfg_rows: List[Dict[str, Any]] = []
            server_cfg_map: Dict[str, Dict[str, Any]] = {}
            if guild_ids:
                marks = ",".join(["?"] * len(guild_ids))
                cfg_rows = await db.execute(
                    f"SELECT guild_id, alert_timer, alert_start, chat_channel_id FROM server WHERE guild_id IN ({marks})",
                    tuple(guild_ids),
                ) or []
                server_cfg_map = {str(r["guild_id"]): r for r in cfg_rows}

            participants_map: Dict[int, List[Dict[str, Any]]] = {pid: [] for pid in party_ids}
            if party_ids:
                marks = ",".join(["?"] * len(party_ids))
                part_rows = await db.execute(
                    f"SELECT p.party_id,p.user_id,p.role,c.char_name,c.item_lvl,c.combat_power,cl.name AS class_name "
                    f"FROM participants p "
                    f"LEFT JOIN `character` c ON p.character_id = c.id "
                    f"LEFT JOIN class cl ON c.class_id = cl.id "
                    f"WHERE p.party_id IN ({marks})",
                    tuple(party_ids),
                ) or []
                for row in part_rows:
                    participants_map[row["party_id"]].append(row)

        return parties, server_cfg_map, participants_map, live_party_ids

    async def _send_notification_prefetched(
        self, party: Dict[str, Any], participants: List[Dict[str, Any]], time_before: str, alert_start: bool
    ) -> None:
        try:
            if not participants:
                return
            embed_data = self._create_notification_embed(party, time_before)
            real_users = [p for p in participants if _is_real_user_id(p.get("user_id"))]
            mentions = " ".join(f"<@{p['user_id']}>" for p in real_users)
            tasks: List[Any] = []
            thread_id = party.get("thread_manage_id")
            if thread_id:
                tasks.append(discord_service.send_to_thread(thread_id, content=(mentions or None), embed=embed_data))
            if alert_start:
                for p in real_users:
                    tasks.append(discord_service.send_dm(p["user_id"], embed=embed_data))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            logger.exception("Notification send failed (party_id=%s)", party.get("id"))

    async def _send_start_notification_prefetched(
        self,
        party: Dict[str, Any],
        participants: List[Dict[str, Any]],
        chat_channel_id: Optional[str],
        alert_start: bool,
    ) -> None:
        try:
            party_id = int(party["id"])
            thread_id = party.get("thread_manage_id")
            thread_id_s = str(thread_id) if thread_id else None

            if not participants:
                await self._delete_party_and_post(
                    party_id,
                    thread_id_s,
                    party=party,
                    participants=participants,
                    notify_status="SKIP",
                    notify_detail={"reason": "no_participants"},
                )
                return

            party_data = {
                "id": party.get("id"),
                "title": party.get("title"),
                "raid_name": party.get("raid_name"),
                "difficulty": party.get("difficulty"),
                "dealer": party.get("dealer_max"),
                "supporter": party.get("supporter_max"),
                "owner": party.get("owner"),
                "participants": self._split_roles(participants),
            }
            try:
                embed_data = await discord_service._create_party_embed(party_data)
            except Exception:
                embed_data = self._create_notification_embed(party, "시작")

            real_users = [p for p in participants if _is_real_user_id(p.get("user_id"))]
            mentions = " ".join(f"<@{p['user_id']}>" for p in real_users)
            start_message = "\n-# **레이드 일정 시간이 되었어요!**"

            tasks: List[Any] = []
            if chat_channel_id:
                tasks.append(
                    discord_service.send_to_channel(
                        chat_channel_id,
                        content=((mentions + " ") if mentions else "") + start_message,
                        embed=embed_data,
                    )
                )

            dm_attempted = 0
            if alert_start and real_users:
                for p in real_users:
                    dm_attempted += 1
                    tasks.append(discord_service.send_dm(p["user_id"], content=start_message, embed=embed_data))

            notify_status = "SKIP"
            notify_detail: Dict[str, Any] = {
                "chat_channel_id": str(chat_channel_id) if chat_channel_id else None,
                "mention_count": len(real_users),
                "dm_attempted": dm_attempted,
            }

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                ok = sum(1 for r in results if not isinstance(r, Exception))
                fail = sum(1 for r in results if isinstance(r, Exception))
                notify_detail.update({"task_ok": ok, "task_fail": fail})
                notify_status = "OK" if fail == 0 else "FAIL"

            await self._delete_party_and_post(
                party_id,
                thread_id_s,
                party=party,
                participants=participants,
                notify_status=notify_status,
                notify_detail=notify_detail,
            )

        except Exception:
            logger.exception("Start notification failed (party_id=%s)", party.get("id"))

    async def _audit_upsert_party(
        self,
        *,
        party_id: int,
        guild_id: str,
        thread_id: Optional[str],
        party: Optional[Dict[str, Any]],
        status: str,
        detail: Dict[str, Any],
    ) -> Optional[int]:
        try:
            p = party or {}
            detail_s = json.dumps(detail, ensure_ascii=False, default=str)
            async with get_db() as db:
                await db.execute(
                    "INSERT INTO party_audit_event (party_id, guild_id, thread_id, party_title, raid_name, raid_difficulty, party_start_date, owner_id, status, detail) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(6), attempt_count = attempt_count + 1, "
                    "guild_id = VALUES(guild_id), thread_id = VALUES(thread_id), party_title = VALUES(party_title), raid_name = VALUES(raid_name), "
                    "raid_difficulty = VALUES(raid_difficulty), party_start_date = VALUES(party_start_date), owner_id = VALUES(owner_id), "
                    "status = VALUES(status), detail = VALUES(detail)",
                    (
                        int(party_id),
                        str(guild_id or ""),
                        str(thread_id) if thread_id else None,
                        str(p.get("title")) if p.get("title") is not None else None,
                        str(p.get("raid_name")) if p.get("raid_name") is not None else None,
                        str(p.get("difficulty")) if p.get("difficulty") is not None else None,
                        p.get("start_date"),
                        str(p.get("owner")) if p.get("owner") is not None else None,
                        str(status),
                        detail_s,
                    ),
                )
                row = await db.execute("SELECT id FROM party_audit_event WHERE party_id = ? LIMIT 1", (int(party_id),)) or []
                await db.commit()
                if row and row[0].get("id"):
                    return int(row[0]["id"])
        except Exception:
            logger.exception("Failed to upsert party audit row (party_id=%s)", party_id)
            return None
        return None

    async def _delete_party_and_post(
        self,
        party_id: int,
        thread_id: Optional[str] = None,
        *,
        party: Optional[Dict[str, Any]] = None,
        participants: Optional[List[Dict[str, Any]]] = None,
        notify_status: str = "SKIP",
        notify_detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        p = party or await self._fetch_party_for_audit(int(party_id)) or {"id": party_id}
        guild_id = str(p.get("guild_id") or "")
        thread_id_eff = thread_id or (str(p.get("thread_manage_id")) if p.get("thread_manage_id") else None)
        parts = participants if participants is not None else await self._fetch_participants_for_audit(int(party_id))

        snapshot_ok = False
        snapshot_count = 0
        snapshot_err = None
        snapshot_sampled = False
        msgs = None

        forum_delete_ok = True
        db_delete_ok = True

        try:
            if thread_id_eff:
                ok, m, err = await discord_service.fetch_channel_messages(
                    str(thread_id_eff), limit_total=PARTY_AUDIT_MESSAGE_SNAPSHOT_LIMIT, batch_size=100
                )
                snapshot_ok = bool(ok)
                msgs = m or []
                snapshot_count = len(msgs)
                snapshot_err = err
                if msgs and len(msgs) > PARTY_AUDIT_MESSAGE_PERSIST_LIMIT:
                    snapshot_sampled = True
                    step = max(1, len(msgs) // PARTY_AUDIT_MESSAGE_PERSIST_LIMIT)
                    msgs = msgs[::step][:PARTY_AUDIT_MESSAGE_PERSIST_LIMIT]

            if thread_id_eff:
                try:
                    forum_delete_ok = await discord_service.safe_delete_forum_post(str(thread_id_eff))
                except Exception:
                    logger.exception(
                        "Failed to delete forum post (party_id=%s, thread_id=%s)",
                        party_id,
                        thread_id_eff,
                    )
                    forum_delete_ok = False

            if thread_id_eff and not forum_delete_ok:
                db_delete_ok = False
            else:
                try:
                    async with get_db() as db:
                        await db.execute("DELETE FROM participants WHERE party_id = ?", (int(party_id),))
                        await db.execute("DELETE FROM party WHERE id = ?", (int(party_id),))
                        await db.commit()
                except Exception:
                    logger.exception("Failed to cleanup party rows (party_id=%s)", party_id)
                    db_delete_ok = False

        finally:
            retry_attempt = int(self._delete_retry_attempts.get(int(party_id), 0))
            overall_ok = (notify_status in ("OK", "SKIP")) and forum_delete_ok and db_delete_ok
            detail = {
                "notify_status": notify_status,
                "notify_detail": notify_detail,
                "snapshot_ok": snapshot_ok,
                "snapshot_count": snapshot_count,
                "snapshot_sampled": snapshot_sampled,
                "snapshot_err": snapshot_err,
                "forum_delete_ok": forum_delete_ok,
                "db_delete_ok": db_delete_ok,
                "delete_retry_attempt": retry_attempt,
                "participants": [
                    {
                        "user_id": str(x.get("user_id")),
                        "role": int(x.get("role") or 0),
                        "char_name": x.get("char_name"),
                        "class_name": x.get("class_name"),
                        "item_lvl": x.get("item_lvl"),
                        "combat_power": x.get("combat_power"),
                    }
                    for x in (parts or [])
                ],
            }

            ev_id = await self._audit_upsert_party(
                party_id=int(party_id),
                guild_id=guild_id,
                thread_id=str(thread_id_eff) if thread_id_eff else None,
                party=p,
                status="OK" if overall_ok else "FAIL",
                detail=detail,
            )

            if ev_id and msgs:
                await self._audit_insert_thread_messages(int(ev_id), msgs)

            if db_delete_ok:
                self.sent_notifications.pop(int(party_id), None)
                self._clear_delete_retry(int(party_id))
            else:
                self._schedule_delete_retry(int(party_id))

    def _create_notification_embed(self, party: Dict[str, Any], time_before: str) -> Dict[str, Any]:
        start_time = party.get("start_date")
        if isinstance(start_time, str):
            try:
                start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                start_dt = datetime.fromisoformat(start_time)
        else:
            start_dt = start_time
        time_str = start_dt.strftime("%Y-%m-%d %H:%M")
        embed = {
            "title": f"🔔 레이드 시작 {time_before} 전 알림",
            "description": f"**{party.get('raid_name')}** ({party.get('difficulty')})",
            "color": 0xFFD700,
            "fields": [
                {"name": "📅 시작 시간", "value": f"```{time_str}```", "inline": True},
                {"name": "⏰ 남은 시간", "value": f"```{time_before} 전```", "inline": True},
            ],
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {"text": "레이드 알림"},
        }
        if party.get("message"):
            embed["fields"].append({"name": "💬 메시지", "value": party["message"], "inline": False})
        return embed

    def _split_roles(self, participants: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        dealers: List[Dict[str, Any]] = []
        supporters: List[Dict[str, Any]] = []
        for row in participants:
            data = {
                "user_id": row["user_id"],
                "name": row.get("char_name"),
                "item_level": row.get("item_lvl"),
                "class_name": row.get("class_name"),
                "combat_power": row.get("combat_power"),
            }
            if row.get("role") == 0:
                dealers.append(data)
            elif row.get("role") == 1:
                supporters.append(data)
        return {"dealers": dealers, "supporters": supporters}

    def _cleanup_notifications(self, live_party_ids: set) -> None:
        now_ts = datetime.now().timestamp()
        to_remove = []
        for pid, meta in self.sent_notifications.items():
            if pid not in live_party_ids:
                to_remove.append(pid)
                continue
            ts = float(meta.get("ts", 0))
            if (now_ts - ts) > CACHE_TTL_SEC:
                to_remove.append(pid)
        for pid in to_remove:
            self.sent_notifications.pop(pid, None)

    async def check_quiz(self):
        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + QUIZ_JOB_BUDGET_SEC
            now_kst = datetime.now(tz=KST)
            hhmm = now_kst.strftime("%H:%M")
            today: date = now_kst.date()
            to_send: List[Dict[str, Any]] = []
            delivered: List[Tuple[Dict[str, Any], int]] = []
            async with get_db() as db:
                rows = await db.execute(
                    "SELECT s.guild_id, c.channel_id, s.hhmm, 1 AS is_schedule "
                    "FROM quiz_schedules s JOIN quiz_config c ON c.guild_id = s.guild_id "
                    "WHERE c.enabled = 1 AND s.enabled = 1 AND c.channel_id IS NOT NULL AND s.hhmm = ? AND (s.last_sent_date IS NULL OR s.last_sent_date < ?)",
                    (hhmm, today),
                ) or []
                if not rows:
                    rows = await db.execute(
                        "SELECT guild_id, channel_id, schedule_hhmm AS hhmm, 0 AS is_schedule "
                        "FROM quiz_config WHERE enabled = 1 AND channel_id IS NOT NULL AND schedule_hhmm = ?",
                        (hhmm,),
                    ) or []

                quiz_pool = await db.execute(
                    "SELECT id, question FROM quiz_bank WHERE enabled = 1"
                ) or []
                if not quiz_pool:
                    return

                for r in rows:
                    gid = str(r["guild_id"])
                    ch = r.get("channel_id")
                    if not ch:
                        continue
                    picked = random.choice(quiz_pool)
                    quiz_id, question = int(picked["id"]), picked["question"]
                    to_send.append(
                        {
                            "guild_id": gid,
                            "channel_id": str(ch),
                            "quiz_id": quiz_id,
                            "question": question,
                            "is_schedule": int(r.get("is_schedule", 0)),
                            "hhmm": r.get("hhmm"),
                        }
                    )

            if not to_send:
                return

            async def _send_quiz_target(item: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], int]]:
                try:
                    send_timeout = max(1.0, min(10.0, deadline - loop.time()))
                    msg_id = await asyncio.wait_for(
                        discord_service.send_quiz_message(
                            item["channel_id"],
                            item["question"],
                            guild_id=item["guild_id"],
                            quiz_id=item["quiz_id"],
                        ),
                        timeout=send_timeout,
                    )
                    if not msg_id:
                        return None
                    return item, int(msg_id)
                except Exception:
                    logger.exception(
                        "Quiz dispatch failed (guild_id=%s, channel_id=%s, quiz_id=%s)",
                        item.get("guild_id"),
                        item.get("channel_id"),
                        item.get("quiz_id"),
                    )
                    return None

            for i in range(0, len(to_send), QUIZ_DISPATCH_CHUNK):
                if loop.time() >= deadline:
                    logger.warning(
                        "quiz_dispatch budget exceeded; remaining_targets=%s",
                        len(to_send) - i,
                    )
                    break
                batch = to_send[i:i + QUIZ_DISPATCH_CHUNK]
                results = await asyncio.gather(
                    *[_send_quiz_target(item) for item in batch],
                    return_exceptions=True,
                )
                for res in results:
                    if isinstance(res, tuple) and len(res) == 2:
                        delivered.append((res[0], int(res[1])))

            if not delivered:
                return

            async with get_db() as db:
                try:
                    active_rows: List[Tuple[str, int, int, int]] = []
                    schedule_map: Dict[str, set[str]] = {}
                    for item, msg_id in delivered:
                        try:
                            active_rows.append(
                                (
                                    str(item["guild_id"]),
                                    int(msg_id),
                                    int(item["channel_id"]),
                                    int(item["quiz_id"]),
                                )
                            )
                            if int(item.get("is_schedule", 0)) == 1 and item.get("hhmm"):
                                schedule_map.setdefault(str(item["hhmm"]), set()).add(str(item["guild_id"]))
                        except Exception:
                            logger.exception(
                                "Quiz delivery normalize failed (guild_id=%s, message_id=%s, hhmm=%s)",
                                item.get("guild_id"),
                                msg_id,
                                item.get("hhmm"),
                            )

                    if active_rows:
                        values = ",".join(["(?, ?, ?, ?)"] * len(active_rows))
                        flat: List[Any] = []
                        for guild_id, message_id, channel_id, quiz_id in active_rows:
                            flat.extend([guild_id, message_id, channel_id, quiz_id])
                        await db.execute(
                            f"INSERT INTO quiz_active (guild_id, message_id, channel_id, quiz_id) VALUES {values}",
                            tuple(flat),
                        )

                    for hhmm_key, guild_ids in schedule_map.items():
                        gid_list = list(guild_ids)
                        for j in range(0, len(gid_list), 200):
                            chunk = gid_list[j:j + 200]
                            marks = ",".join(["?"] * len(chunk))
                            await db.execute(
                                f"UPDATE quiz_schedules SET last_sent_date = ? WHERE hhmm = ? AND guild_id IN ({marks})",
                                (today, hhmm_key, *chunk),
                            )
                except Exception:
                    logger.exception("Quiz DB batch write failed (delivered=%s)", len(delivered))
                await db.commit()

        except asyncio.TimeoutError:
            logger.warning("quiz_dispatch timeout; cycle truncated")
        except Exception:
            logger.exception("Unhandled error in quiz_dispatch")

    def _parse_discord_ts(self, s: Any) -> Optional[datetime]:
        if not s:
            return None
        try:
            t = str(s).replace("Z", "+00:00")
            dt = datetime.fromisoformat(t)
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return None

    async def _audit_insert_thread_messages(self, event_id: int, messages: List[Dict[str, Any]]) -> int:
        if not event_id or not messages:
            return 0

        async def _insert_chunk(db, chunk: List[tuple]) -> None:
            vals = ",".join(["(?, ?, ?, ?, ?, ?)"] * len(chunk))
            flat: List[Any] = []
            for r in chunk:
                flat.extend(list(r))
            await db.execute(
                f"INSERT IGNORE INTO party_audit_thread_message (event_id, message_id, author_id, created_at, content, raw_json) VALUES {vals}",
                tuple(flat),
            )

        inserted = 0
        try:
            async with get_db() as db:
                chunk: List[tuple] = []
                for m in messages:
                    mid = str(m.get("id") or "")
                    if not mid:
                        continue
                    author = m.get("author") or {}
                    author_id = str(author.get("id") or "") or None
                    created_at = self._parse_discord_ts(m.get("timestamp"))
                    content = m.get("content")
                    raw_json = json.dumps(m, ensure_ascii=False, default=str)
                    chunk.append((int(event_id), mid, author_id, created_at, content, raw_json))
                    if len(chunk) >= 50:
                        await _insert_chunk(db, chunk)
                        inserted += len(chunk)
                        chunk = []
                if chunk:
                    await _insert_chunk(db, chunk)
                    inserted += len(chunk)
                await db.commit()
        except Exception:
            logger.exception(
                "Failed to insert audit thread messages (event_id=%s, message_count=%s)",
                event_id,
                len(messages or []),
            )
            return inserted
        return inserted

    async def _fetch_party_for_audit(self, party_id: int) -> Optional[Dict[str, Any]]:
        async with get_db() as db:
            row = await db.execute(
                "SELECT p.id,p.title,p.message,p.start_date,p.owner,p.thread_manage_id,p.guild_id,r.name AS raid_name,r.difficulty "
                "FROM party p LEFT JOIN raid r ON p.raid_id = r.id WHERE p.id = ? LIMIT 1",
                (int(party_id),),
            ) or []
            return row[0] if row else None

    async def _fetch_participants_for_audit(self, party_id: int) -> List[Dict[str, Any]]:
        async with get_db() as db:
            rows = await db.execute(
                "SELECT p.party_id,p.user_id,p.role,c.char_name,c.item_lvl,c.combat_power,cl.name AS class_name "
                "FROM participants p LEFT JOIN `character` c ON p.character_id = c.id LEFT JOIN class cl ON c.class_id = cl.id "
                "WHERE p.party_id = ?",
                (int(party_id),),
            ) or []
            return rows


party_scheduler = PartyScheduler()
