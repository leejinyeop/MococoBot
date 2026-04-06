from typing import Dict, List, Optional, Tuple, Any
from database.connection import get_db, DatabaseManager
from services.discord_service import discord_service
from services.lostark_service import search_lostark_character
from utils.datetime_utils import parse_start_date
import asyncio
import hashlib
from utils.task_utils import fire_and_forget
import json
import logging
import contextlib
import os
import time

logger = logging.getLogger(__name__)

class PartyService:
    def __init__(self):
        self._discord_update_locks: Dict[int, asyncio.Lock] = {}
        self._discord_payload_fingerprint: Dict[int, Tuple[str, float]] = {}
        self._discord_fingerprint_cache_max = max(
            100,
            int(os.getenv("PARTY_DISCORD_FINGERPRINT_CACHE_MAX", "5000")),
        )
        self._discord_fingerprint_ttl_sec = max(
            5.0,
            float(os.getenv("PARTY_DISCORD_FINGERPRINT_TTL_SEC", "20")),
        )

    def _discord_update_coalesce_key(self, party_id: int) -> str:
        return f"party:discord_update:{int(party_id)}"

    def _waitlist_reconcile_coalesce_key(self, party_id: int) -> str:
        return f"party:waitlist_reconcile:{int(party_id)}"

    def _get_discord_update_lock(self, party_id: int) -> asyncio.Lock:
        pid = int(party_id)
        lock = self._discord_update_locks.get(pid)
        if lock is None:
            lock = asyncio.Lock()
            self._discord_update_locks[pid] = lock
        return lock

    def _build_discord_payload_fingerprint(self, party_info: Dict[str, Any]) -> str:
        payload = {
            "id": party_info.get("id"),
            "title": party_info.get("title"),
            "guild_id": party_info.get("guild_id"),
            "raid_id": party_info.get("raid_id"),
            "raid_name": party_info.get("raid_name"),
            "difficulty": party_info.get("difficulty"),
            "start_date": party_info.get("start_date"),
            "owner": party_info.get("owner"),
            "message": party_info.get("message"),
            "thread_manage_id": party_info.get("thread_manage_id"),
            "is_dealer_closed": party_info.get("is_dealer_closed"),
            "is_supporter_closed": party_info.get("is_supporter_closed"),
            "participants": party_info.get("participants") or {},
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _remember_discord_payload_fingerprint(self, party_id: int, fingerprint: str) -> None:
        pid = int(party_id)
        now = time.monotonic()
        if pid in self._discord_payload_fingerprint:
            self._discord_payload_fingerprint[pid] = (fingerprint, now)
            return
        if len(self._discord_payload_fingerprint) >= self._discord_fingerprint_cache_max:
            oldest = next(iter(self._discord_payload_fingerprint), None)
            if oldest is not None:
                self._discord_payload_fingerprint.pop(oldest, None)
                self._discord_update_locks.pop(int(oldest), None)
        self._discord_payload_fingerprint[pid] = (fingerprint, now)

    def _is_duplicate_discord_payload(self, party_id: int, fingerprint: str) -> bool:
        cached = self._discord_payload_fingerprint.get(int(party_id))
        if not cached:
            return False
        cached_fp, cached_ts = cached
        age_sec = max(0.0, time.monotonic() - float(cached_ts or 0.0))
        return cached_fp == fingerprint and age_sec <= self._discord_fingerprint_ttl_sec
    
    @staticmethod
    def _is_temp_user(user_id: str) -> bool:
        try:
            return str(user_id).startswith("TEMP-")
        except Exception:
            logger.debug("Failed to parse temp user flag (user_id=%r)", user_id, exc_info=True)
            return False
            
    async def get_participants_data(self, party_id: int) -> Dict[str, List]:
        """파티 참가자 조회 및 딜러/서폿 분리"""
        async with get_db() as db:
            return await self.get_participants_data_with_db(db, party_id)

    async def get_participants_data_with_db(self, db: DatabaseManager, party_id: int) -> Dict[str, List]:
        rows = await db.execute(
            """
            SELECT 
                p.id AS participant_id,
                p.role,
                p.user_id,
                c.class_id AS class_id,
                c.char_name AS name, 
                c.item_lvl AS item_level, 
                c.combat_power,
                cl.name AS class_name, 
                cl.emoji AS class_emoji,
                p.joined_at
            FROM participants p 
            LEFT JOIN `character` c ON p.character_id = c.id 
            LEFT JOIN class cl ON c.class_id = cl.id
            WHERE p.party_id = ?
            ORDER BY p.joined_at ASC
            """,
            (party_id,),
        )

        dealers: List[Dict[str, Any]] = []
        supporters: List[Dict[str, Any]] = []
        if not rows:
            return {"dealers": dealers, "supporters": supporters}

        d_append = dealers.append
        s_append = supporters.append
        for p in rows:
            data = {
                "participant_id": p["participant_id"],
                "user_id": p["user_id"],
                "class_id": p.get("class_id"),
                "name": p.get("name"),
                "item_level": p.get("item_level"),
                "combat_power": p.get("combat_power"),
                "class_name": p.get("class_name"),
                "class_emoji": p.get("class_emoji"),
            }
            j = p.get("joined_at")
            if j:
                try:
                    data["joined_at"] = j.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    logger.debug(
                        "Failed to format joined_at (party_id=%s, participant_id=%s, joined_at=%r)",
                        party_id,
                        p.get("participant_id"),
                        j,
                        exc_info=True,
                    )

            if p["role"] == 0:
                d_append(data)
            elif p["role"] == 1:
                s_append(data)

        return {"dealers": dealers, "supporters": supporters}

    async def create_party(self, guild_id: int, party_data: Dict) -> Dict:
        message_text = (party_data.get('message') or '')
        if len(message_text) > 200:
            return {"message": "message는 200자를 초과할 수 없습니다.", "status_code": 400}

        async with get_db() as db:
            raid_result = await db.execute(
                "SELECT id, dealer, supporter FROM raid WHERE name = ? AND difficulty = ?",
                (party_data['raid_name'], party_data['difficulty'])
            )
            if not raid_result:
                return {"message": "해당 레이드를 찾을 수 없습니다.", "status_code": 404}
            raid_info = raid_result[0]

            parsed_start_date = parse_start_date(party_data.get('start_date', ''))
            try:
                await db.execute("""
                    INSERT INTO party (title, raid_id, start_date, owner, message)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    party_data['title'],
                    raid_info['id'],
                    parsed_start_date,
                    party_data.get('owner_id'),
                    party_data.get('message', '')
                ))
                party_id = db.lastrowid

                # 디스코드 포스트 생성 시도
                ok, thread_id, err = await self._create_discord_post(
                    db, party_id, guild_id, party_data, raid_info
                )
                if not ok or not thread_id:
                    await db.rollback()
                    
                    return {
                        "message": "Discord 포럼글 생성 실패",
                        "status_code": 404,
                        "error": err or {"detail": "알 수 없는 오류"},
                        "party_id": party_id,
                    }

                # 모두 성공 > 커밋
                await db.commit()

                return {
                    "title": party_data['title'],
                    "start_date": parsed_start_date,
                    "party_id": party_id,
                    "thread_id": thread_id,
                    "discord_posted": True,
                    "message": "파티가 성공적으로 생성되었습니다."
                }

            except Exception as e:
                await db.rollback()
                logger.exception(
                    "Create party failed (guild_id=%s, title=%s, owner_id=%s)",
                    guild_id,
                    party_data.get("title"),
                    party_data.get("owner_id"),
                )
                return {
                    "message": "파티 생성 실패",
                    "status_code": 400,
                    "error": {"type": e.__class__.__name__, "detail": str(e)}
                }
        
    async def join_party(self, party_id: int, character_id: int, user_id: str, role: int = 0) -> Dict:
        async with get_db() as db:
            party_result = await db.execute(
                """
                SELECT 
                    p.raid_id,
                    p.guild_id,
                    p.is_dealer_closed,
                    p.is_supporter_closed,
                    p.title,
                    p.start_date,
                    p.thread_manage_id,
                    r.min_lvl,
                    r.dealer,
                    r.supporter,
                    r.name AS raid_name,
                    r.difficulty
                FROM party p
                JOIN raid r ON p.raid_id = r.id
                WHERE p.id = ?
                LIMIT 1
                """,
                (party_id,),
            )
            if not party_result:
                raise Exception("파티를 찾을 수 없습니다.")

            party_info = party_result[0]
            guild_id = str(party_info.get("guild_id") or "")

            try:
                stale_row = await db.execute(
                    """
                    SELECT char_name,
                           (updated_at IS NULL OR updated_at < (NOW() - INTERVAL 1 DAY)) AS is_stale
                    FROM `character`
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (character_id,),
                )
                if not stale_row:
                    raise Exception("캐릭터를 찾을 수 없습니다.")

                if stale_row[0].get("is_stale"):
                    api_data = await search_lostark_character(stale_row[0]["char_name"])
                    if api_data:
                        await db.execute(
                            """
                            UPDATE `character`
                               SET item_lvl = ?, combat_power = ?, updated_at = NOW()
                             WHERE id = ?
                            """,
                            (api_data.get("item_lvl"), api_data.get("combat_power"), character_id),
                        )
                        await db.commit()
            except Exception:
                logger.exception(
                    "Stale character refresh failed (party_id=%s, character_id=%s, user_id=%s)",
                    party_id,
                    character_id,
                    user_id,
                )

            character_result = await db.execute(
                "SELECT id, item_lvl, class_id FROM `character` WHERE id = ? LIMIT 1",
                (character_id,),
            )
            if not character_result:
                raise Exception("캐릭터를 찾을 수 없습니다.")

            min_lvl = float(party_info.get("min_lvl") or 0)
            char_item_lvl = float(character_result[0].get("item_lvl") or 0)
            if min_lvl > 0 and char_item_lvl < min_lvl:
                return {
                    "party_id": party_id,
                    "message": f"아이템 레벨이 충족되지 않습니다. (필요: {min_lvl}, 보유: {char_item_lvl})",
                    "status_code": 404,
                }

            role = int(role or 0)
            if role not in (0, 1):
                return {"party_id": party_id, "message": "role 값은 0(딜러) 또는 1(서포터)만 허용됩니다.", "status_code": 400}

            if not self._is_temp_user(user_id):
                exists = await db.execute(
                    "SELECT 1 FROM participants WHERE party_id = ? AND user_id = ? LIMIT 1",
                    (party_id, str(user_id)),
                )
                if exists:
                    return {"party_id": party_id, "message": "이미 참가한 파티입니다.", "status_code": 409}

            waitlist_enabled = False
            if guild_id:
                waitlist_enabled = await self._is_waitlist_enabled(db, guild_id)

            await db.execute("START TRANSACTION")

            lock_party = await db.execute("SELECT id FROM party WHERE id = ? FOR UPDATE", (party_id,))
            if not lock_party:
                await db.rollback()
                raise Exception("파티를 찾을 수 없습니다.")

            flags = await db.execute(
                "SELECT is_dealer_closed, is_supporter_closed FROM party WHERE id = ? FOR UPDATE",
                (party_id,),
            )
            dealer_closed = int(flags[0].get("is_dealer_closed") or 0)
            supporter_closed = int(flags[0].get("is_supporter_closed") or 0)

            if role == 0 and dealer_closed == 1:
                await db.rollback()
                return {"party_id": party_id, "message": "딜러 신청은 더 이상 받고 있지 않습니다.", "status_code": 400, "reason": "dealer_closed"}
            if role == 1 and supporter_closed == 1:
                await db.rollback()
                return {"party_id": party_id, "message": "서포터 신청은 더 이상 받고 있지 않습니다.", "status_code": 400, "reason": "supporter_closed"}

            max_count = int(party_info["dealer"] if role == 0 else party_info["supporter"]) or 0
            cnt_row = await db.execute(
                "SELECT COUNT(*) as cnt FROM participants WHERE party_id = ? AND role = ?",
                (party_id, role),
            )
            current_count = int(cnt_row[0]["cnt"]) if cnt_row else 0

            if current_count < max_count:
                await db.execute(
                    """
                    INSERT INTO participants (party_id, character_id, user_id, role, joined_at)
                    VALUES (?, ?, ?, ?, NOW())
                    """,
                    (party_id, character_id, str(user_id), role),
                )
                new_id = db.lastrowid
                await db.commit()

                fire_and_forget(
                    self.update_discord_after_change(party_id),
                    name="party:update_discord_after_join",
                    timeout_sec=20,
                    coalesce_key=self._discord_update_coalesce_key(party_id),
                )

                return {
                    "status_code": 201,
                    "joined": True,
                    "waitlisted": False,
                    "id": new_id,
                    "party_id": party_id,
                    "character_id": character_id,
                    "user_id": str(user_id),
                    "role": role,
                    "class_id": character_result[0].get("class_id"),
                    "message": "파티에 성공적으로 참가했습니다.",
                }

            if not waitlist_enabled:
                await db.rollback()
                message = "딜러 자리가 모두 찼습니다." if role == 0 else "서포터 자리가 모두 찼습니다."
                return {"party_id": party_id, "message": message, "status_code": 404}

            wl_row = await db.execute(
                """
                SELECT id
                FROM party_waitlist
                WHERE party_id = ? AND user_id = ? AND role = ?
                LIMIT 1
                """,
                (party_id, str(user_id), role),
            )
            if wl_row:
                wl_id = int(wl_row[0]["id"])
                pos = await self._get_waitlist_position(db, party_id, role, wl_id)
                await db.commit()
                fire_and_forget(
                    self._send_waitlist_registered_dm(party_id, str(user_id), pos, role),
                    name="party:waitlist_registered_dm",
                    timeout_sec=10,
                )
                return {
                    "status_code": 200,
                    "joined": False,
                    "waitlisted": True,
                    "party_id": party_id,
                    "user_id": str(user_id),
                    "role": role,
                    "waitlist_position": pos,
                    "message": "이미 대기열에 등록되어 있습니다.",
                }

            await db.execute(
                """
                INSERT INTO party_waitlist (party_id, character_id, user_id, role, created_at)
                VALUES (?, ?, ?, ?, NOW())
                """,
                (party_id, character_id, str(user_id), role),
            )
            wl_id = int(db.lastrowid or 0)
            pos = await self._get_waitlist_position(db, party_id, role, wl_id)
            await db.commit()

        fire_and_forget(
            self._send_waitlist_registered_dm(party_id, str(user_id), pos, role),
            name="party:waitlist_registered_dm",
            timeout_sec=10,
        )

        return {
            "status_code": 202,
            "joined": False,
            "waitlisted": True,
            "party_id": party_id,
            "user_id": str(user_id),
            "role": role,
            "waitlist_position": pos,
            "message": "정원이 가득 차 대기열로 등록되었습니다.",
        }

    async def purge_user_participations_in_guild(self, guild_id: str, user_id: str) -> Dict:
        async with get_db() as db:
            party_ids = []
            affected_rows = 0

            try:
                guild_exists = await db.execute(
                    "SELECT 1 FROM party WHERE guild_id = ? LIMIT 1",
                    (str(guild_id),)
                )
                if not guild_exists:
                    await db.execute("""
                        INSERT INTO server_leave_log
                          (guild_id, user_id, status, affected_rows, affected_parties, error_message, source)
                        VALUES (?, ?, 'SKIPPED', 0, ?, NULL, 'discord_member_remove')
                    """, (str(guild_id), str(user_id), json.dumps([])))
                    await db.commit()
                    return None

                cancel_rows = await db.execute("""
                    SELECT
                        p.id AS participant_id,
                        p.party_id,
                        p.user_id,
                        c.char_name,
                        party.title,
                        party.guild_id,
                        party.thread_manage_id
                    FROM participants p
                    LEFT JOIN `character` c ON p.character_id = c.id
                    LEFT JOIN party ON p.party_id = party.id
                    WHERE party.guild_id = ? AND p.user_id = ?
                """, (str(guild_id), str(user_id))) or []

                party_ids = sorted({row["party_id"] for row in cancel_rows}) if cancel_rows else []

                await db.execute("""
                    DELETE FROM participants
                    WHERE user_id = ?
                      AND party_id IN (SELECT id FROM party WHERE guild_id = ?)
                """, (str(user_id), str(guild_id)))

                affected_rows = int(getattr(db, "rowcount", 0) or 0)

                await db.execute("""
                    INSERT INTO server_leave_log
                      (guild_id, user_id, status, affected_rows, affected_parties, error_message, source)
                    VALUES (?, ?, 'DONE', ?, ?, NULL, 'discord_member_remove')
                """, (str(guild_id), str(user_id), affected_rows, json.dumps(party_ids)))

                await db.commit()

                if cancel_rows and affected_rows > 0:
                    for row in cancel_rows:
                        fire_and_forget(
                            self._send_cancel_notification(row),
                            name="party:cancel_notification",
                            timeout_sec=10,
                        )

                for pid in party_ids:
                    fire_and_forget(
                        self.update_discord_after_change(pid),
                        name="party:update_discord_after_purge",
                        timeout_sec=20,
                        coalesce_key=self._discord_update_coalesce_key(int(pid)),
                    )
                    fire_and_forget(
                        self.reconcile_waitlists_for_party(int(pid)),
                        name="party:reconcile_waitlist_after_purge",
                        timeout_sec=20,
                        coalesce_key=self._waitlist_reconcile_coalesce_key(int(pid)),
                    )

                return {
                    "guild_id": str(guild_id),
                    "user_id": str(user_id),
                    "deleted": True,
                    "affected_rows": affected_rows,
                    "affected_parties": party_ids,
                    "message": "해당 서버에서 유저의 모든 참가정보를 삭제했습니다."
                }

            except Exception as e:
                logger.exception(
                    "Failed to purge user participations in guild (guild_id=%s, user_id=%s)",
                    guild_id,
                    user_id,
                )
                with contextlib.suppress(Exception):
                    await db.rollback()
                with contextlib.suppress(Exception):
                    await db.execute("""
                        INSERT INTO server_leave_log
                          (guild_id, user_id, status, affected_rows, affected_parties, error_message, source)
                        VALUES (?, ?, 'ERROR', ?, ?, ?, 'discord_member_remove')
                    """, (str(guild_id), str(user_id), affected_rows, json.dumps(party_ids), str(e)))
                    await db.commit()
                raise
            
    async def leave_party(self, party_id: int, user_id: str = None, participant_id: int = None) -> Dict:
        async with get_db() as db:
            party_row = await db.execute("SELECT id, guild_id FROM party WHERE id = ? LIMIT 1", (party_id,))
            if not party_row:
                raise Exception("파티를 찾을 수 없습니다.")

            guild_id = str(party_row[0].get("guild_id") or "")

            cancel_user_info = None
            left_role = None

            if participant_id:
                cancel_user_result = await db.execute(
                    """
                    SELECT 
                        p.user_id,
                        p.role,
                        c.char_name,
                        party.title,
                        party.guild_id,
                        party.thread_manage_id
                    FROM participants p
                    LEFT JOIN `character` c ON p.character_id = c.id
                    LEFT JOIN party ON p.party_id = party.id
                    WHERE p.party_id = ? AND p.id = ?
                    LIMIT 1
                    """,
                    (party_id, participant_id),
                )
                cancel_user_info = cancel_user_result[0] if cancel_user_result else None
            elif user_id:
                cancel_user_result = await db.execute(
                    """
                    SELECT 
                        p.user_id,
                        p.role,
                        c.char_name,
                        party.title,
                        party.guild_id,
                        party.thread_manage_id
                    FROM participants p
                    LEFT JOIN `character` c ON p.character_id = c.id
                    LEFT JOIN party ON p.party_id = party.id
                    WHERE p.party_id = ? AND p.user_id = ?
                    LIMIT 1
                    """,
                    (party_id, str(user_id)),
                )
                cancel_user_info = cancel_user_result[0] if cancel_user_result else None
            else:
                raise Exception("participant_id 또는 user_id가 필요합니다.")

            if cancel_user_info:
                try:
                    left_role = int(cancel_user_info.get("role") or 0)
                except Exception:
                    logger.debug(
                        "Failed to parse left role (party_id=%s, user_id=%s, raw_role=%r)",
                        party_id,
                        cancel_user_info.get("user_id"),
                        cancel_user_info.get("role"),
                        exc_info=True,
                    )
                    left_role = 0

            await db.execute("START TRANSACTION")
            await db.execute("SELECT id FROM party WHERE id = ? FOR UPDATE", (party_id,))
            if participant_id:
                await db.execute("DELETE FROM participants WHERE party_id = ? AND id = ?", (party_id, participant_id))
                identifier = participant_id
            else:
                await db.execute("DELETE FROM participants WHERE party_id = ? AND user_id = ?", (party_id, str(user_id)))
                identifier = str(user_id)

            affected_rows = int(getattr(db, "rowcount", 0) or 0)
            if affected_rows <= 0:
                waitlisted = False
                if user_id:
                    wl = await db.execute(
                        """
                        SELECT 1
                        FROM party_waitlist
                        WHERE party_id = ? AND user_id = ?
                        LIMIT 1
                        """,
                        (party_id, str(user_id)),
                    )
                    waitlisted = bool(wl)
                await db.rollback()
                if waitlisted:
                    return {
                        "party_id": party_id,
                        "identifier": identifier,
                        "deleted": False,
                        "affected_rows": 0,
                        "message": "대기열 참가 상태입니다. 대기열 취소를 이용해주세요.",
                        "status_code": 404,
                    }
                return {
                    "party_id": party_id,
                    "identifier": identifier,
                    "deleted": False,
                    "affected_rows": 0,
                    "message": "파티 참가자가 아닙니다.",
                    "status_code": 404,
                }

            promoted = None
            if affected_rows > 0 and left_role in (0, 1) and guild_id and await self._is_waitlist_enabled(db, guild_id):
                promoted = await self._promote_next_waiter_locked(db, party_id, left_role)

            await db.commit()

            if cancel_user_info and affected_rows > 0:
                fire_and_forget(
                    self._send_cancel_notification(cancel_user_info),
                    name="party:cancel_notification",
                    timeout_sec=10,
                )

            fire_and_forget(
                self.update_discord_after_change(party_id),
                name="party:update_discord_after_leave",
                timeout_sec=20,
                coalesce_key=self._discord_update_coalesce_key(party_id),
            )

            if promoted:
                fire_and_forget(
                    self._send_waitlist_promoted_dm(party_id, promoted["user_id"], promoted["role"]),
                    name="party:waitlist_promoted_dm",
                    timeout_sec=10,
                )

            return {
                "party_id": party_id,
                "identifier": identifier,
                "deleted": True,
                "affected_rows": affected_rows,
                "promoted": bool(promoted),
                "promoted_user_id": promoted["user_id"] if promoted else None,
                "message": "파티에서 성공적으로 나갔습니다.",
            }

    async def delete_party(self, party_id: int) -> Dict:
        """파티 삭제"""
        async with get_db() as db:
            party_info = await db.execute("SELECT thread_manage_id FROM party WHERE id = ?", (party_id,))
            if not party_info:
                raise Exception("파티를 찾을 수 없습니다.")

            thread_id = party_info[0].get("thread_manage_id")

            await db.execute("DELETE FROM participants WHERE party_id = ?", (party_id,))
            await db.execute("DELETE FROM party_waitlist WHERE party_id = ?", (party_id,))
            await db.execute("DELETE FROM party WHERE id = ?", (party_id,))
            await db.commit()
            self._discord_payload_fingerprint.pop(int(party_id), None)
            self._discord_update_locks.pop(int(party_id), None)

            if thread_id:
                fire_and_forget(
                    discord_service.delete_forum_post(thread_id),
                    name="party:delete_forum_post_on_delete",
                    timeout_sec=20,
                )

            return {"party_id": party_id, "message": "파티가 성공적으로 삭제되었습니다."}

    async def update_party(self, party_id: int, party_data: Dict) -> Dict:
        """파티 정보 수정"""
        async with get_db() as db:
            # 기존 파티/스레드/길드 조회
            party_result = await db.execute(
                "SELECT id, thread_manage_id, guild_id FROM party WHERE id = ?",
                (party_id,)
            )
            if not party_result:
                raise Exception("파티를 찾을 수 없습니다.")
            old_thread_id = party_result[0].get("thread_manage_id")
            current_guild_id = party_result[0].get("guild_id")

            # 레이드 조회
            raid_result = await db.execute(
                "SELECT id, dealer, supporter FROM raid WHERE name = ? AND difficulty = ?",
                (party_data['raid_name'], party_data['difficulty'])
            )
            if not raid_result:
                raise Exception("해당 레이드를 찾을 수 없습니다.")
            raid_info = raid_result[0]

            # 시작일 파싱 후 기본 필드 업데이트 (스레드 필드는 건드리지 않음)
            parsed_start_date = parse_start_date(party_data.get('start_date', ''))
            await db.execute(
                """
                UPDATE party
                SET title = ?, raid_id = ?, start_date = ?, owner = ?, message = ?
                WHERE id = ?
                """,
                (
                    party_data['title'],
                    raid_info['id'],
                    parsed_start_date,
                    party_data.get('owner_id'),
                    party_data.get('message', ''),
                    party_id,
                ),
            )
            await db.commit()

            # "새 스레드 먼저" 생성 시도 (guild_id 우선순위: 요청값 -> 기존값)
            target_guild_id = party_data.get('guild_id') or current_guild_id
            # 생성 실패하면 False, None 반환
            discord_ok, new_thread_id, _ = await self._create_discord_post(
                db, party_id, target_guild_id, party_data, raid_info
            )

            # 새 스레드 생성이 "성공한 경우에만" 교체
            if discord_ok and new_thread_id:
                # DB에 재확인 저장
                await db.execute(
                    "UPDATE party SET thread_manage_id = ?, guild_id = ? WHERE id = ?",
                    (str(new_thread_id), str(target_guild_id), party_id),
                )
                await db.commit()

                # 기존 스레드는 "나중에" 비동기 삭제
                if old_thread_id and str(old_thread_id) != str(new_thread_id):
                    fire_and_forget(
                        discord_service.delete_forum_post(old_thread_id),
                        name="party:delete_old_forum_post_on_update",
                        timeout_sec=20,
                    )

                return {
                    "party_id": party_id,
                    "updated": True,
                    "discord_created": True,
                    "thread_id": new_thread_id,
                    "message": "파티 정보가 성공적으로 수정되었습니다."
                }

            # 6) 새 스레드 생성 실패 시: 기존 thread_manage_id 유지, 삭제도 하지 않음
            return {
                "party_id": party_id,
                "updated": True,
                "discord_created": False,
                "thread_id": old_thread_id,
                "message": "파티 기본 정보만 수정되었으며, 새 스레드 생성은 실패했습니다."
            }
    
    async def update_discord_after_change(self, party_id: int):
        """파티 변경 후 Discord 업데이트"""
        try:
            pid = int(party_id)
            lock = self._get_discord_update_lock(pid)
            async with lock:
                party_info = None
                async with get_db() as db:
                    party_result = await db.execute("""
                        SELECT
                            p.id, p.title, p.guild_id, p.raid_id, p.start_date, p.owner, p.message,
                            p.thread_manage_id, p.is_dealer_closed, p.is_supporter_closed, NULL AS created_at, NULL AS updated_at,
                            r.name AS raid_name, r.difficulty, r.dealer, r.supporter
                        FROM party p LEFT JOIN raid r ON p.raid_id = r.id 
                        WHERE p.id = ?
                    """, (pid,))

                    if party_result:
                        party_info = party_result[0]
                        party_info["participants"] = await self.get_participants_data_with_db(db, pid)

                if not party_info:
                    self._discord_payload_fingerprint.pop(pid, None)
                    self._discord_update_locks.pop(pid, None)
                    return

                thread_id = party_info.get("thread_manage_id")
                if not thread_id:
                    self._discord_payload_fingerprint.pop(pid, None)
                    self._discord_update_locks.pop(pid, None)
                    return

                fingerprint = self._build_discord_payload_fingerprint(party_info)
                if self._is_duplicate_discord_payload(pid, fingerprint):
                    logger.info("Skip duplicate discord update (party_id=%s)", pid)
                    return

                await discord_service.update_forum_post(thread_id, party_info)
                self._remember_discord_payload_fingerprint(pid, fingerprint)

        except Exception:
            logger.exception("Discord update after party change failed (party_id=%s)", party_id)
    
    async def _create_discord_post(
        self, db: DatabaseManager, party_id: int,
        guild_id: int, party_data: Dict, raid_info: Dict
    ) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        """Discord 포럼 포스트 생성"""
        def _shorten(obj: Any, limit: int = 500) -> str:
            try:
                s = json.dumps(obj, ensure_ascii=False, default=str)
            except Exception:
                logger.debug("Failed to json-dump discord result preview", exc_info=True)
                s = str(obj)
            return s if len(s) <= limit else s[:limit] + "...(truncated)"

        try:
            # 서버 포럼 채널 조회
            server_result = await db.execute(
                "SELECT forum_channel_id FROM server WHERE guild_id = ?",
                (str(guild_id),)
            )
            if not server_result or not server_result[0].get('forum_channel_id'):
                err = {
                    "stage": "lookup_forum_channel",
                    "detail": "서버에 forum_channel_id가 설정되어 있지 않음",
                    "guild_id": str(guild_id),
                }
                return False, None, err

            forum_channel_id = server_result[0]['forum_channel_id']

            # 디스코드 전송 페이로드
            discord_party_data = {
                "id": party_id,
                "title": party_data['title'],
                "raid_name": party_data['raid_name'],
                "difficulty": party_data['difficulty'],
                "message": party_data.get('message', ''),
                "dealer": raid_info['dealer'],
                "supporter": raid_info['supporter'],
                "owner": party_data.get('owner_id', ''),
                "participants": await self.get_participants_data_with_db(db, party_id),
            }

            # 포럼 포스트 생성
            result = await discord_service.create_forum_post(
                str(forum_channel_id), discord_party_data, str(guild_id)
            )

            if not isinstance(result, dict):
                err = {
                    "stage": "discord_create_forum_post",
                    "detail": "discord_service가 dict를 반환하지 않음",
                    "result_type": type(result).__name__,
                }
                return False, None, err

            if 'id' not in result:
                err = {
                    "stage": "discord_create_forum_post",
                    "detail": "응답에 thread id가 없음",
                    "result_preview": _shorten(result),
                }
                return False, None, err

            thread_id = str(result['id'])

            # 성공 시에만 DB에 기록
            await db.execute(
                "UPDATE party SET thread_manage_id = ?, guild_id = ? WHERE id = ?",
                (thread_id, str(guild_id), party_id),
            )
            await db.commit()

            return True, thread_id, None

        except Exception as e:
            logger.exception(
                "Discord post creation failed (party_id=%s, guild_id=%s)",
                party_id,
                guild_id,
            )

            err = {
                "stage": "exception",
                "detail": str(e),
                "type": e.__class__.__name__,
            }
            return False, None, err

    
    async def _is_waitlist_enabled(self, db: DatabaseManager, guild_id: str) -> bool:
        row = await db.execute(
            "SELECT waitlist_use FROM server WHERE guild_id = ? LIMIT 1",
            (str(guild_id),),
        )
        if not row:
            return False
        try:
            return int(row[0].get("waitlist_use") or 0) == 1
        except Exception:
            logger.debug("Failed to parse waitlist_use flag (guild_id=%s, row=%r)", guild_id, row[0], exc_info=True)
            return False

    async def _get_waitlist_position(self, db: DatabaseManager, party_id: int, role: int, wait_id: int) -> int:
        row = await db.execute(
            """
            SELECT COUNT(*) AS c
            FROM party_waitlist
            WHERE party_id = ? AND role = ? AND id <= ?
            """,
            (party_id, int(role), int(wait_id)),
        )
        try:
            return int(row[0]["c"] or 0)
        except Exception:
            logger.debug(
                "Failed to parse waitlist position count (party_id=%s, role=%s, wait_id=%s, row=%r)",
                party_id,
                role,
                wait_id,
                row[0] if row else None,
                exc_info=True,
            )
            return 0

    async def _get_party_dm_info(self, db: DatabaseManager, party_id: int) -> Optional[Dict[str, Any]]:
        row = await db.execute(
            """
            SELECT 
                p.id,
                p.title,
                p.start_date,
                p.guild_id,
                p.thread_manage_id,
                r.name AS raid_name,
                r.difficulty
            FROM party p
            LEFT JOIN raid r ON p.raid_id = r.id
            WHERE p.id = ?
            LIMIT 1
            """,
            (party_id,),
        )
        return row[0] if row else None

    def _format_start_date(self, dt: Any) -> str:
        if dt is None:
            return "-"
        try:
            return dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            logger.debug("Failed to format start date (dt=%r)", dt, exc_info=True)
            return str(dt)

    def _build_party_link(self, party: Dict[str, Any]) -> Optional[str]:
        try:
            gid = str(party.get("guild_id") or "")
            tid = str(party.get("thread_manage_id") or "")
            if gid.isdigit() and tid.isdigit():
                return f"https://discord.com/channels/{gid}/{tid}"
        except Exception:
            logger.debug("Failed to build party link (party=%r)", party, exc_info=True)
        return None

    async def _send_waitlist_registered_dm(self, party_id: int, user_id: str, position: int, role: int) -> None:
        if not str(user_id).isdigit():
            return
        async with get_db() as db:
            party = await self._get_party_dm_info(db, party_id)
        if not party:
            return

        role_name = "딜러" if int(role) == 0 else "서포터"
        start_text = self._format_start_date(party.get("start_date"))
        link = self._build_party_link(party)
        title_text = party.get("title") or "파티"

        desc = f"**{title_text}**\n{role_name} 자리가 현재 정원 초과라서 **{int(position)}번째 대기열**로 등록되었습니다.\n\n자리가 생기면 자동으로 참가가 확정되고, DM으로 알려드릴게요."
        if link:
            desc = desc + f"\n\n파티 링크: {link}"

        embed = {
            "title": "대기열 등록 완료",
            "description": desc,
            "color": 0x5865F2,
            "fields": [
                {"name": "일정", "value": start_text, "inline": True},
                {"name": "레이드", "value": f"{party.get('raid_name') or '-'} {party.get('difficulty') or ''}".strip(), "inline": True},
                {"name": "대기 순번", "value": f"{int(position)}번", "inline": True},
            ],
        }
        await discord_service.send_dm(str(user_id), embed=embed)

    async def _send_waitlist_promoted_dm(self, party_id: int, user_id: str, role: int) -> None:
        if not str(user_id).isdigit():
            return
        async with get_db() as db:
            party = await self._get_party_dm_info(db, party_id)
        if not party:
            return

        role_name = "딜러" if int(role) == 0 else "서포터"
        start_text = self._format_start_date(party.get("start_date"))
        link = self._build_party_link(party)
        title_text = party.get("title") or "파티"

        desc = f"대기열에서 순번이 돌아와 **참가가 확정**되었습니다.\n\n**{title_text}**\n역할: **{role_name}**\n일정: **{start_text}**"
        if link:
            desc = desc + f"\n\n파티 링크: {link}"

        embed = {
            "title": "참가 확정",
            "description": desc,
            "color": 0x57F287,
        }
        await discord_service.send_dm(str(user_id), embed=embed)

    async def _promote_next_waiter_locked(self, db: DatabaseManager, party_id: int, role: int) -> Optional[Dict[str, Any]]:
        w = await db.execute(
            """
            SELECT id, user_id, character_id, role
            FROM party_waitlist
            WHERE party_id = ? AND role = ?
            ORDER BY id ASC
            LIMIT 1
            FOR UPDATE
            """,
            (party_id, int(role)),
        )
        if not w:
            return None

        w0 = w[0]
        uid = str(w0.get("user_id") or "")
        cid = int(w0.get("character_id") or 0)
        r = int(w0.get("role") or 0)

        if not self._is_temp_user(uid):
            dup = await db.execute(
                "SELECT 1 FROM participants WHERE party_id = ? AND user_id = ? LIMIT 1",
                (party_id, uid),
            )
            if dup:
                await db.execute("DELETE FROM party_waitlist WHERE id = ?", (int(w0["id"]),))
                return None

        await db.execute(
            """
            INSERT INTO participants (party_id, character_id, user_id, role, joined_at)
            VALUES (?, ?, ?, ?, NOW())
            """,
            (party_id, cid, uid, r),
        )
        await db.execute("DELETE FROM party_waitlist WHERE id = ?", (int(w0["id"]),))

        return {"user_id": uid, "role": r}

    async def get_waitlist(self, party_id: int, role: Optional[int] = None) -> Dict[str, List[Dict[str, Any]]]:
        async with get_db() as db:
            where = "WHERE w.party_id = ?"
            params: list[Any] = [party_id]
            if role in (0, 1):
                where += " AND w.role = ?"
                params.append(int(role))

            rows = await db.execute(
                f"""
                SELECT 
                    w.id AS wait_id,
                    w.role,
                    w.user_id,
                    w.created_at,
                    c.id AS character_id,
                    c.char_name,
                    c.item_lvl,
                    c.combat_power,
                    cl.name AS class_name,
                    cl.emoji AS class_emoji
                FROM party_waitlist w
                LEFT JOIN `character` c ON w.character_id = c.id
                LEFT JOIN class cl ON c.class_id = cl.id
                {where}
                ORDER BY w.role ASC, w.id ASC
                """,
                tuple(params),
            ) or []

        dealers: List[Dict[str, Any]] = []
        supporters: List[Dict[str, Any]] = []
        d_pos = 0
        s_pos = 0

        for r in rows:
            rr = int(r.get("role") or 0)
            if rr == 0:
                d_pos += 1
                dealers.append(
                    {
                        "position": d_pos,
                        "wait_id": r.get("wait_id"),
                        "user_id": r.get("user_id"),
                        "character_id": r.get("character_id"),
                        "name": r.get("char_name"),
                        "item_level": r.get("item_lvl"),
                        "combat_power": r.get("combat_power"),
                        "class_name": r.get("class_name"),
                        "class_emoji": r.get("class_emoji"),
                    }
                )
            else:
                s_pos += 1
                supporters.append(
                    {
                        "position": s_pos,
                        "wait_id": r.get("wait_id"),
                        "user_id": r.get("user_id"),
                        "character_id": r.get("character_id"),
                        "name": r.get("char_name"),
                        "item_level": r.get("item_lvl"),
                        "combat_power": r.get("combat_power"),
                        "class_name": r.get("class_name"),
                        "class_emoji": r.get("class_emoji"),
                    }
                )

        return {"dealers": dealers, "supporters": supporters}

    async def get_waitlist_my_position(self, party_id: int, user_id: str, role: int) -> Dict[str, Any]:
        role = int(role or 0)
        async with get_db() as db:
            row = await db.execute(
                """
                SELECT id
                FROM party_waitlist
                WHERE party_id = ? AND user_id = ? AND role = ?
                LIMIT 1
                """,
                (party_id, str(user_id), role),
            )
            if not row:
                return {"party_id": party_id, "user_id": str(user_id), "role": role, "in_waitlist": False}
            wid = int(row[0]["id"])
            pos = await self._get_waitlist_position(db, party_id, role, wid)
            return {
                "party_id": party_id,
                "user_id": str(user_id),
                "role": role,
                "in_waitlist": True,
                "wait_id": wid,
                "position": pos,
            }

    async def cancel_waitlist(self, party_id: int, user_id: str, role: int = 0) -> Dict[str, Any]:
        role = int(role or 0)
        async with get_db() as db:
            await db.execute(
                "DELETE FROM party_waitlist WHERE party_id = ? AND user_id = ? AND role = ?",
                (party_id, str(user_id), role),
            )
            affected = int(getattr(db, "rowcount", 0) or 0)
            await db.commit()
        return {
            "status_code": 200,
            "party_id": party_id,
            "user_id": str(user_id),
            "role": role,
            "deleted": affected > 0,
            "affected_rows": affected,
        }

    async def reconcile_waitlists_for_party(self, party_id: int) -> Dict[str, Any]:
        promotions: List[Dict[str, Any]] = []
        async with get_db() as db:
            pr = await db.execute(
                """
                SELECT 
                    p.id,
                    p.guild_id,
                    r.dealer,
                    r.supporter
                FROM party p
                JOIN raid r ON p.raid_id = r.id
                WHERE p.id = ?
                LIMIT 1
                """,
                (party_id,),
            )
            if not pr:
                return {"party_id": party_id, "promoted": 0}

            guild_id = str(pr[0].get("guild_id") or "")
            if not guild_id or not await self._is_waitlist_enabled(db, guild_id):
                return {"party_id": party_id, "promoted": 0}

            max_dealer = int(pr[0].get("dealer") or 0)
            max_supporter = int(pr[0].get("supporter") or 0)

            await db.execute("START TRANSACTION")
            await db.execute("SELECT id FROM party WHERE id = ? FOR UPDATE", (party_id,))

            for r in (0, 1):
                max_count = max_dealer if r == 0 else max_supporter
                if max_count <= 0:
                    continue

                cnt_row = await db.execute(
                    "SELECT COUNT(*) as cnt FROM participants WHERE party_id = ? AND role = ?",
                    (party_id, r),
                )
                cur = int(cnt_row[0]["cnt"]) if cnt_row else 0

                while cur < max_count:
                    promoted = await self._promote_next_waiter_locked(db, party_id, r)
                    if not promoted:
                        break
                    promotions.append(promoted)
                    cur += 1

            await db.commit()

        if promotions:
            fire_and_forget(
                self.update_discord_after_change(party_id),
                name="party:update_discord_after_waitlist_reconcile",
                timeout_sec=20,
                coalesce_key=self._discord_update_coalesce_key(party_id),
            )
            for p in promotions:
                fire_and_forget(
                    self._send_waitlist_promoted_dm(party_id, p["user_id"], p["role"]),
                    name="party:waitlist_promoted_dm",
                    timeout_sec=10,
                )

        return {"party_id": party_id, "promoted": len(promotions), "promotions": promotions}

    async def reconcile_waitlists_tick(self, max_parties: int = 50) -> Dict[str, Any]:
        try:
            max_parties = int(max_parties or 50)
        except Exception:
            logger.warning("Invalid max_parties value; fallback to default 50 (value=%r)", max_parties)
            max_parties = 50
        if max_parties <= 0:
            max_parties = 50

        async with get_db() as db:
            rows = await db.execute(
                "SELECT DISTINCT party_id FROM party_waitlist ORDER BY party_id DESC LIMIT ?",
                (max_parties,),
            ) or []

        promoted_total = 0
        parties = 0
        for r in rows:
            try:
                pid = int(r.get("party_id"))
            except Exception:
                logger.debug("Invalid party_id in waitlist tick row; skipped row=%r", r, exc_info=True)
                continue
            parties += 1
            res = await self.reconcile_waitlists_for_party(pid)
            promoted_total += int(res.get("promoted") or 0)

        return {"checked_parties": parties, "promoted_total": promoted_total}

    async def _check_party_capacity(self, db: DatabaseManager, party_id: int, 
                            raid_id: int, role: int) -> Tuple[bool, str]:
        """파티 자리 확인"""
        raid_info = await db.execute("SELECT dealer, supporter FROM raid WHERE id = ?", (raid_id,))
        if not raid_info:
            return False, "레이드 정보를 찾을 수 없습니다."
        max_count = int(raid_info[0]["dealer"] if role == 0 else raid_info[0]["supporter"]) or 0

        cnt_row = await db.execute(
            "SELECT COUNT(*) as cnt FROM participants WHERE party_id = ? AND role = ?",
            (party_id, role)
        )
        current_count = int(cnt_row[0]["cnt"]) if cnt_row else 0

        if current_count >= max_count:
            message = "딜러 자리가 모두 찼습니다." if role == 0 else "서포터 자리가 모두 찼습니다."
            return False, message
        return True, ""
    
    async def _send_cancel_notification(self, cancel_info: Dict):
        """참가 취소 알림 발송"""
        try:
            guild_id = cancel_info.get('guild_id')
            if not guild_id:
                return

            cancel_channel_id = None
            async with get_db() as db:
                # 서버 설정에서 cancel_join_channel_id 조회
                server_result = await db.execute(
                    "SELECT cancel_join_channel_id FROM server WHERE guild_id = ?",
                    (str(guild_id),)
                )
                
                if not server_result or not server_result[0].get('cancel_join_channel_id'):
                    return
                
                cancel_channel_id = server_result[0]['cancel_join_channel_id']

            if not cancel_channel_id:
                return

            # 알림 메시지 생성
            user_id = cancel_info['user_id']
            char_name = cancel_info.get('char_name', '알 수 없음')
            party_title = cancel_info.get('title', '알 수 없는 파티')
            thread_id = cancel_info.get('thread_manage_id')

            # 파티 제목에 하이퍼링크 추가
            if thread_id:
                party_link = f"https://discord.com/channels/{guild_id}/{thread_id}"
                party_title_display = f"[{party_title}]({party_link})"
            else:
                party_title_display = f"**{party_title}**"

            embed_data = {
                "title": "🚪 파티 참가 취소",
                "description": f"{party_title_display} 파티에서 참가자가 취소하였어요.",
                "color": 0xFF6B6B,
                "fields": [
                    {
                        "name": "유저",
                        "value": f"<@{user_id}>",
                        "inline": True
                    },
                    {
                        "name": "캐릭터",
                        "value": f"**{char_name}**",
                        "inline": True
                    }
                ]
            }

            # Discord 채널에 알림 발송
            await discord_service.send_to_channel(
                cancel_channel_id,
                embed=embed_data
            )
                
        except Exception as e:
            logger.exception(
                "Cancel notification send failed (guild_id=%s, user_id=%s, party_title=%s)",
                cancel_info.get("guild_id"),
                cancel_info.get("user_id"),
                cancel_info.get("title"),
            )

# 전역 인스턴스
party_service = PartyService()
