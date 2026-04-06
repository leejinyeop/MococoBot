import asyncio
from typing import Any, Awaitable, Callable, List, Optional, Tuple

from database.connection import get_db
from services.discord_service import discord_service


def chunked(seq: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [seq]
    return [seq[i:i + size] for i in range(0, len(seq), size)]


async def log_delivery(kind: str, user_id: int | None, guild_id: int | None, channel_id: int | None, logger: Any) -> None:
    async with get_db() as db:
        try:
            if user_id is not None:
                await db.execute(
                    "INSERT INTO delivery_log (user_id, type, game_date, sent_at, channel_id, guild_id) VALUES (?, ?, CURDATE(), NOW(), NULL, NULL)",
                    (int(user_id), kind),
                )
            else:
                if channel_id is None:
                    return
                await db.execute(
                    "INSERT INTO delivery_log (user_id, channel_id, type, game_date, sent_at, guild_id) VALUES (0, ?, ?, CURDATE(), NOW(), ?)",
                    (int(channel_id), kind, int(guild_id) if guild_id is not None else None),
                )
            await db.commit()
        except Exception:
            logger.exception(
                "Failed to log %s delivery (user_id=%s, guild_id=%s, channel_id=%s)",
                kind,
                user_id,
                guild_id,
                channel_id,
            )


async def log_delivery_bulk(
    kind: str,
    user_ids: List[int],
    channel_targets: List[Tuple[int, int]],
    logger: Any,
    chunk_size: int,
) -> None:
    rows: List[Tuple[int, Optional[int], Optional[int]]] = []
    for uid in user_ids:
        rows.append((int(uid), None, None))
    for gid, cid in channel_targets:
        rows.append((0, int(cid), int(gid)))
    if not rows:
        return

    async def _insert_chunk_safe(db: Any, chunk: List[Tuple[int, Optional[int], Optional[int]]]) -> None:
        if not chunk:
            return
        values = ",".join(["(?, ?, ?, CURDATE(), NOW(), ?)"] * len(chunk))
        flat: List[Any] = []
        for user_id, channel_id, guild_id in chunk:
            flat.extend([user_id, channel_id, kind, guild_id])
        try:
            await db.execute(
                "INSERT INTO delivery_log (user_id, channel_id, type, game_date, sent_at, guild_id) "
                f"VALUES {values}",
                tuple(flat),
            )
        except Exception:
            if len(chunk) == 1:
                raise
            mid = len(chunk) // 2
            await _insert_chunk_safe(db, chunk[:mid])
            await _insert_chunk_safe(db, chunk[mid:])

    async with get_db() as db:
        try:
            for i in range(0, len(rows), chunk_size):
                await _insert_chunk_safe(db, rows[i:i + chunk_size])
            await db.commit()
        except Exception:
            logger.exception("Failed to bulk log %s delivery rows=%s", kind, len(rows))


async def dispatch_embed(
    embed: dict,
    user_targets: List[int],
    channel_targets: List[Tuple[int, int]],
    batch_size: int,
) -> Tuple[List[int], List[Tuple[int, int]]]:
    async def _send_dm(uid: int) -> Optional[int]:
        ok = await discord_service.send_dm(str(uid), embed=embed)
        return uid if ok else None

    async def _send_channel(gid: int, cid: int) -> Optional[Tuple[int, int]]:
        ok = await discord_service.send_to_channel(str(cid), embed=embed)
        return (gid, cid) if ok else None

    delivered_users: List[int] = []
    delivered_channels: List[Tuple[int, int]] = []

    for batch in chunked(user_targets, batch_size):
        results = await asyncio.gather(*[_send_dm(uid) for uid in batch], return_exceptions=True)
        for res in results:
            if isinstance(res, int):
                delivered_users.append(res)

    for batch in chunked(channel_targets, batch_size):
        results = await asyncio.gather(*[_send_channel(gid, cid) for gid, cid in batch], return_exceptions=True)
        for res in results:
            if isinstance(res, tuple) and len(res) == 2:
                delivered_channels.append((int(res[0]), int(res[1])))

    return delivered_users, delivered_channels
