from typing import Any, Dict, List, Optional, Tuple
import logging

from database.connection import get_db
from services.discord_service import discord_service
from services.lostark_service import search_lostark_character as _search_lostark_character

logger = logging.getLogger(__name__)


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return default


def chunked(seq: List[int], size: int) -> List[List[int]]:
    if size <= 0:
        return [seq]
    return [seq[i:i + size] for i in range(0, len(seq), size)]


async def search_lostark_character(char_name: str) -> Optional[Dict[str, Any]]:
    return await _search_lostark_character(char_name)


async def save_character_to_db(char_data: Dict[str, Any], user_id: str = "") -> Optional[Dict[str, Any]]:
    try:
        async with get_db() as db:
            combined_result = await db.execute(
                """
                SELECT
                    cl.id as class_id, cl.emoji as class_emoji,
                    c.id as existing_id, c.combat_power as existing_combat_power,
                    c.item_lvl as existing_item_lvl
                FROM class cl
                LEFT JOIN `character` c ON c.char_name = ? AND c.class_id = cl.id
                WHERE cl.name = ?
                LIMIT 1
                """,
                (char_data.get("char_name"), char_data.get("class_name")),
            )
            if not combined_result:
                return None

            result = combined_result[0]
            class_id = result["class_id"]
            class_emoji = result["class_emoji"]
            existing_id = result["existing_id"]
            existing_combat_power = parse_float(result.get("existing_combat_power"))
            existing_item_lvl = parse_float(result.get("existing_item_lvl"))
            new_item_lvl = parse_float(char_data.get("item_lvl"))
            new_combat_power = parse_float(char_data.get("combat_power"))

            if existing_id:
                update_fields: List[str] = []
                params: List[Any] = []
                if new_item_lvl != existing_item_lvl:
                    update_fields.append("item_lvl = ?")
                    params.append(new_item_lvl)
                if new_combat_power > existing_combat_power:
                    update_fields.append("combat_power = ?")
                    params.append(new_combat_power)
                if update_fields:
                    update_fields.append("class_id = ?")
                    params.append(class_id)
                    params.append(existing_id)
                    await db.execute(
                        f"UPDATE `character` SET {', '.join(update_fields)} WHERE id = ?",
                        tuple(params),
                    )
                char_id = existing_id
                final_combat_power = max(existing_combat_power, new_combat_power)
            else:
                await db.execute(
                    """
                    INSERT INTO `character` (class_id, char_name, item_lvl, combat_power)
                    VALUES (?, ?, ?, ?)
                    """,
                    (class_id, char_data.get("char_name"), new_item_lvl, new_combat_power),
                )
                char_id = db.lastrowid
                final_combat_power = new_combat_power

            await db.commit()
            return {
                "id": char_id,
                "user_id": user_id,
                "class_id": class_id,
                "char_name": char_data.get("char_name"),
                "item_lvl": new_item_lvl,
                "combat_power": final_combat_power,
                "class_name": char_data.get("class_name"),
                "class_emoji": class_emoji,
            }
    except Exception:
        return None


async def get_character_parties_and_update(character_id: int, include_discord: bool = False) -> Tuple[List[Dict], Optional[Dict]]:
    try:
        async with get_db() as db:
            parties = await db.execute(
                """
                SELECT p.id, p.title, p.thread_manage_id, r.name as raid_name,
                       r.difficulty, r.dealer, r.supporter
                FROM party p
                LEFT JOIN raid r ON p.raid_id = r.id
                LEFT JOIN participants pt ON p.id = pt.party_id
                WHERE pt.character_id = ? AND p.thread_manage_id IS NOT NULL
                """,
                (character_id,),
            )
            parties_list = parties or []
            if not include_discord or not parties_list:
                return parties_list, None
            party_ids = [party["id"] for party in parties_list]
            return parties_list, {"party_ids": party_ids}
    except Exception:
        return [], None


async def batch_update_discord_posts(party_ids: List[int]) -> Dict[str, Any]:
    if not party_ids:
        return {"success": [], "failed": [], "total": 0}

    results = {"success": [], "failed": [], "total": len(party_ids)}
    try:
        for batch in chunked(party_ids, 200):
            async with get_db() as db:
                placeholders = ",".join("?" for _ in batch)
                parties_data = await db.execute(
                    f"""
                    SELECT p.id, p.title, p.thread_manage_id, p.owner, r.name AS raid_name,
                           r.difficulty, r.dealer, r.supporter
                    FROM party p
                    LEFT JOIN raid r ON p.raid_id = r.id
                    WHERE p.id IN ({placeholders}) AND p.thread_manage_id IS NOT NULL
                    """,
                    batch,
                )
                if not parties_data:
                    continue

                party_id_set = [p["id"] for p in parties_data]
                placeholders2 = ",".join("?" for _ in party_id_set)
                participants_rows = await db.execute(
                    f"""
                    SELECT
                        p.party_id, p.user_id, p.role,
                        c.char_name, c.item_lvl, c.combat_power,
                        cl.name AS class_name
                    FROM participants p
                    LEFT JOIN `character` c ON p.character_id = c.id
                    LEFT JOIN class cl ON c.class_id = cl.id
                    WHERE p.party_id IN ({placeholders2})
                    ORDER BY p.joined_at ASC
                    """,
                    party_id_set,
                ) or []

                grouped: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}
                for row in participants_rows:
                    pid = row["party_id"]
                    g = grouped.get(pid)
                    if g is None:
                        g = {"dealers": [], "supporters": []}
                        grouped[pid] = g
                    data = {
                        "user_id": row["user_id"],
                        "name": row["char_name"],
                        "item_level": row["item_lvl"],
                        "class_name": row["class_name"],
                        "combat_power": row["combat_power"],
                    }
                    if row["role"] == 0:
                        g["dealers"].append(data)
                    elif row["role"] == 1:
                        g["supporters"].append(data)

            for party in parties_data:
                pid = party["id"]
                try:
                    party_update_data = {
                        **party,
                        "participants": grouped.get(pid, {"dealers": [], "supporters": []}),
                    }
                    success = await discord_service.update_forum_post(party["thread_manage_id"], party_update_data)
                    if success:
                        results["success"].append(pid)
                    else:
                        results["failed"].append({"party_id": pid, "reason": "Discord API failed"})
                except Exception as e:
                    results["failed"].append({"party_id": pid, "reason": str(e)})
    except Exception:
        logger.exception("Batch discord post update failed (party_count=%s)", len(party_ids))
    return results
