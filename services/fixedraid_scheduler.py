from __future__ import annotations
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from typing import Optional
from database.connection import get_db
from utils.fixedraid import weekly_generate_for_guild

_scheduler: Optional[AsyncIOScheduler] = None

async def _run_all_guilds():
    async with get_db() as db:
        rows = await db.fetch_all("SELECT DISTINCT guild_id FROM fixed_raid WHERE is_active = 1")
    for r in rows:
        await weekly_generate_for_guild(int(r["guild_id"]))

def start():
    global _scheduler
    if _scheduler and _scheduler.running:
        return
    _scheduler = AsyncIOScheduler(timezone="Asia/Seoul")
    _scheduler.add_job(
        _run_all_guilds,
        CronTrigger(day_of_week="wed", hour=0, minute=1),
        id="fixedraid_weekly",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
        replace_existing=True,
    )
    _scheduler.start()

def stop():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    _scheduler = None
