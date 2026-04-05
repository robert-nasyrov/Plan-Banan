"""
Plan Banan Cowork Bot

Monitors supergroup topics, tracks episode production pipeline,
sends daily status checks and auto-notifies responsible team members.

Stack: Telethon (userbot/bot hybrid), psycopg3, Claude API, APScheduler
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta

import httpx
import psycopg
from psycopg.rows import dict_row
from telethon import TelegramClient, events
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from config import (
    API_ID, API_HASH, BOT_TOKEN,
    CLAUDE_API_KEY, CLAUDE_MODEL,
    DATABASE_URL,
    PLAN_BANAN_GROUP_ID, TOPICS, ANIMATORS_CHAT_ID,
    TEAM, TIMEZONE, DAILY_CHECK_HOUR,
)
from db import (
    init_db, create_episode, update_episode_status,
    get_active_episodes, get_stalled_episodes, save_chat_summary,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("banan-bot")

# ──────────────────────────────────────────────
# Telethon client
# ──────────────────────────────────────────────

bot = TelegramClient("banan_bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# DB connection (initialized on startup)
db_conn = None


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

STATUS_FLOW = [
    "сценарий",
    "перевод",
    "озвучка_назначена",
    "музыка",
    "анимация",
    "готово",
]

STATUS_RESPONSIBLE = {
    "сценарий": "vadim",
    "перевод": "mohinur",
    "озвучка_назначена": "robert",  # Robert coordinates with Kamila
    "музыка": "stas",
    "анимация": "iroda",  # + sheroz
}


def mention(username: str) -> str:
    """Create a @mention string."""
    return f"@{username}" if username else ""


async def ask_claude(prompt: str, system: str = "") -> str:
    """Call Claude API for natural language understanding."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": CLAUDE_API_KEY,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": 1024,
                "system": system,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        data = resp.json()
        return data["content"][0]["text"]


async def parse_robert_message(text: str) -> dict | None:
    """
    Use Claude to parse natural language commands from Robert.
    Returns dict with action and params, or None if not a command.
    
    Examples:
      "Камила придет завтра в 15:00" → {"action": "schedule_voiceover", "date": "..."}
      "Стас скинул музыку для серии 26" → {"action": "mark_music_done", "episode": "26"}
    """
    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
    
    system = f"""Ты парсер команд для бота управления производством мультфильма.
Сегодня: {today}. Timezone: Asia/Tashkent.

Если сообщение содержит команду — верни JSON. Если это обычный чат — верни {{"action": null}}.

Возможные action:
- "schedule_voiceover" — назначена озвучка. Поля: episode_title (str или null), date (ISO), time (HH:MM)
- "mark_done" — этап завершён. Поля: episode_title (str или null), stage (сценарий/перевод/музыка/анимация)
- "new_episode" — новая серия. Поля: title (str)
- "status" — запрос статуса. Поля: нет

Отвечай ТОЛЬКО JSON, без markdown."""

    try:
        result = await ask_claude(text, system)
        return json.loads(result.strip())
    except Exception as e:
        log.error(f"Claude parse error: {e}")
        return None


# ──────────────────────────────────────────────
# Event: New scenario in "Сценарии RU"
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(
    chats=PLAN_BANAN_GROUP_ID,
    func=lambda e: getattr(e.message, 'reply_to', None) 
        and getattr(e.message.reply_to, 'reply_to_top_id', None) == TOPICS["scenarios_ru"]
        or getattr(e.message.reply_to, 'reply_to_msg_id', None) == TOPICS["scenarios_ru"]
))
async def on_new_scenario(event):
    """Vadim posts a scenario → notify Mohinur in UZ topic."""
    sender = await event.get_sender()
    if not sender or sender.username != TEAM["vadim"].username:
        return
    
    log.info(f"New scenario detected from {sender.username}")
    
    # Create episode in DB
    # Try to extract title from message
    title = f"Новая серия ({datetime.now().strftime('%d.%m')})"
    episode = await create_episode(db_conn, title)
    await update_episode_status(
        db_conn, episode["id"], "перевод",
        scenario_done_at=datetime.now(pytz.utc),
        scenario_by=sender.username,
    )
    
    # Notify Mohinur in UZ topic
    await bot.send_message(
        PLAN_BANAN_GROUP_ID,
        f"Здравствуйте, {mention(TEAM['mohinur'].username)}! "
        f"Новый сценарий на перевод, спасибо!",
        reply_to=TOPICS["scenarios_uz"],
    )
    log.info(f"Notified Mohinur, episode {episode['id']} → перевод")


# ──────────────────────────────────────────────
# Event: Translation done in "Сценарии UZ"
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(
    chats=PLAN_BANAN_GROUP_ID,
    func=lambda e: getattr(e.message, 'reply_to', None)
        and getattr(e.message.reply_to, 'reply_to_top_id', None) == TOPICS["scenarios_uz"]
        or getattr(e.message.reply_to, 'reply_to_msg_id', None) == TOPICS["scenarios_uz"]
))
async def on_translation_done(event):
    """Mohinur posts translation → notify Robert."""
    sender = await event.get_sender()
    if not sender or sender.username != TEAM["mohinur"].username:
        return
    
    log.info(f"Translation detected from {sender.username}")
    
    # Find latest episode in 'перевод' status
    episodes = await get_active_episodes(db_conn)
    ep = next((e for e in episodes if e["status"] == "перевод"), None)
    
    if ep:
        await update_episode_status(
            db_conn, ep["id"], "озвучка_назначена",
            translation_done_at=datetime.now(pytz.utc),
            translation_by=sender.username,
        )
    
    # Notify Robert
    await bot.send_message(
        PLAN_BANAN_GROUP_ID,
        f"{mention(TEAM['robert'].username)} Перевод готов! "
        f"Договорись с Камилой об озвучке.",
        reply_to=TOPICS["scenarios_uz"],
    )


# ──────────────────────────────────────────────
# Event: Robert's natural language commands
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(
    chats=PLAN_BANAN_GROUP_ID,
    from_users="nasyrov_robert",
))
async def on_robert_message(event):
    """Parse Robert's messages for commands via Claude."""
    text = event.message.text
    if not text:
        return
    
    parsed = await parse_robert_message(text)
    if not parsed or parsed.get("action") is None:
        return
    
    action = parsed["action"]
    
    if action == "schedule_voiceover":
        date_str = parsed.get("date", "")
        time_str = parsed.get("time", "")
        
        # Update episode
        episodes = await get_active_episodes(db_conn)
        ep = next((e for e in episodes if e["status"] == "озвучка_назначена"), None)
        if ep:
            voiceover_dt = datetime.fromisoformat(f"{date_str}T{time_str}:00")
            await update_episode_status(
                db_conn, ep["id"], "озвучка_назначена",
                voiceover_date=voiceover_dt,
            )
        
        # Notify Stas in music topic
        await bot.send_message(
            PLAN_BANAN_GROUP_ID,
            f"{mention(TEAM['stas'].username)} Камила придет на озвучку "
            f"{date_str} в {time_str}. Подготовь всё!",
            reply_to=TOPICS["music_voiceover"],
        )
        await event.reply("Записал, Стас уведомлён.")
    
    elif action == "mark_done":
        stage = parsed.get("stage", "")
        episodes = await get_active_episodes(db_conn)
        # Find matching episode
        ep = next((e for e in episodes if e["status"] != "готово"), None)
        if ep and stage == "музыка":
            await update_episode_status(
                db_conn, ep["id"], "анимация",
                music_done_at=datetime.now(pytz.utc),
            )
            # Notify animators in separate chat
            await bot.send_message(
                ANIMATORS_CHAT_ID,
                f"{mention(TEAM['iroda'].username)} {mention(TEAM['sheroz'].username)} "
                f"Музыка и озвучка для «{ep['title']}» готовы! Можно делать анимацию.",
            )
            await event.reply("Ирода и Шероз уведомлены.")
    
    elif action == "status":
        episodes = await get_active_episodes(db_conn)
        if not episodes:
            await event.reply("Нет активных серий.")
            return
        lines = []
        for ep in episodes:
            responsible = STATUS_RESPONSIBLE.get(ep["status"], "?")
            member = TEAM.get(responsible)
            name = member.name if member else "?"
            lines.append(f"• {ep['title']} — {ep['status']} ({name})")
        await event.reply("Активные серии:\n" + "\n".join(lines))


# ──────────────────────────────────────────────
# Event: Music/voiceover file from Stas
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(
    chats=PLAN_BANAN_GROUP_ID,
    func=lambda e: getattr(e.message, 'reply_to', None)
        and (getattr(e.message.reply_to, 'reply_to_top_id', None) == TOPICS["music_voiceover"]
        or getattr(e.message.reply_to, 'reply_to_msg_id', None) == TOPICS["music_voiceover"])
        and e.message.media is not None
))
async def on_music_uploaded(event):
    """Stas uploads a file in music topic → notify animators."""
    sender = await event.get_sender()
    if not sender or sender.username != TEAM["stas"].username:
        return
    
    log.info(f"Music/voiceover file from Stas")
    
    episodes = await get_active_episodes(db_conn)
    ep = next((e for e in episodes if e["status"] in ("озвучка_назначена", "музыка")), None)
    
    if ep:
        await update_episode_status(
            db_conn, ep["id"], "анимация",
            music_done_at=datetime.now(pytz.utc),
            music_by=sender.username,
        )
        
        # Notify animators in separate chat
        await bot.send_message(
            ANIMATORS_CHAT_ID,
            f"{mention(TEAM['iroda'].username)} {mention(TEAM['sheroz'].username)} "
            f"Музыка и озвучка для «{ep['title']}» готовы! Можно делать анимацию.",
        )
        log.info(f"Episode {ep['id']} → анимация, animators notified")


# ──────────────────────────────────────────────
# Daily status check (cron 10:00 Tashkent)
# ──────────────────────────────────────────────

async def daily_status_check():
    """Check all active episodes, ping whoever is stalling."""
    log.info("Running daily status check")
    
    episodes = await get_active_episodes(db_conn)
    if not episodes:
        return
    
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    
    for ep in episodes:
        updated = ep["updated_at"]
        if updated.tzinfo is None:
            updated = pytz.utc.localize(updated)
        
        hours_stuck = (now - updated.astimezone(tz)).total_seconds() / 3600
        
        # If stuck for more than 24 hours, ping responsible
        if hours_stuck < 24:
            continue
        
        responsible_key = STATUS_RESPONSIBLE.get(ep["status"])
        if not responsible_key:
            continue
        
        member = TEAM[responsible_key]
        days_stuck = int(hours_stuck // 24)
        
        status_messages = {
            "сценарий": f"{mention(member.username)} Сценарий для «{ep['title']}» в работе уже {days_stuck} дн. Как дела?",
            "перевод": f"{mention(member.username)} Перевод для «{ep['title']}» ждём уже {days_stuck} дн. Как прогресс?",
            "озвучка_назначена": f"{mention(member.username)} Озвучка для «{ep['title']}» ещё не назначена ({days_stuck} дн). Договорись с Камилой!",
            "музыка": f"{mention(member.username)} Музыка для «{ep['title']}» ждём уже {days_stuck} дн. Когда будет готово?",
            "анимация": f"{mention(TEAM['iroda'].username)} {mention(TEAM['sheroz'].username)} Анимация для «{ep['title']}» в работе {days_stuck} дн. Как прогресс?",
        }
        
        msg = status_messages.get(ep["status"])
        if not msg:
            continue
        
        # Send to appropriate chat
        if ep["status"] == "анимация":
            await bot.send_message(ANIMATORS_CHAT_ID, msg)
        else:
            topic = {
                "сценарий": TOPICS["scenarios_ru"],
                "перевод": TOPICS["scenarios_uz"],
                "озвучка_назначена": TOPICS["scenarios_uz"],
                "музыка": TOPICS["music_voiceover"],
            }.get(ep["status"])
            
            if topic:
                await bot.send_message(PLAN_BANAN_GROUP_ID, msg, reply_to=topic)
            else:
                await bot.send_message(PLAN_BANAN_GROUP_ID, msg)
    
    log.info("Daily check done")


# ──────────────────────────────────────────────
# Daily context collector
# ──────────────────────────────────────────────

async def collect_daily_context():
    """
    Read last 24h of messages from monitored chats,
    summarize with Claude, save to DB.
    """
    log.info("Collecting daily context")
    
    tz = pytz.timezone(TIMEZONE)
    since = datetime.now(tz) - timedelta(hours=24)
    
    chats_to_monitor = [
        (PLAN_BANAN_GROUP_ID, "План Банан (supergroup)"),
        (ANIMATORS_CHAT_ID, "Отдел аниматоров"),
    ]
    
    for chat_id, chat_name in chats_to_monitor:
        messages = []
        async for msg in bot.iter_messages(chat_id, offset_date=datetime.now(), limit=200):
            if msg.date.astimezone(tz) < since:
                break
            if msg.text:
                sender = await msg.get_sender()
                name = getattr(sender, 'first_name', '?') if sender else '?'
                messages.append({
                    "from": name,
                    "text": msg.text,
                    "date": msg.date.isoformat(),
                    "topic_id": getattr(msg.reply_to, 'reply_to_top_id', None) if msg.reply_to else None,
                })
        
        if not messages:
            continue
        
        # Summarize with Claude
        summary = await ask_claude(
            f"Вот сообщения за последние 24 часа из чата '{chat_name}':\n\n"
            + "\n".join(f"[{m['from']}]: {m['text']}" for m in messages[-50:]),
            system="Ты помощник продюсера мультфильма Plan Banan. "
                   "Кратко резюмируй: что сделано, что в процессе, какие проблемы. "
                   "2-5 предложений. На русском.",
        )
        
        await save_chat_summary(db_conn, chat_id, None, summary, messages)
        log.info(f"Saved context for {chat_name}: {len(messages)} messages")


# ──────────────────────────────────────────────
# Startup
# ──────────────────────────────────────────────

async def main():
    global db_conn
    
    log.info("Starting Plan Banan Cowork Bot...")
    
    # Init DB
    await init_db(DATABASE_URL)
    db_conn = await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row)
    log.info("Database connected")
    
    # Scheduler
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(daily_status_check, CronTrigger(hour=DAILY_CHECK_HOUR, minute=0))
    scheduler.add_job(collect_daily_context, CronTrigger(hour=DAILY_CHECK_HOUR, minute=5))
    scheduler.start()
    log.info(f"Scheduler started: daily check at {DAILY_CHECK_HOUR}:00 {TIMEZONE}")
    
    # Run bot
    log.info("Bot is running!")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
