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
    PLAN_BANAN_GROUP_ID, TOPICS,
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

bot = TelegramClient("banan_bot", API_ID, API_HASH)

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
# Command: /discover — shows chat_id and topic_id
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/discover'))
async def on_discover(event):
    """Reply with chat_id and topic_id for setup."""
    chat_id = event.chat_id
    topic_id = None
    if event.message.reply_to:
        topic_id = getattr(event.message.reply_to, 'reply_to_top_id', None) or \
                   getattr(event.message.reply_to, 'reply_to_msg_id', None)
    
    await event.reply(
        f"chat_id: {chat_id}\n"
        f"topic_id: {topic_id}\n"
        f"message_thread_id: {getattr(event.message, 'reply_to', None)}"
    )


# ──────────────────────────────────────────────
# Command: /done — manually close an episode
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/done', from_users="nasyrov_robert"))
async def on_done_command(event):
    """Robert closes an episode manually. /done or /done серия_id"""
    episodes = await get_active_episodes(db_conn)
    if not episodes:
        await event.reply("Нет активных серий.")
        return
    
    # If just /done — close the oldest active episode
    text = (event.message.text or "").strip()
    parts = text.split(maxsplit=1)
    
    if len(parts) > 1:
        # Try to find by ID or title fragment
        query = parts[1].strip()
        ep = None
        # Try as ID
        try:
            ep_id = int(query)
            ep = next((e for e in episodes if e["id"] == ep_id), None)
        except ValueError:
            # Try as title fragment
            ep = next((e for e in episodes if query.lower() in e["title"].lower()), None)
    else:
        # Close the oldest active
        ep = episodes[0]
    
    if not ep:
        await event.reply(f"Серия не найдена. Активные:\n" + 
            "\n".join(f"  {e['id']}. {e['title']} — {e['status']}" for e in episodes))
        return
    
    await update_episode_status(
        db_conn, ep["id"], "готово",
        animation_done_at=datetime.now(pytz.utc),
    )
    await event.reply(f"«{ep['title']}» закрыта!")
    log.info(f"Episode {ep['id']} «{ep['title']}» manually closed by Robert")


# ──────────────────────────────────────────────
# Command: /status — show active episodes
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/status', from_users="nasyrov_robert"))
async def on_status_command(event):
    """Show all active episodes and their status."""
    episodes = await get_active_episodes(db_conn)
    if not episodes:
        await event.reply("Нет активных серий.")
        return
    
    lines = []
    for ep in episodes:
        responsible_key = STATUS_RESPONSIBLE.get(ep["status"], "?")
        member = TEAM.get(responsible_key)
        name = member.name if member else "?"
        days = (datetime.now(pytz.utc) - ep["updated_at"].replace(tzinfo=pytz.utc)).days if ep["updated_at"] else 0
        lines.append(f"{ep['id']}. «{ep['title']}» — {ep['status']} ({name}, {days} дн)")
    
    await event.reply("Активные серии:\n" + "\n".join(lines))


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
    
    # Extract title from scenario text via Claude
    text = event.message.text or ""
    title = f"Новая серия ({datetime.now().strftime('%d.%m')})"
    if text:
        try:
            extracted = await ask_claude(
                text[:500],
                system="Из текста сценария мультфильма извлеки короткое название серии (тема). "
                       "Ответь ТОЛЬКО названием, 2-5 слов. Без кавычек, без пояснений. "
                       "Пример: Курбан Хаит, Самолёты, Море"
            )
            if extracted and len(extracted.strip()) < 50:
                title = extracted.strip()
        except Exception as e:
            log.error(f"Title extraction failed: {e}")
    
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
            # Notify animators in music topic
            await bot.send_message(
                PLAN_BANAN_GROUP_ID,
                f"{mention(TEAM['iroda'].username)} {mention(TEAM['sheroz'].username)} "
                f"Музыка и озвучка для «{ep['title']}» готовы! Можно делать анимацию.",
                reply_to=TOPICS["music_voiceover"],
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
        
        # Notify animators in music topic
        await bot.send_message(
            PLAN_BANAN_GROUP_ID,
            f"{mention(TEAM['iroda'].username)} {mention(TEAM['sheroz'].username)} "
            f"Музыка и озвучка для «{ep['title']}» готовы! Можно делать анимацию.",
            reply_to=TOPICS["music_voiceover"],
        )
        log.info(f"Episode {ep['id']} → анимация, animators notified")


# ──────────────────────────────────────────────
# Event: Team member marks work as done
# ──────────────────────────────────────────────

DONE_KEYWORDS = ["готово", "сделано", "done", "готова", "всё готово", "все готово", "закончила", "закончил"]

@bot.on(events.NewMessage(
    chats=PLAN_BANAN_GROUP_ID,
))
async def on_done_message(event):
    """Team member says 'готово' → advance pipeline."""
    text = (event.message.text or "").lower().strip()
    if not any(kw in text for kw in DONE_KEYWORDS):
        return
    
    sender = await event.get_sender()
    if not sender or not sender.username:
        return
    
    username = sender.username
    episodes = await get_active_episodes(db_conn)
    if not episodes:
        return
    
    # Find which stage this person is responsible for
    stage_map = {
        TEAM["mohinur"].username: "перевод",
        TEAM["stas"].username: "музыка",
        TEAM["iroda"].username: "анимация",
        TEAM["sheroz"].username: "анимация",
    }
    
    responsible_stage = stage_map.get(username)
    if not responsible_stage:
        return
    
    # Find episode in that stage
    ep = next((e for e in episodes if e["status"] == responsible_stage), None)
    if not ep:
        return
    
    # Advance to next stage
    next_stage_map = {
        "перевод": ("озвучка_назначена", "translation_done_at"),
        "музыка": ("анимация", "music_done_at"),
        "анимация": ("готово", "animation_done_at"),
    }
    
    next_info = next_stage_map.get(responsible_stage)
    if not next_info:
        return
    
    next_status, timestamp_field = next_info
    await update_episode_status(
        db_conn, ep["id"], next_status,
        **{timestamp_field: datetime.now(pytz.utc)}
    )
    
    # Notify next person
    if next_status == "озвучка_назначена":
        await bot.send_message(
            PLAN_BANAN_GROUP_ID,
            f"{mention(TEAM['robert'].username)} Перевод для «{ep['title']}» готов! "
            f"Договорись с Камилой об озвучке.",
            reply_to=TOPICS["scenarios_uz"],
        )
    elif next_status == "анимация":
        await bot.send_message(
            PLAN_BANAN_GROUP_ID,
            f"{mention(TEAM['iroda'].username)} {mention(TEAM['sheroz'].username)} "
            f"Музыка и озвучка для «{ep['title']}» готовы! Можно делать анимацию.",
            reply_to=TOPICS["music_voiceover"],
        )
    
    await event.reply(f"Принято! «{ep['title']}» → {next_status}")
    log.info(f"Episode {ep['id']} «{ep['title']}» → {next_status} (by {username})")


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
        
        # Send to appropriate topic
        topic = {
            "сценарий": TOPICS["scenarios_ru"],
            "перевод": TOPICS["scenarios_uz"],
            "озвучка_назначена": TOPICS["scenarios_uz"],
            "музыка": TOPICS["music_voiceover"],
            "анимация": TOPICS["music_voiceover"],
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
    
    # Start bot
    await bot.start(bot_token=BOT_TOKEN)
    log.info("Bot connected to Telegram")
    
    # Init DB
    await init_db(DATABASE_URL)
    db_conn = await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row)
    log.info("Database connected")
    
    # Scheduler
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(daily_status_check, CronTrigger(hour=DAILY_CHECK_HOUR, minute=0, timezone=TIMEZONE))
    scheduler.add_job(collect_daily_context, CronTrigger(hour=DAILY_CHECK_HOUR, minute=5, timezone=TIMEZONE))
    scheduler.start()
    log.info(f"Scheduler started: daily check at {DAILY_CHECK_HOUR}:00 {TIMEZONE}")
    
    # Run bot
    log.info("Bot is running!")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    bot.loop.run_until_complete(main())
