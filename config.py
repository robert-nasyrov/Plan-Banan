import os
from dataclasses import dataclass, field

# ──────────────────────────────────────────────
# Plan Banan Cowork Bot — Configuration
# ──────────────────────────────────────────────

# Telegram
API_ID = int(os.getenv("TELEGRAM_API_ID", 0))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Claude API
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY", "")
CLAUDE_MODEL = "claude-opus-4-6"

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "")

# ──────────────────────────────────────────────
# Chats
# ──────────────────────────────────────────────

# Main supergroup "План Банан" — replace with real chat_id
PLAN_BANAN_GROUP_ID = int(os.getenv("PLAN_BANAN_GROUP_ID", 0))

# Topic IDs inside supergroup (message_thread_id)
# Run `python utils/discover_topics.py` to find these
TOPICS = {
    "scenarios_ru": int(os.getenv("TOPIC_SCENARIOS_RU", 0)),
    "scenarios_uz": int(os.getenv("TOPIC_SCENARIOS_UZ", 0)),
    "music_voiceover": int(os.getenv("TOPIC_MUSIC_VOICEOVER", 0)),
    "banan_uz": int(os.getenv("TOPIC_BANAN_UZ", 0)),
}


# ──────────────────────────────────────────────
# Team
# ──────────────────────────────────────────────

@dataclass
class TeamMember:
    name: str
    username: str  # without @
    role: str
    user_id: int = 0  # telegram user_id, fill later

TEAM = {
    "vadim": TeamMember("Вадим", "madvadps", "сценарий"),
    "mohinur": TeamMember("Мохинур", "nurontelegram", "перевод"),
    "robert": TeamMember("Роберт", "nasyrov_robert", "продюсер"),
    "kamila": TeamMember("Камила", "", "озвучка"),  # no username, personal comms
    "stas": TeamMember("Стас", "Guaho13", "музыка"),
    "iroda": TeamMember("Ирода", "lrobby", "анимация"),
    "sheroz": TeamMember("Шероз", "radjabovsh", "AI вставки"),
    "nigina": TeamMember("Нигина", "nigina_marketing", "публикация"),
}

# ──────────────────────────────────────────────
# Schedule
# ──────────────────────────────────────────────

DAILY_CHECK_HOUR = 10  # 10:00 Tashkent
TIMEZONE = "Asia/Tashkent"
