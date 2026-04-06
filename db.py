"""
Database schema and helpers for Plan Banan Cowork Bot.

Tables:
  episodes    — one row per episode being tracked
  chat_log    — daily context snapshots from group chats
"""

import asyncio
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row

SCHEMA = """
CREATE TABLE IF NOT EXISTS episodes (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,            -- e.g. "Серия 26 — Самолёт"
    status          TEXT NOT NULL DEFAULT 'сценарий',
    -- statuses: сценарий → перевод → озвучка_назначена → музыка → анимация → готово
    
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    
    -- timestamps per stage (filled as we progress)
    scenario_done_at    TIMESTAMPTZ,
    translation_done_at TIMESTAMPTZ,
    voiceover_date      TIMESTAMPTZ,      -- scheduled date for Kamila
    voiceover_done_at   TIMESTAMPTZ,
    music_done_at       TIMESTAMPTZ,
    animation_done_at   TIMESTAMPTZ,
    published_at        TIMESTAMPTZ,
    
    -- who confirmed each stage (username)
    scenario_by         TEXT,
    translation_by      TEXT,
    music_by            TEXT,
    
    notes           TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS chat_log (
    id              SERIAL PRIMARY KEY,
    chat_id         BIGINT NOT NULL,
    topic_id        INTEGER,
    date            DATE NOT NULL DEFAULT CURRENT_DATE,
    summary         TEXT NOT NULL,          -- Claude-generated daily summary
    raw_messages    JSONB DEFAULT '[]',     -- raw messages for context
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_episodes_status ON episodes(status);
CREATE INDEX IF NOT EXISTS idx_chat_log_date ON chat_log(date);
"""


async def init_db(database_url: str):
    """Create tables if they don't exist."""
    async with await psycopg.AsyncConnection.connect(database_url) as conn:
        await conn.execute(SCHEMA)
        await conn.commit()


async def create_episode(conn, title: str) -> dict:
    """Create a new episode and return it."""
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute(
        """INSERT INTO episodes (title, status) 
           VALUES (%s, 'сценарий') 
           RETURNING *""",
        (title,),
    )
    await conn.commit()
    return await cur.fetchone()


async def update_episode_status(conn, episode_id: int, new_status: str, **kwargs):
    """
    Move episode to next status.
    kwargs can include timestamp fields like scenario_done_at, music_by, etc.
    """
    sets = ["status = %s", "updated_at = now()"]
    params = [new_status]
    
    for key, value in kwargs.items():
        sets.append(f"{key} = %s")
        params.append(value)
    
    params.append(episode_id)
    query = f"UPDATE episodes SET {', '.join(sets)} WHERE id = %s RETURNING *"
    
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute(query, params)
    await conn.commit()
    return await cur.fetchone()


async def get_active_episodes(conn) -> list[dict]:
    """Get all episodes not yet in 'готово' status."""
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute(
        "SELECT * FROM episodes WHERE status != 'готово' ORDER BY created_at"
    )
    return await cur.fetchall()


async def get_stalled_episodes(conn, stall_hours: int = 48) -> list[dict]:
    """Get episodes that haven't moved in stall_hours."""
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute(
        """SELECT * FROM episodes 
           WHERE status != 'готово' 
           AND updated_at < now() - interval '%s hours'
           ORDER BY updated_at""",
        (stall_hours,),
    )
    return await cur.fetchall()


async def save_chat_summary(conn, chat_id: int, topic_id: int | None, summary: str, raw_messages: list):
    """Save daily chat summary."""
    await conn.execute(
        """INSERT INTO chat_log (chat_id, topic_id, summary, raw_messages)
           VALUES (%s, %s, %s, %s::jsonb)""",
        (chat_id, topic_id, summary, psycopg.types.json.Json(raw_messages))
    )
    await conn.commit()
