"""
Run this once to discover chat IDs and topic IDs.
Lists all groups/chats the bot is in, then topics for each.

Usage:
    railway run python utils/discover_topics.py
"""

import asyncio
from telethon import TelegramClient
import os

API_ID = int(os.getenv("TELEGRAM_API_ID", 0))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")


async def main():
    bot = TelegramClient("discover", API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)
    
    print("=" * 60)
    print("CHATS WHERE BOT IS A MEMBER:")
    print("=" * 60)
    
    async for dialog in bot.iter_dialogs():
        print(f"\n  Name: {dialog.name}")
        print(f"  Chat ID: {dialog.id}")
        print(f"  Type: {type(dialog.entity).__name__}")
        
        # Try to find topics in this chat
        try:
            topics = {}
            async for msg in bot.iter_messages(dialog.id, limit=200):
                topic_id = None
                if msg.reply_to:
                    topic_id = getattr(msg.reply_to, 'reply_to_top_id', None) or \
                               getattr(msg.reply_to, 'reply_to_msg_id', None)
                
                if topic_id and topic_id not in topics:
                    preview = (msg.text or "[media]")[:50]
                    topics[topic_id] = preview
            
            if topics:
                print(f"  Topics found:")
                for tid, preview in topics.items():
                    print(f"    Topic ID: {tid:>8} | {preview}")
        except Exception as e:
            print(f"  Could not read messages: {e}")
    
    print("\n" + "=" * 60)
    print("Copy the values above into Railway Variables")
    print("=" * 60)
    
    await bot.disconnect()


asyncio.run(main())
