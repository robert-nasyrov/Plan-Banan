"""
Run this once to discover topic IDs in the Plan Banan supergroup.
It will print all recent messages with their message_thread_id (topic ID).

Usage:
    python utils/discover_topics.py
"""

import asyncio
from telethon import TelegramClient
from config import API_ID, API_HASH, BOT_TOKEN, PLAN_BANAN_GROUP_ID

bot = TelegramClient("discover", API_ID, API_HASH).start(bot_token=BOT_TOKEN)


async def main():
    topics = {}
    
    async for msg in bot.iter_messages(PLAN_BANAN_GROUP_ID, limit=100):
        topic_id = None
        if msg.reply_to:
            topic_id = getattr(msg.reply_to, 'reply_to_top_id', None) or \
                       getattr(msg.reply_to, 'reply_to_msg_id', None)
        
        if topic_id and topic_id not in topics:
            preview = (msg.text or "[media]")[:60]
            topics[topic_id] = preview
            print(f"Topic ID: {topic_id:>8} | {preview}")
    
    print(f"\nFound {len(topics)} topics")
    print("\nCopy these to your .env:")
    for tid, preview in topics.items():
        print(f"# {preview}")
        print(f"TOPIC_XXX={tid}")


asyncio.run(main())
