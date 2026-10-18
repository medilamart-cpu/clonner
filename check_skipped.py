import sqlite3
import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv('/home/ubuntu/telegram-transfer-bot/config.env')
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')

conn = sqlite3.connect('/home/ubuntu/telegram-transfer-bot/cloner.db')
c = conn.cursor()
c.execute('SELECT source_msg_id FROM cloned_messages')
cloned_ids = set(r[0] for r in c.fetchall())

async def check():
    client = TelegramClient('/home/ubuntu/telegram-transfer-bot/user_session', api_id, api_hash)
    await client.connect()
    async for d in client.iter_dialogs(limit=50):
        if '☠' in d.title:
            source = d.entity
            print(f'Checking source: {d.title} (ID: {d.id})')
            skipped = []
            async for m in client.iter_messages(source, limit=1400):
                if m.id not in cloned_ids:
                    skipped.append(m)
            print(f'Total skipped in source: {len(skipped)}')
            for m in skipped:
                action_name = type(m.action).__name__ if m.action else 'None'
                text_snippet = (m.text or '')[:40].replace('\n', ' ')
                print(f'  Msg #{m.id} | Action: {action_name} | Media: {bool(m.media)} | Text: "{text_snippet}"')
            break
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(check())
8ii