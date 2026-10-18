import asyncio
import time
from telethon import TelegramClient
from telethon import utils
import os
from dotenv import load_dotenv

load_dotenv('/home/ubuntu/telegram-transfer-bot/config.env')
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')

async def test_download_speed():
    client = TelegramClient('/home/ubuntu/telegram-transfer-bot/user_session', api_id, api_hash)
    await client.connect()
    
    dialogs = await client.get_dialogs(limit=50)
    target = None
    for d in dialogs:
        if 'prep' in (d.title or '').lower():
            target = d.entity
            break
            
    if target:
        async for m in client.iter_messages(target, limit=100):
            if m.media and hasattr(m.media, 'document') and m.media.document:
                doc = m.media.document
                print(f"Testing download of real 124.4MB file ID {doc.id}...")
                
                from fast_telethon import fast_download_media
                out_test = "test_download_real.bin"
                t0 = time.time()
                await fast_download_media(client, m, out_test)
                t_down = time.time() - t0
                speed = (doc.size / (1024 * 1024)) / t_down
                print(f"Downloaded {doc.size / (1024*1024):.1f} MB in {t_down:.2f}s -> Speed: {speed:.2f} MB/s ({speed*8:.1f} Mbps)")
                
                if os.path.exists(out_test):
                    os.remove(out_test)
                break
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(test_download_speed())
